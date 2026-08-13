//! runtime WebSocket 通道（硬停止命令投递 + 持有列表上报）。
//!
//! 消息（JSON，`type` 区分）：
//! - hub→runtime：`{"type":"command","operation_id":…,"session_id":…,"run_id":…,
//!                  "kind":"force_stop"|"abandon","require_snapshot":bool}`
//! - runtime→hub：`{"type":"ack","operation_id":…,"status":"ok"|"snapshot_lost"}`
//! - runtime→hub：`{"type":"owned_sessions","session_ids":[…]}`（10 秒周期上报）
//!
//! 可靠性协议（用户定稿）：命令必须 ack；10 秒无 ack 重发，最多重试 3 次，
//! 全部超时 → 断开该 WebSocket。断线后 runtime 重连补报持有列表，hub 对账发现
//! force_stopping/非权威会话 → 重新下发命令（新连接继续走 ACK 状态机）。
//! 5 分钟未处理完的快照 → 定时任务标记 snapshot_lost。

use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::sync::watch;

use axum::extract::ws::{Message, WebSocket, WebSocketUpgrade};
use axum::extract::State;
use axum::response::IntoResponse;
use futures_util::{FutureExt, SinkExt, StreamExt};
use serde::Deserialize;
use serde_json::{json, Value};
use tokio::sync::mpsc;
use tokio::time::{sleep_until, Instant};
use uuid::Uuid;

use crate::api::support::authn::require_runtime;
use crate::api::support::error::ApiError;
use crate::api::support::state::AppState;

/// 每 runtime 一条 WebSocket 连接的命令通道（新连接替换旧连接）。
/// 命令投递失败（连接不存在/已关闭）由 runtime 10 秒持有列表上报兜底重推。
#[derive(Clone, Default)]
pub(crate) struct RuntimeWsRegistry {
    senders: Arc<Mutex<HashMap<Uuid, Entry>>>,
}

struct Entry {
    command_tx: mpsc::Sender<String>,
    cancel_tx: watch::Sender<bool>,
}

impl RuntimeWsRegistry {
    /// 注册（或替换）runtime 的连接，返回命令接收端 + 本连接自己的
    /// sender token + 取消接收端。替换旧连接时立即 cancel 旧连接。
    pub(crate) fn register(
        &self,
        runtime_id: Uuid,
    ) -> (
        mpsc::Receiver<String>,
        mpsc::Sender<String>,
        watch::Receiver<bool>,
    ) {
        let (command_tx, command_rx) = mpsc::channel(64);
        let (cancel_tx, cancel_rx) = watch::channel(false);
        let mut map = self.senders.lock().unwrap();
        if let Some(old) = map.insert(
            runtime_id,
            Entry {
                command_tx: command_tx.clone(),
                cancel_tx,
            },
        ) {
            let _ = old.cancel_tx.send(true);
        }
        (command_rx, command_tx, cancel_rx)
    }

    /// 推送命令；连接不存在或通道已满 → false（由上报兜底重推）。
    pub(crate) fn push(&self, runtime_id: Uuid, message: String) -> bool {
        let map = self.senders.lock().unwrap();
        match map.get(&runtime_id) {
            Some(entry) => entry.command_tx.try_send(message).is_ok(),
            None => false,
        }
    }

    /// 移除连接；仅当仍是本连接（token 同通道）时移除，避免误删新连接。
    pub(crate) fn unregister(&self, runtime_id: Uuid, token: &mpsc::Sender<String>) {
        let mut map = self.senders.lock().unwrap();
        if let Some(entry) = map.get(&runtime_id) {
            if entry.command_tx.same_channel(token) {
                map.remove(&runtime_id);
            }
        }
    }
}

const ACK_TIMEOUT: Duration = Duration::from_secs(10);
const MAX_ACK_SENDS: u32 = 4; // 首次 + 最多 3 次重发

#[derive(Debug, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
enum UpstreamMessage {
    Ack { operation_id: Uuid, status: String },
    OwnedSessions { sessions: Vec<OwnedSessionReport> },
}

/// 上报的持有会话（run_id 为本地 record 的 reserved_run_id，无活动 run 时为 null）。
#[derive(Debug, Deserialize)]
pub(crate) struct OwnedSessionReport {
    pub session_id: Uuid,
    pub run_id: Option<Uuid>,
}

/// 连接内 in-flight 命令：等待 ack，超时重发，3 次全超时断开连接。
struct PendingCommand {
    operation_id: Uuid,
    text: String,
    attempts: u32,
    deadline: Instant,
}

pub(crate) async fn runtime_ws_upgrade(
    State(state): State<Arc<AppState>>,
    headers: axum::http::HeaderMap,
    ws: WebSocketUpgrade,
) -> Result<impl IntoResponse, ApiError> {
    let runtime_id = require_runtime(&state, &headers).await?;
    Ok(ws.on_upgrade(move |socket| runtime_ws_loop(state, runtime_id, socket)))
}

async fn runtime_ws_loop(state: Arc<AppState>, runtime_id: Uuid, socket: WebSocket) {
    let registry = state.runtime_ws.clone();
    let (mut commands, my_token, mut cancel) = registry.register(runtime_id);
    let (mut sink, mut stream) = socket.split();

    let mut pending: Option<PendingCommand> = None;

    loop {
        let timeout_fut = match &pending {
            Some(p) => sleep_until(p.deadline).boxed(),
            None => std::future::pending().boxed(),
        };
        // pending 期间不消费命令通道：命令留在 channel 排队，ack 后再取。
        let recv_fut = if pending.is_none() {
            commands.recv().boxed()
        } else {
            std::future::pending().boxed()
        };
        // 取消信号始终监听：被新连接替换时立即退出。
        let cancel_fut = async {
            if cancel.changed().await.is_err() || *cancel.borrow() {
                Some(())
            } else {
                None
            }
        }
        .boxed();
        tokio::select! {
            _ = timeout_fut => {
                // ack 超时：重发（<3 次）或断开（已达 3 次）。
                let mut p = pending.take().unwrap();
                if p.attempts >= MAX_ACK_SENDS {
                    tracing::warn!(%runtime_id, operation_id = %p.operation_id,
                        attempts = p.attempts, "force stop command not acked, closing ws");
                    break;
                }
                p.attempts += 1;
                p.deadline = Instant::now() + ACK_TIMEOUT;
                if sink.send(Message::Text(p.text.clone().into())).await.is_err() {
                    break;
                }
                pending = Some(p);
            }
            incoming = stream.next() => {
                match incoming {
                    Some(Ok(Message::Text(text))) => {
                        handle_upstream(&state, runtime_id, &text, &mut pending).await;
                    }
                    Some(Ok(Message::Close(_))) | None => break,
                    Some(Ok(_)) => {}
                    Some(Err(_)) => break,
                }
            }
            _ = cancel_fut => break,
            command = recv_fut => {
                match command {
                    Some(text) => {
                        // pending 分支已禁用 recv，走到这里必为 None。
                        let operation_id = serde_json::from_str::<Value>(&text)
                            .ok()
                            .and_then(|v| {
                                v.get("operation_id")
                                    .and_then(|x| x.as_str())
                                    .map(str::to_owned)
                            })
                            .and_then(|s| Uuid::parse_str(&s).ok());
                        if sink.send(Message::Text(text.clone().into())).await.is_err() {
                            break;
                        }
                        pending = Some(PendingCommand {
                            operation_id: operation_id.unwrap_or(Uuid::nil()),
                            text,
                            attempts: 1,
                            deadline: Instant::now() + ACK_TIMEOUT,
                        });
                    }
                    None => break,
                }
            }
        }
    }
    // 注销本连接（仅当仍是同一条连接时移除，避免误删新连接）。
    registry.unregister(runtime_id, &my_token);
}

async fn handle_upstream(
    state: &Arc<AppState>,
    runtime_id: Uuid,
    text: &str,
    pending: &mut Option<PendingCommand>,
) {
    let Ok(message) = serde_json::from_str::<UpstreamMessage>(text) else {
        return;
    };
    match message {
        UpstreamMessage::Ack {
            operation_id,
            status,
        } => {
            // 匹配 in-flight 命令 → 确认完成。
            if let Some(p) = pending.as_ref() {
                if p.operation_id == operation_id {
                    *pending = None;
                }
            }
            // 快照丢失：operation→snapshot_lost、会话释放归属转 offline（无工作区，
            // 对话仍在 DB，用户可继续发消息重建）。
            if status == "snapshot_lost" {
                let _ = sqlx::query(
                    "UPDATE hub_sessions s
                        SET runtime_owner_id = NULL, lifecycle_status = 'offline',
                            updated_at = now()
                      FROM force_stop_operation o
                     WHERE o.operation_id = $1 AND o.session_id = s.id
                       AND o.state = 'pending'
                       AND s.lifecycle_status = 'force_stopping'",
                )
                .bind(operation_id)
                .execute(&state.pool)
                .await;
                let _ = sqlx::query(
                    "UPDATE force_stop_operation
                        SET state = 'snapshot_lost', updated_at = now()
                      WHERE operation_id = $1 AND state = 'pending'",
                )
                .bind(operation_id)
                .execute(&state.pool)
                .await;
            }
            // status == "ok" 不在此处提交：快照上传成功以 HTTP 上传端点的原子事务为准。
        }
        UpstreamMessage::OwnedSessions { sessions } => {
            reconcile_owned_sessions(state, runtime_id, sessions).await;
        }
    }
}

/// 对比上报持有列表与权威归属：runtime 上报了 hub 认为不属于它的会话 → 推 abandon
///（不要求快照）；hub 认为应停止（force_stopping）且 runtime 持有 → 重推 force_stop。
pub(crate) async fn reconcile_owned_sessions(
    state: &Arc<AppState>,
    runtime_id: Uuid,
    reported: Vec<OwnedSessionReport>,
) {
    if reported.is_empty() {
        return;
    }
    let ids: Vec<Uuid> = reported.iter().map(|r| r.session_id).collect();
    let rows: Vec<(Uuid, String, String)> = sqlx::query_as(
        "SELECT id, lifecycle_status, COALESCE(runtime_owner_id::text, '')
         FROM hub_sessions
         WHERE id = ANY($1)
           AND (lifecycle_status = 'force_stopping'
                OR COALESCE(runtime_owner_id::text, '') <> $2::text)",
    )
    .bind(&ids)
    .bind(runtime_id.to_string())
    .fetch_all(&state.pool)
    .await
    .unwrap_or_default();

    for (session_id, lifecycle, owner) in rows {
        let owned_by_me = owner.parse::<Uuid>().ok() == Some(runtime_id);
        // 非权威持有 → abandon（不要求快照，run_id 用上报的本地 token）；
        // hub 权威应停止（force_stopping 且归本 runtime）→ 重推 force_stop。
        let (kind, require_snapshot, operation_id) = if lifecycle == "force_stopping" && owned_by_me
        {
            let operation_id: Option<Uuid> = sqlx::query_scalar(
                "SELECT operation_id FROM force_stop_operation
                 WHERE session_id = $1 AND state = 'pending' LIMIT 1",
            )
            .bind(session_id)
            .fetch_optional(&state.pool)
            .await
            .unwrap_or(None);
            match operation_id {
                Some(operation_id) => ("force_stop", true, operation_id),
                // 会话 force_stopping 但 operation 已终态（快照已标记丢失/上传已
                // 提交）→ 会话应已释放；上报兜底发 abandon 清理 runtime 残留。
                None => ("abandon", false, Uuid::nil()),
            }
        } else if !owned_by_me {
            ("abandon", false, Uuid::nil())
        } else {
            continue;
        };
        let Some(report) = reported.iter().find(|r| r.session_id == session_id) else {
            continue;
        };
        let message = json!({
            "type": "command",
            "operation_id": operation_id,
            "session_id": session_id,
            "run_id": report.run_id,
            "kind": kind,
            "require_snapshot": require_snapshot,
        });
        state.runtime_ws.push(runtime_id, message.to_string());
    }
}

/// 5 分钟定时兜底：force_stopping 超过 5 分钟未完成 → 标记快照丢失并释放会话。
pub(crate) async fn expire_stuck_force_stops(state: &Arc<AppState>) {
    let rows: Vec<(Uuid, Uuid)> = sqlx::query_as(
        "SELECT o.operation_id, o.session_id
         FROM force_stop_operation o
         JOIN hub_sessions s ON s.id = o.session_id
         WHERE o.state = 'pending'
           AND s.lifecycle_status = 'force_stopping'
           AND o.created_at < now() - interval '5 minutes'",
    )
    .fetch_all(&state.pool)
    .await
    .unwrap_or_default();
    for (operation_id, session_id) in rows {
        let _ = sqlx::query(
            "UPDATE hub_sessions
                SET runtime_owner_id = NULL, lifecycle_status = 'offline', updated_at = now()
              WHERE id = $1 AND lifecycle_status = 'force_stopping'",
        )
        .bind(session_id)
        .execute(&state.pool)
        .await;
        let _ = sqlx::query(
            "UPDATE force_stop_operation
                SET state = 'snapshot_lost', updated_at = now()
              WHERE operation_id = $1 AND state = 'pending'",
        )
        .bind(operation_id)
        .execute(&state.pool)
        .await;
        tracing::warn!(%operation_id, %session_id, "force stop timed out, marked snapshot lost");
    }
}

/// force-stop 提交后推送命令（连接在线时）；断线由上报兜底。
pub(crate) async fn push_force_stop_command(
    state: &Arc<AppState>,
    runtime_id: Uuid,
    operation_id: Uuid,
    session_id: Uuid,
    run_id: Uuid,
) {
    let message = json!({
        "type": "command",
        "operation_id": operation_id,
        "session_id": session_id,
        "run_id": run_id,
        "kind": "force_stop",
        "require_snapshot": true,
    });
    state.runtime_ws.push(runtime_id, message.to_string());
}
