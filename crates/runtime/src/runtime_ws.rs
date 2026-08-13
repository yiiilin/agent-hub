//! runtime WebSocket 通道：连接 hub、10 秒上报持有会话列表、
//! 接收 force_stop/abandon 命令并执行（杀 Pi → 打包快照 → 上传 → ack）。
//!
//! ack 协议：命令接管成功（record 移除 + 杀 Pi 已开始）→ 立即 ack `ok`；
//! 后台打包/上传失败（3 次）→ 回传 ack `snapshot_lost`。WS 主循环保持
//! 收发，不阻塞在上传上。

use std::sync::Arc;
use std::time::Duration;

use anyhow::Context;
use futures_util::{SinkExt, StreamExt};
use serde::Deserialize;
use serde_json::json;
use tokio::sync::mpsc;
use tokio_tungstenite::tungstenite::client::IntoClientRequest;
use tokio_tungstenite::tungstenite::http::HeaderValue;
use tokio_tungstenite::tungstenite::Message;
use uuid::Uuid;

use crate::session_bundle::{create_session_bundle, SessionBundleCreateSpec};
use crate::{HubClient, SessionPaths, SessionSupervisorManager};

const REPORT_INTERVAL: Duration = Duration::from_secs(10);
const MAX_UPLOAD_ATTEMPTS: u32 = 3;

#[derive(Debug, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
enum DownstreamMessage {
    Command {
        operation_id: Uuid,
        session_id: Uuid,
        run_id: Option<Uuid>,
        kind: String,
        require_snapshot: bool,
    },
}

/// 后台任务回传的延迟 ack（上传失败 → snapshot_lost）。
struct LateAck {
    operation_id: Uuid,
    status: &'static str,
}

/// 后台循环：连接（带重连退避）→ 周期上报 → 处理命令 → ack。
pub(crate) async fn runtime_ws_loop(
    config: crate::Config,
    client: HubClient,
    manager: Arc<SessionSupervisorManager>,
) {
    let mut backoff = Duration::from_secs(1);
    loop {
        if let Err(error) = run_ws_connection(&config, &client, &manager).await {
            tracing::warn!(error = %error, "runtime WebSocket connection ended");
        }
        tokio::time::sleep(backoff).await;
        backoff = (backoff * 2).min(Duration::from_secs(30));
    }
}

async fn run_ws_connection(
    config: &crate::Config,
    client: &HubClient,
    manager: &Arc<SessionSupervisorManager>,
) -> anyhow::Result<()> {
    let ws_base = if let Some(rest) = client.hub_url.strip_prefix("http://") {
        format!("ws://{rest}")
    } else if let Some(rest) = client.hub_url.strip_prefix("https://") {
        format!("wss://{rest}")
    } else {
        format!("ws://{}", client.hub_url)
    };
    let url = format!("{ws_base}/api/runtime/ws");
    let mut request = url
        .into_client_request()
        .context("build WebSocket request")?;
    let token = client.runtime_credential();
    request.headers_mut().insert(
        "Authorization",
        HeaderValue::from_str(&format!("Bearer {token}"))
            .context("build WebSocket Authorization header")?,
    );
    let (mut socket, _) = tokio_tungstenite::connect_async(request)
        .await
        .context("connect runtime WebSocket")?;
    tracing::info!("runtime WebSocket connected");

    let (late_ack_tx, mut late_ack_rx) = mpsc::channel::<LateAck>(16);
    let mut report_deadline = tokio::time::Instant::now() + REPORT_INTERVAL;
    loop {
        tokio::select! {
            _ = tokio::time::sleep_until(report_deadline) => {
                let sessions: Vec<serde_json::Value> = manager
                    .owned_session_reports()
                    .into_iter()
                    .map(|(session_id, run_id)| json!({ "session_id": session_id, "run_id": run_id }))
                    .collect();
                let message = json!({ "type": "owned_sessions", "sessions": sessions });
                if socket
                    .send(Message::Text(message.to_string().into()))
                    .await
                    .is_err()
                {
                    break;
                }
                report_deadline = tokio::time::Instant::now() + REPORT_INTERVAL;
            }
            late = late_ack_rx.recv() => {
                let Some(late) = late else { break };
                let ack = json!({
                    "type": "ack",
                    "operation_id": late.operation_id,
                    "status": late.status,
                });
                if socket.send(Message::Text(ack.to_string().into())).await.is_err() {
                    break;
                }
            }
            incoming = socket.next() => {
                match incoming {
                    Some(Ok(Message::Text(text))) => {
                        let Ok(message) = serde_json::from_str::<DownstreamMessage>(&text) else {
                            continue;
                        };
                        let DownstreamMessage::Command {
                            operation_id,
                            session_id,
                            run_id,
                            kind,
                            require_snapshot,
                        } = message;
                        // 接管（同步、短）：record 移除 + 杀 Pi。fence 不匹配/已不存在
                        // → 幂等 ok，不启后台任务。abandon 命令 run_id 为 nil（无
                        // operation）时按会话存在性移除。
                        // abandon 无 operation/run（run_id null）→ 按会话存在性移除。
                        if !manager.force_stop_session(session_id, run_id) {
                            let ack = json!({
                                "type": "ack",
                                "operation_id": operation_id,
                                "status": "ok",
                            });
                            if socket.send(Message::Text(ack.to_string().into())).await.is_err() {
                                break;
                            }
                            continue;
                        };
                        // 接管成功：先 spawn 快照任务（杀 Pi 后打包/上传可能耗时，
                        // 且 ack 发送失败不得丢弃快照），再发 ack ok（清 hub 侧 pending）。
                        let config = config.clone();
                        let client = client.clone();
                        let manager = Arc::clone(manager);
                        let late_ack_tx = late_ack_tx.clone();
                        tokio::spawn(async move {
                            let status = execute_force_stop(
                                &config, &client, &manager,
                                operation_id, session_id, require_snapshot,
                            )
                            .await;
                            if status == "snapshot_lost" {
                                let _ = late_ack_tx
                                    .send(LateAck { operation_id, status })
                                    .await;
                            }
                        });
                        let ack = json!({
                            "type": "ack",
                            "operation_id": operation_id,
                            "status": "ok",
                        });
                        if socket.send(Message::Text(ack.to_string().into())).await.is_err() {
                            break;
                        }
                    }
                    Some(Ok(Message::Close(_))) | None => break,
                    Some(Ok(_)) => {}
                    Some(Err(_)) => break,
                }
            }
        }
    }
    Ok(())
}

/// 打包工作区快照并上传（重试 3 次）。返回 "ok" 或 "snapshot_lost"。
async fn execute_force_stop(
    config: &crate::Config,
    client: &HubClient,
    _manager: &Arc<SessionSupervisorManager>,
    operation_id: Uuid,
    session_id: Uuid,
    require_snapshot: bool,
) -> &'static str {
    let session_root = SessionPaths::for_session(&config.work_root, session_id).root;
    let cleanup = || {
        let session_root = session_root.clone();
        async move {
            if let Err(error) = tokio::fs::remove_dir_all(&session_root).await {
                if error.kind() != std::io::ErrorKind::NotFound {
                    tracing::warn!(%session_id, error = %error, "force stop session cleanup failed");
                }
            }
        }
    };

    if !require_snapshot {
        // abandon：不要求快照，直接清理。
        cleanup().await;
        tracing::info!(%session_id, "session abandoned on hub command");
        return "ok";
    }

    // 打包工作区快照（force-stop 快照：manifest 仅 session + workspace + 格式版本）。
    let workspace = SessionPaths::for_session(&config.work_root, session_id).workspace;
    if !workspace.is_dir() {
        cleanup().await;
        return "ok";
    }
    let archive_path = config
        .work_root
        .join("bundle-staging")
        .join(format!("force-stop-{operation_id}.tar.zst"));
    let spec = SessionBundleCreateSpec {
        session_id,
        history_checkpoint: 0,
        bundle_generation: 0,
        ownership_generation: 0,
        producing_engine_version: String::new(),
        created_at: chrono::Utc::now(),
        workspace,
        archive_path: archive_path.clone(),
        force_stop_snapshot: true,
    };
    // 打包是 CPU/IO 密集操作：移出 async 运行时线程。
    let artifact = match tokio::task::spawn_blocking(move || create_session_bundle(&spec))
        .await
        .context("force stop bundle task stopped")
    {
        Ok(Ok(artifact)) => artifact,
        Ok(Err(error)) | Err(error) => {
            tracing::error!(%session_id, error = %error, "force stop bundle creation failed");
            let _ = tokio::fs::remove_file(&archive_path).await;
            cleanup().await;
            return "snapshot_lost";
        }
    };

    // 上传重试 3 次；全部失败 → 快照丢失。
    let mut attempts = 0;
    loop {
        match client
            .upload_force_stop_bundle(operation_id, &artifact)
            .await
        {
            Ok(()) => break,
            Err(error) => {
                attempts += 1;
                if attempts >= MAX_UPLOAD_ATTEMPTS {
                    tracing::error!(
                        %session_id, %operation_id, error = %error,
                        "force stop bundle upload failed after {attempts} attempts; snapshot lost"
                    );
                    let _ = tokio::fs::remove_file(&artifact.archive_path).await;
                    cleanup().await;
                    return "snapshot_lost";
                }
                tracing::warn!(%session_id, %operation_id, error = %error,
                    "force stop bundle upload failed (attempt {attempts}/3); retrying");
                tokio::time::sleep(Duration::from_secs(2 * u64::from(attempts))).await;
            }
        }
    }
    // 上传成功：删除 staging 归档（避免每次硬停残留）。
    let _ = tokio::fs::remove_file(&artifact.archive_path).await;
    cleanup().await;
    tracing::info!(%session_id, %operation_id, "force stop snapshot uploaded");
    "ok"
}
