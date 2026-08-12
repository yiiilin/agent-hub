//! automations 领域：自动化 CRUD、触发与调度。

use super::*;
use crate::insert_run_for_agent_tx;
use agent_hub_shared::*;
use axum::extract::{Path, Query, State};
use axum::http::HeaderMap;
use axum::Json;
use chrono::{DateTime, Datelike, Duration as ChronoDuration, Timelike, Utc};
use serde::Deserialize;
use sqlx::{PgPool, Row};
use std::sync::Arc;
use std::time::Duration;
use tracing::warn;
use uuid::Uuid;

pub(crate) async fn list_automations(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
) -> Result<Json<Vec<AutomationDto>>, ApiError> {
    let user = require_user(&state, &headers).await?;
    let rows = sqlx::query(
        "SELECT id, agent_id, owner_id, name, trigger_type, prompt, schedule,
                NULL::text AS webhook_token, enabled, last_triggered_at, created_at
         FROM automations WHERE owner_id = $1 ORDER BY created_at DESC",
    )
    .bind(user.id)
    .fetch_all(&state.pool)
    .await?;
    Ok(Json(rows.into_iter().map(automation_from_row).collect()))
}

#[derive(Debug, Default, Deserialize)]
pub(crate) struct AutomationRunListQuery {
    pub(crate) page: Option<i64>,
    pub(crate) page_size: Option<i64>,
}

impl AutomationRunListQuery {
    pub(crate) fn validated(self) -> Result<(i64, i64, i64), ApiError> {
        let page = self.page.unwrap_or(1);
        let page_size = self.page_size.unwrap_or(20);
        if page < 1 || !(1..=100).contains(&page_size) {
            return Err(ApiError::bad_request("invalid automation run pagination"));
        }
        let offset = page
            .checked_sub(1)
            .and_then(|value| value.checked_mul(page_size))
            .ok_or_else(|| ApiError::bad_request("invalid automation run pagination"))?;
        Ok((page, page_size, offset))
    }
}

pub(crate) async fn list_automation_runs(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    Path(automation_id): Path<Uuid>,
    Query(query): Query<AutomationRunListQuery>,
) -> Result<Json<RunListResponse>, ApiError> {
    let user = require_user(&state, &headers).await?;
    let (page, page_size, offset) = query.validated()?;
    let owned: Option<Uuid> =
        sqlx::query_scalar("SELECT id FROM automations WHERE id = $1 AND owner_id = $2")
            .bind(automation_id)
            .bind(user.id)
            .fetch_optional(&state.pool)
            .await?;
    if owned.is_none() {
        return Err(ApiError::not_found("automation not found"));
    }
    let total = sqlx::query_scalar::<_, i64>("SELECT count(*) FROM runs WHERE automation_id = $1")
        .bind(automation_id)
        .fetch_one(&state.pool)
        .await?;
    let rows = sqlx::query(
        "SELECT id, agent_id, automation_id, integration_session_id, parent_run_id, runtime_id,
                hub_session_id, hub_message_id, hub_turn_id, session_ownership_generation,
                status, initial_message, native_session_id, work_dir_ref, source, created_at, updated_at
         FROM runs
         WHERE automation_id = $1
         ORDER BY created_at DESC, id DESC
         LIMIT $2 OFFSET $3",
    )
    .bind(automation_id)
    .bind(page_size)
    .bind(offset)
    .fetch_all(&state.pool)
    .await?;
    Ok(Json(RunListResponse {
        items: rows.into_iter().map(run_from_row).collect(),
        total,
        page,
        page_size,
    }))
}

pub(crate) async fn create_automation(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    Json(req): Json<CreateAutomationRequest>,
) -> Result<Json<AutomationDto>, ApiError> {
    let user = require_user(&state, &headers).await?;
    if req.name.trim().is_empty() {
        return Err(ApiError::bad_request("automation name is required"));
    }
    let trigger_type = normalize_automation_trigger(&req.trigger_type)?;
    if req.prompt.trim().is_empty() {
        return Err(ApiError::bad_request("automation prompt is required"));
    }
    validate_automation_schedule(trigger_type, req.schedule.as_deref())?;
    let webhook_token = if trigger_type == "webhook" {
        Some(format!(
            "ahw_{}{}",
            Uuid::new_v4().simple(),
            Uuid::new_v4().simple()
        ))
    } else {
        None
    };
    let webhook_token_hash = webhook_token.as_deref().map(sha256_hex);
    let mut tx = state.pool.begin().await?;
    // 先锁 Agent，再插入 Automation；归档、手动触发和 scheduler 都遵循这一顺序。
    // Automation 可绑定当前用户可调用（owner/public/public_to）的 Agent。
    let agent = sqlx::query(
        "SELECT id, owner_id FROM agents
         WHERE id = $1 AND deleted_at IS NULL
           AND 'automation' = ANY(endpoint_exposure)
           AND (owner_id = $2 OR visibility = 'public'
                OR (visibility = 'public_to' AND $2 = ANY(public_to)))
         FOR UPDATE",
    )
    .bind(req.agent_id)
    .bind(user.id)
    .fetch_optional(&mut *tx)
    .await?
    .ok_or(ApiError::forbidden(
        "automation requires an accessible agent",
    ))?;
    let row = sqlx::query(
        "INSERT INTO automations (id, agent_id, owner_id, name, trigger_type, prompt, schedule, webhook_token_hash, enabled)
         VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
         RETURNING id, agent_id, owner_id, name, trigger_type, prompt, schedule,
                   NULL::text AS webhook_token, enabled, last_triggered_at, created_at",
    )
    .bind(Uuid::new_v4())
    .bind(agent.get::<Uuid, _>("id"))
    .bind(user.id)
    .bind(req.name.trim())
    .bind(trigger_type)
    .bind(req.prompt.trim())
    .bind(req.schedule.as_deref().map(str::trim).filter(|value| !value.is_empty()))
    .bind(webhook_token_hash)
    .bind(req.enabled)
    .fetch_one(&mut *tx)
    .await?;
    tx.commit().await?;
    let mut automation = automation_from_row(row);
    automation.webhook_token = webhook_token;
    Ok(Json(automation))
}

pub(crate) async fn update_automation(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    Path(automation_id): Path<Uuid>,
    Json(req): Json<UpdateAutomationRequest>,
) -> Result<Json<AutomationDto>, ApiError> {
    let user = require_user(&state, &headers).await?;
    let mut tx = state.pool.begin().await?;
    let agent_id: Option<Uuid> =
        sqlx::query_scalar("SELECT agent_id FROM automations WHERE id = $1 AND owner_id = $2")
            .bind(automation_id)
            .bind(user.id)
            .fetch_optional(&mut *tx)
            .await?;
    let Some(agent_id) = agent_id else {
        return Err(ApiError::not_found("automation not found"));
    };

    // All Automation mutations and triggers lock Agent before Automation.
    // Agent 必须仍对 Automation owner 可调用（owner/public/public_to）。
    let active_agent: Option<Uuid> = sqlx::query_scalar(
        "SELECT id FROM agents
         WHERE id = $1 AND deleted_at IS NULL
           AND (owner_id = $2 OR visibility = 'public'
                OR (visibility = 'public_to' AND $2 = ANY(public_to)))
         FOR UPDATE",
    )
    .bind(agent_id)
    .bind(user.id)
    .fetch_optional(&mut *tx)
    .await?;
    if active_agent.is_none() {
        return Err(ApiError::not_found("automation not found"));
    }
    let current = sqlx::query(
        "SELECT trigger_type, webhook_token_hash FROM automations
         WHERE id = $1 AND owner_id = $2 AND agent_id = $3
         FOR UPDATE",
    )
    .bind(automation_id)
    .bind(user.id)
    .bind(agent_id)
    .fetch_optional(&mut *tx)
    .await?
    .ok_or(ApiError::not_found("automation not found"))?;

    if req.name.trim().is_empty() {
        return Err(ApiError::bad_request("automation name is required"));
    }
    let trigger_type = normalize_automation_trigger(&req.trigger_type)?;
    if req.prompt.trim().is_empty() {
        return Err(ApiError::bad_request("automation prompt is required"));
    }
    validate_automation_schedule(trigger_type, req.schedule.as_deref())?;

    let previous_trigger: String = current.get("trigger_type");
    let new_webhook_token = (previous_trigger != "webhook" && trigger_type == "webhook")
        .then(|| format!("ahw_{}{}", Uuid::new_v4().simple(), Uuid::new_v4().simple()));
    let webhook_token_hash = match (previous_trigger.as_str(), trigger_type) {
        ("webhook", "webhook") => current.get::<Option<String>, _>("webhook_token_hash"),
        (_, "webhook") => new_webhook_token.as_deref().map(sha256_hex),
        _ => None,
    };
    let row = sqlx::query(
        "UPDATE automations
         SET name = $1, trigger_type = $2, prompt = $3, schedule = $4,
             webhook_token_hash = $5, enabled = $6, updated_at = now()
         WHERE id = $7 AND owner_id = $8
         RETURNING id, agent_id, owner_id, name, trigger_type, prompt, schedule,
                   NULL::text AS webhook_token, enabled, last_triggered_at, created_at",
    )
    .bind(req.name.trim())
    .bind(trigger_type)
    .bind(req.prompt.trim())
    .bind(
        req.schedule
            .as_deref()
            .map(str::trim)
            .filter(|value| !value.is_empty()),
    )
    .bind(webhook_token_hash)
    .bind(req.enabled)
    .bind(automation_id)
    .bind(user.id)
    .fetch_one(&mut *tx)
    .await?;
    tx.commit().await?;
    let mut automation = automation_from_row(row);
    automation.webhook_token = new_webhook_token;
    Ok(Json(automation))
}

pub(crate) async fn trigger_automation(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    Path(automation_id): Path<Uuid>,
    Json(req): Json<TriggerAutomationRequest>,
) -> Result<Json<RunDto>, ApiError> {
    let user = require_user(&state, &headers).await?;
    let automation = load_automation_for_user(&state.pool, automation_id, user.id).await?;
    trigger_loaded_automation(
        &state.pool,
        automation,
        req.message,
        "automation:manual",
        None,
    )
    .await
    .map(Json)
}

pub(crate) async fn trigger_automation_webhook(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    Json(req): Json<TriggerAutomationRequest>,
) -> Result<Json<RunDto>, ApiError> {
    let token = webhook_token_from_headers(&headers)
        .ok_or(ApiError::unauthorized("missing automation webhook token"))?;
    let automation = load_automation_by_webhook_token(&state.pool, &token).await?;
    let token_hash = sha256_hex(&token);
    trigger_loaded_automation(
        &state.pool,
        automation,
        req.message,
        "automation:webhook",
        Some(&token_hash),
    )
    .await
    .map(Json)
}

pub(crate) fn normalize_automation_trigger(trigger_type: &str) -> Result<&'static str, ApiError> {
    match trigger_type.trim() {
        "manual" => Ok("manual"),
        "webhook" => Ok("webhook"),
        "interval" => Ok("interval"),
        "cron" => Ok("cron"),
        _ => Err(ApiError::bad_request("unsupported automation trigger type")),
    }
}

pub(crate) fn validate_automation_schedule(
    trigger_type: &str,
    schedule: Option<&str>,
) -> Result<(), ApiError> {
    let has_schedule = schedule.is_some_and(|value| !value.trim().is_empty());
    match trigger_type {
        "manual" | "webhook" if has_schedule => Err(ApiError::bad_request(
            "schedule is only valid for cron or interval automation",
        )),
        "cron" | "interval" if !has_schedule => Err(ApiError::bad_request(
            "schedule is required for cron or interval automation",
        )),
        "interval" => {
            parse_interval_schedule(schedule.unwrap_or_default())?;
            Ok(())
        }
        "cron" => validate_cron_schedule(schedule.unwrap_or_default()),
        _ => Ok(()),
    }
}

pub(crate) async fn automation_scheduler_loop(pool: PgPool) {
    let mut tick = tokio::time::interval(Duration::from_secs(1));
    loop {
        tick.tick().await;
        if let Err(error) = trigger_due_scheduled_automations(&pool).await {
            warn!(error = %error.message, "automation scheduler scan failed");
        }
    }
}

pub(crate) async fn trigger_due_scheduled_automations(pool: &PgPool) -> Result<(), ApiError> {
    let now = Utc::now();
    let rows = sqlx::query(
        "SELECT au.id
         FROM automations au
         JOIN agents ag ON ag.id = au.agent_id AND ag.deleted_at IS NULL
         WHERE au.enabled = true AND au.trigger_type IN ('interval', 'cron')
         ORDER BY au.created_at ASC",
    )
    .fetch_all(pool)
    .await?;

    for row in rows {
        let automation_id: Uuid = row.get("id");
        if let Err(error) = trigger_scheduled_automation_if_due(pool, automation_id, now).await {
            warn!(
                automation_id = %automation_id,
                error = %error.message,
                "scheduled automation trigger failed"
            );
        }
    }
    Ok(())
}

pub(crate) async fn trigger_scheduled_automation_if_due(
    pool: &PgPool,
    automation_id: Uuid,
    now: DateTime<Utc>,
) -> Result<Option<RunDto>, ApiError> {
    let mut tx = pool.begin().await?;
    // 先用稳定的 Agent 锁避免与归档的反向锁顺序，再锁 Automation 重判 due。
    let agent_id: Option<Uuid> = sqlx::query_scalar(
        "SELECT agent_id FROM automations
         WHERE id = $1 AND enabled = true AND trigger_type IN ('interval', 'cron')",
    )
    .bind(automation_id)
    .fetch_optional(&mut *tx)
    .await?;
    let Some(agent_id) = agent_id else {
        tx.commit().await?;
        return Ok(None);
    };
    let active_agent: Option<Uuid> =
        sqlx::query_scalar("SELECT id FROM agents WHERE id = $1 AND deleted_at IS NULL FOR UPDATE")
            .bind(agent_id)
            .fetch_optional(&mut *tx)
            .await?;
    if active_agent.is_none() {
        tx.commit().await?;
        return Ok(None);
    }
    let row = sqlx::query(
        "SELECT id, agent_id, owner_id, name, trigger_type, prompt, schedule,
                NULL::text AS webhook_token, enabled, last_triggered_at, created_at
         FROM automations
         WHERE id = $1 AND enabled = true AND trigger_type IN ('interval', 'cron')
         FOR UPDATE SKIP LOCKED",
    )
    .bind(automation_id)
    .fetch_optional(&mut *tx)
    .await?;
    let Some(row) = row else {
        return Ok(None);
    };
    let automation = automation_from_row(row);
    if !scheduled_automation_due(&automation, now) {
        tx.commit().await?;
        return Ok(None);
    }
    let run = insert_run_for_agent_tx(
        &mut tx,
        automation.agent_id,
        automation.owner_id,
        automation.prompt.clone(),
        "automation:scheduler",
        Some(automation.id),
        None,
        None,
    )
    .await?;
    sqlx::query("UPDATE automations SET last_triggered_at = $1, updated_at = now() WHERE id = $2")
        .bind(now)
        .bind(automation.id)
        .execute(&mut *tx)
        .await?;
    tx.commit().await?;
    Ok(Some(run))
}

pub(crate) fn scheduled_automation_due(automation: &AutomationDto, now: DateTime<Utc>) -> bool {
    if !automation.enabled {
        return false;
    }
    match automation.trigger_type.as_str() {
        "interval" => automation
            .schedule
            .as_deref()
            .ok_or_else(|| ApiError::bad_request("schedule is required"))
            .and_then(parse_interval_schedule)
            .is_ok_and(|interval| {
                let last = automation
                    .last_triggered_at
                    .unwrap_or(automation.created_at);
                now.signed_duration_since(last) >= interval
            }),
        "cron" => automation.schedule.as_deref().is_some_and(|schedule| {
            cron_schedule_matches(schedule, now)
                && automation
                    .last_triggered_at
                    .is_none_or(|last| minute_key(last) != minute_key(now))
        }),
        _ => false,
    }
}

pub(crate) fn parse_interval_schedule(schedule: &str) -> Result<ChronoDuration, ApiError> {
    let trimmed = schedule.trim();
    if trimmed.len() < 2 {
        return Err(ApiError::bad_request("interval schedule must be like 5m"));
    }
    let (amount, unit) = trimmed.split_at(trimmed.len() - 1);
    let amount: i64 = amount
        .parse()
        .map_err(|_| ApiError::bad_request("interval amount must be a number"))?;
    if amount <= 0 {
        return Err(ApiError::bad_request("interval amount must be positive"));
    }
    match unit {
        "s" => checked_interval_duration(amount, 1),
        "m" => checked_interval_duration(amount, 60),
        "h" => checked_interval_duration(amount, 60 * 60),
        _ => Err(ApiError::bad_request(
            "interval schedule must use s, m, or h",
        )),
    }
}

pub(crate) fn checked_interval_duration(
    amount: i64,
    seconds_per_unit: i64,
) -> Result<ChronoDuration, ApiError> {
    amount
        .checked_mul(seconds_per_unit)
        .and_then(ChronoDuration::try_seconds)
        .ok_or_else(|| ApiError::bad_request("interval schedule is too large"))
}

pub(crate) fn validate_cron_schedule(schedule: &str) -> Result<(), ApiError> {
    let fields = cron_fields(schedule)?;
    for (index, field) in fields.iter().enumerate() {
        if *field == "*" {
            continue;
        }
        let value: u32 = field
            .parse()
            .map_err(|_| ApiError::bad_request("cron fields must be * or a number"))?;
        let valid = match index {
            0 => value <= 59,
            1 => value <= 23,
            2 => (1..=31).contains(&value),
            3 => (1..=12).contains(&value),
            4 => value <= 7,
            _ => false,
        };
        if !valid {
            return Err(ApiError::bad_request("cron field is out of range"));
        }
    }
    Ok(())
}

pub(crate) fn cron_schedule_matches(schedule: &str, now: DateTime<Utc>) -> bool {
    let Ok(fields) = cron_fields(schedule) else {
        return false;
    };
    cron_field_matches(fields[0], now.minute(), 0)
        && cron_field_matches(fields[1], now.hour(), 1)
        && cron_field_matches(fields[2], now.day(), 2)
        && cron_field_matches(fields[3], now.month(), 3)
        && cron_field_matches(fields[4], now.weekday().num_days_from_sunday(), 4)
}

pub(crate) fn cron_fields(schedule: &str) -> Result<Vec<&str>, ApiError> {
    let fields = schedule.split_whitespace().collect::<Vec<_>>();
    if fields.len() != 5 {
        return Err(ApiError::bad_request("cron schedule must have 5 fields"));
    }
    Ok(fields)
}

pub(crate) fn cron_field_matches(field: &str, value: u32, index: usize) -> bool {
    if field == "*" {
        return true;
    }
    field.parse::<u32>().is_ok_and(|expected| {
        let expected = if index == 4 && expected == 7 {
            0
        } else {
            expected
        };
        let valid = match index {
            0 => expected <= 59,
            1 => expected <= 23,
            2 => (1..=31).contains(&expected),
            3 => (1..=12).contains(&expected),
            4 => expected <= 6,
            _ => false,
        };
        valid && expected == value
    })
}

pub(crate) fn minute_key(value: DateTime<Utc>) -> i64 {
    value.timestamp() / 60
}

pub(crate) async fn trigger_loaded_automation(
    pool: &PgPool,
    automation: AutomationDto,
    message: Option<String>,
    source: &str,
    expected_webhook_token_hash: Option<&str>,
) -> Result<RunDto, ApiError> {
    match (source, automation.trigger_type.as_str()) {
        ("automation:manual", "manual") | ("automation:webhook", "webhook") => {}
        ("automation:scheduler", "interval") | ("automation:scheduler", "cron") => {}
        ("automation:manual", _) => {
            return Err(ApiError::forbidden(
                "only manual automation can be triggered here",
            ));
        }
        ("automation:webhook", _) => {
            return Err(ApiError::forbidden(
                "only webhook automation can be triggered here",
            ));
        }
        ("automation:scheduler", _) => {
            return Err(ApiError::forbidden(
                "only scheduled automation can be triggered here",
            ));
        }
        _ => {
            return Err(ApiError::bad_request(
                "unsupported automation trigger source",
            ))
        }
    }
    let message = message
        .as_deref()
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .unwrap_or(&automation.prompt)
        .to_owned();
    let mut tx = pool.begin().await?;
    // 归档和所有触发路径均先锁 Agent，消除 agent/automation 交叉死锁。
    // Agent 必须仍对 Automation owner 可调用（owner/public/public_to）。
    let active_agent = sqlx::query(
        "SELECT id
         FROM agents
         WHERE id = $1 AND deleted_at IS NULL
           AND (owner_id = $2 OR visibility = 'public'
                OR (visibility = 'public_to' AND $2 = ANY(public_to)))
         FOR UPDATE",
    )
    .bind(automation.agent_id)
    .bind(automation.owner_id)
    .fetch_optional(&mut *tx)
    .await?;
    let active_automation = sqlx::query(
        "SELECT enabled, trigger_type, webhook_token_hash
         FROM automations
         WHERE id = $1 AND owner_id = $2
         FOR UPDATE",
    )
    .bind(automation.id)
    .bind(automation.owner_id)
    .fetch_optional(&mut *tx)
    .await?;
    if active_agent.is_none() {
        return Err(ApiError::forbidden(
            "automation is disabled or agent is deleted",
        ));
    }
    let Some(active_automation) = active_automation else {
        return Err(ApiError::forbidden(
            "automation is disabled or agent is deleted",
        ));
    };
    let token_matches = expected_webhook_token_hash.is_none_or(|expected| {
        active_automation
            .get::<Option<String>, _>("webhook_token_hash")
            .as_deref()
            == Some(expected)
    });
    if !active_automation.get::<bool, _>("enabled")
        || active_automation.get::<String, _>("trigger_type") != automation.trigger_type
        || !token_matches
    {
        return Err(if expected_webhook_token_hash.is_some() {
            ApiError::unauthorized("invalid automation webhook token")
        } else {
            ApiError::forbidden("automation is disabled or agent is deleted")
        });
    }
    let run = insert_run_for_agent_tx(
        &mut tx,
        automation.agent_id,
        automation.owner_id,
        message,
        source,
        Some(automation.id),
        None,
        None,
    )
    .await?;
    sqlx::query(
        "UPDATE automations SET last_triggered_at = now(), updated_at = now() WHERE id = $1",
    )
    .bind(automation.id)
    .execute(&mut *tx)
    .await?;
    tx.commit().await?;
    Ok(run)
}

pub(crate) async fn load_automation_for_user(
    pool: &PgPool,
    automation_id: Uuid,
    user_id: Uuid,
) -> Result<AutomationDto, ApiError> {
    let row = sqlx::query(
        "SELECT id, agent_id, owner_id, name, trigger_type, prompt, schedule,
                NULL::text AS webhook_token, enabled, last_triggered_at, created_at
         FROM automations WHERE id = $1 AND owner_id = $2",
    )
    .bind(automation_id)
    .bind(user_id)
    .fetch_optional(pool)
    .await?;
    row.map(automation_from_row)
        .ok_or(ApiError::not_found("automation not found"))
}

pub(crate) async fn load_automation_by_webhook_token(
    pool: &PgPool,
    token: &str,
) -> Result<AutomationDto, ApiError> {
    let row = sqlx::query(
        "SELECT id, agent_id, owner_id, name, trigger_type, prompt, schedule,
                NULL::text AS webhook_token, enabled, last_triggered_at, created_at
         FROM automations WHERE webhook_token_hash = $1",
    )
    .bind(sha256_hex(token))
    .fetch_optional(pool)
    .await?;
    row.map(automation_from_row)
        .ok_or(ApiError::unauthorized("invalid automation webhook token"))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::api::support::test_util::*;
    use axum::http::StatusCode;
    use std::collections::BTreeSet;

    #[test]
    fn automation_run_pagination_is_one_based_and_bounded() {
        assert_eq!(
            AutomationRunListQuery::default().validated().unwrap(),
            (1, 20, 0)
        );
        assert_eq!(
            AutomationRunListQuery {
                page: Some(3),
                page_size: Some(100),
            }
            .validated()
            .unwrap(),
            (3, 100, 200)
        );
        for query in [
            AutomationRunListQuery {
                page: Some(0),
                page_size: None,
            },
            AutomationRunListQuery {
                page: None,
                page_size: Some(0),
            },
            AutomationRunListQuery {
                page: None,
                page_size: Some(101),
            },
            AutomationRunListQuery {
                page: Some(i64::MAX),
                page_size: Some(100),
            },
        ] {
            assert_eq!(
                query.validated().unwrap_err().status,
                StatusCode::BAD_REQUEST
            );
        }
    }

    #[sqlx::test(migrations = "./migrations")]
    #[ignore = "requires DATABASE_URL and PostgreSQL CREATE DATABASE privilege"]
    async fn automation_update_enforces_owner_validation_and_webhook_secrecy(pool: PgPool) {
        let fixture = automation_update_fixture(pool).await;
        let state = Arc::new(test_state_with_browser_session_auth(fixture.pool.clone()));
        let update =
            |trigger_type: &str, schedule: Option<&str>, enabled: bool| UpdateAutomationRequest {
                name: " Updated automation ".into(),
                trigger_type: trigger_type.into(),
                prompt: " Updated prompt ".into(),
                schedule: schedule.map(str::to_owned),
                enabled,
            };

        let Json(manual) = update_automation(
            State(state.clone()),
            session_headers(&fixture.owner_session),
            Path(fixture.automation_id),
            Json(update("manual", None, false)),
        )
        .await
        .unwrap();
        assert_eq!(manual.name, "Updated automation");
        assert_eq!(manual.prompt, "Updated prompt");
        assert!(!manual.enabled);
        assert!(manual.webhook_token.is_none());

        let foreign = update_automation(
            State(state.clone()),
            session_headers(&fixture.foreign_session),
            Path(fixture.automation_id),
            Json(update("manual", None, true)),
        )
        .await
        .unwrap_err();
        assert_eq!(foreign.status, StatusCode::NOT_FOUND);

        for invalid in [
            UpdateAutomationRequest {
                name: " ".into(),
                ..update("manual", None, true)
            },
            UpdateAutomationRequest {
                prompt: " ".into(),
                ..update("manual", None, true)
            },
            update("unknown", None, true),
            update("interval", Some("later"), true),
            update("cron", None, true),
        ] {
            let error = update_automation(
                State(state.clone()),
                session_headers(&fixture.owner_session),
                Path(fixture.automation_id),
                Json(invalid),
            )
            .await
            .unwrap_err();
            assert_eq!(error.status, StatusCode::BAD_REQUEST);
        }

        let Json(webhook) = update_automation(
            State(state.clone()),
            session_headers(&fixture.owner_session),
            Path(fixture.automation_id),
            Json(update("webhook", None, true)),
        )
        .await
        .unwrap();
        let token = webhook.webhook_token.expect("new webhook token");
        let first_hash: Option<String> =
            sqlx::query_scalar("SELECT webhook_token_hash FROM automations WHERE id = $1")
                .bind(fixture.automation_id)
                .fetch_one(&fixture.pool)
                .await
                .unwrap();
        assert_eq!(first_hash.as_deref(), Some(sha256_hex(&token).as_str()));

        let Json(webhook_again) = update_automation(
            State(state.clone()),
            session_headers(&fixture.owner_session),
            Path(fixture.automation_id),
            Json(update("webhook", None, true)),
        )
        .await
        .unwrap();
        assert!(webhook_again.webhook_token.is_none());
        let preserved_hash: Option<String> =
            sqlx::query_scalar("SELECT webhook_token_hash FROM automations WHERE id = $1")
                .bind(fixture.automation_id)
                .fetch_one(&fixture.pool)
                .await
                .unwrap();
        assert_eq!(preserved_hash, first_hash);

        let Json(interval) = update_automation(
            State(state.clone()),
            session_headers(&fixture.owner_session),
            Path(fixture.automation_id),
            Json(update("interval", Some("5m"), true)),
        )
        .await
        .unwrap();
        assert_eq!(interval.schedule.as_deref(), Some("5m"));
        let cleared_hash: Option<String> =
            sqlx::query_scalar("SELECT webhook_token_hash FROM automations WHERE id = $1")
                .bind(fixture.automation_id)
                .fetch_one(&fixture.pool)
                .await
                .unwrap();
        assert!(cleared_hash.is_none());
        let listed = list_automations(State(state), session_headers(&fixture.owner_session))
            .await
            .unwrap()
            .0;
        assert!(listed.iter().all(|item| item.webhook_token.is_none()));
    }

    #[sqlx::test(migrations = "./migrations")]
    #[ignore = "requires DATABASE_URL and PostgreSQL CREATE DATABASE privilege"]
    async fn automation_run_history_tracks_sources_owner_pagination_and_deletion(pool: PgPool) {
        let owner_id = Uuid::new_v4();
        let foreign_id = Uuid::new_v4();
        let agent_id = Uuid::new_v4();
        let owner_session = format!("ahs_{}", Uuid::new_v4().simple());
        let foreign_session = format!("ahs_{}", Uuid::new_v4().simple());
        let unique = Uuid::new_v4().simple().to_string();
        for (id, label) in [(owner_id, "owner"), (foreign_id, "foreign")] {
            sqlx::query(
                "INSERT INTO users (id, email, password, display_name, role)
                 VALUES ($1, $2, 'unused', $3, 'member')",
            )
            .bind(id)
            .bind(format!("automation-history-{label}-{unique}@example.com"))
            .bind(format!("automation-history-{label}-{unique}"))
            .execute(&pool)
            .await
            .unwrap();
        }
        for (token, user_id) in [(&owner_session, owner_id), (&foreign_session, foreign_id)] {
            sqlx::query(
                "INSERT INTO sessions (token_hash, user_id, expires_at)
                 VALUES ($1, $2, now() + interval '1 hour')",
            )
            .bind(sha256_hex(token))
            .bind(user_id)
            .execute(&pool)
            .await
            .unwrap();
        }
        sqlx::query(
            "INSERT INTO agents (id, owner_id, name, instructions, visibility)
             VALUES ($1, $2, 'Automation History Agent', 'test', 'private')",
        )
        .bind(agent_id)
        .bind(owner_id)
        .execute(&pool)
        .await
        .unwrap();
        attach_test_model_connection(&pool, agent_id, owner_id, "automation-history-model").await;
        let history_index: String = sqlx::query_scalar(
            "SELECT indexdef FROM pg_indexes
             WHERE schemaname = current_schema() AND indexname = 'runs_automation_created_idx'",
        )
        .fetch_one(&pool)
        .await
        .unwrap();
        assert!(history_index.contains("automation_id, created_at DESC, id DESC"));

        let manual_id = Uuid::new_v4();
        let webhook_id = Uuid::new_v4();
        let interval_id = Uuid::new_v4();
        let cron_id = Uuid::new_v4();
        let webhook_token = format!("ahw_{}", Uuid::new_v4().simple());
        for (id, trigger_type, schedule, token_hash) in [
            (manual_id, "manual", None, None),
            (
                webhook_id,
                "webhook",
                None,
                Some(sha256_hex(&webhook_token)),
            ),
            (interval_id, "interval", Some("1s"), None),
            (cron_id, "cron", Some("* * * * *"), None),
        ] {
            sqlx::query(
                "INSERT INTO automations
                 (id, agent_id, owner_id, name, trigger_type, prompt, schedule,
                  webhook_token_hash, enabled, created_at)
                 VALUES ($1, $2, $3, $4, $5, 'history prompt', $6, $7, true,
                         now() - interval '2 minutes')",
            )
            .bind(id)
            .bind(agent_id)
            .bind(owner_id)
            .bind(format!("{trigger_type} history"))
            .bind(trigger_type)
            .bind(schedule)
            .bind(token_hash)
            .execute(&pool)
            .await
            .unwrap();
        }

        let manual = load_automation_for_user(&pool, manual_id, owner_id)
            .await
            .unwrap();
        let manual_run = trigger_loaded_automation(
            &pool,
            manual,
            Some("manual history".into()),
            "automation:manual",
            None,
        )
        .await
        .unwrap();
        let webhook = load_automation_by_webhook_token(&pool, &webhook_token)
            .await
            .unwrap();
        let webhook_run = trigger_loaded_automation(
            &pool,
            webhook,
            Some("webhook history".into()),
            "automation:webhook",
            Some(&sha256_hex(&webhook_token)),
        )
        .await
        .unwrap();
        let interval_run = trigger_scheduled_automation_if_due(&pool, interval_id, Utc::now())
            .await
            .unwrap()
            .unwrap();
        let cron_run = trigger_scheduled_automation_if_due(&pool, cron_id, Utc::now())
            .await
            .unwrap()
            .unwrap();
        let ordinary_run = create_run_for_agent(
            &pool,
            agent_id,
            owner_id,
            "ordinary run".into(),
            "console",
            None,
            None,
            None,
        )
        .await
        .unwrap();
        assert_eq!(manual_run.automation_id, Some(manual_id));
        assert_eq!(webhook_run.automation_id, Some(webhook_id));
        assert_eq!(interval_run.automation_id, Some(interval_id));
        assert_eq!(cron_run.automation_id, Some(cron_id));
        assert!(ordinary_run.automation_id.is_none());
        let automation_sessions = [
            manual_run.hub_session_id.unwrap(),
            webhook_run.hub_session_id.unwrap(),
            interval_run.hub_session_id.unwrap(),
            cron_run.hub_session_id.unwrap(),
        ];
        assert_eq!(automation_sessions.iter().collect::<BTreeSet<_>>().len(), 4);
        assert_eq!(
            sqlx::query_scalar::<_, i64>(
                "SELECT count(*) FROM hub_sessions
                 WHERE id = ANY($1) AND origin_kind = 'hub_native'",
            )
            .bind(automation_sessions.to_vec())
            .fetch_one(&pool)
            .await
            .unwrap(),
            4,
        );

        let second_manual = trigger_loaded_automation(
            &pool,
            load_automation_for_user(&pool, manual_id, owner_id)
                .await
                .unwrap(),
            Some("manual history two".into()),
            "automation:manual",
            None,
        )
        .await
        .unwrap();
        let stable_time = Utc::now() - ChronoDuration::seconds(10);
        sqlx::query("UPDATE runs SET created_at = $1 WHERE automation_id = $2")
            .bind(stable_time)
            .bind(manual_id)
            .execute(&pool)
            .await
            .unwrap();
        let state = Arc::new(test_state_with_browser_session_auth(pool.clone()));
        let Json(page_one) = list_automation_runs(
            State(state.clone()),
            session_headers(&owner_session),
            Path(manual_id),
            Query(AutomationRunListQuery {
                page: Some(1),
                page_size: Some(1),
            }),
        )
        .await
        .unwrap();
        assert_eq!(page_one.total, 2);
        assert_eq!(page_one.items.len(), 1);
        assert_eq!(page_one.items[0].automation_id, Some(manual_id));
        let expected_latest = if manual_run.id > second_manual.id {
            &manual_run
        } else {
            &second_manual
        };
        assert_eq!(
            page_one.items[0].hub_session_id,
            expected_latest.hub_session_id
        );
        assert_eq!(page_one.items[0].hub_turn_id, expected_latest.hub_turn_id);
        assert_eq!(
            page_one.items[0].session_ownership_generation,
            expected_latest.session_ownership_generation
        );
        assert_eq!(
            page_one.items[0].id,
            std::cmp::max(manual_run.id, second_manual.id)
        );
        let foreign = list_automation_runs(
            State(state),
            session_headers(&foreign_session),
            Path(manual_id),
            Query(AutomationRunListQuery::default()),
        )
        .await
        .unwrap_err();
        assert_eq!(foreign.status, StatusCode::NOT_FOUND);
        let missing = list_automation_runs(
            State(Arc::new(test_state_with_browser_session_auth(pool.clone()))),
            session_headers(&owner_session),
            Path(Uuid::new_v4()),
            Query(AutomationRunListQuery::default()),
        )
        .await
        .unwrap_err();
        assert_eq!(missing.status, StatusCode::NOT_FOUND);

        sqlx::query("DELETE FROM automations WHERE id = $1")
            .bind(manual_id)
            .execute(&pool)
            .await
            .unwrap();
        let retained: (i64, i64) =
            sqlx::query_as("SELECT count(*), count(automation_id) FROM runs WHERE id = ANY($1)")
                .bind(vec![manual_run.id, second_manual.id])
                .fetch_one(&pool)
                .await
                .unwrap();
        assert_eq!(retained, (2, 0));
    }
}
