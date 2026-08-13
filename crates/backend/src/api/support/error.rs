//! HTTP API 统一错误类型。

use agent_hub_shared::SecretGrantRequirementDto;
use axum::http::{header, HeaderValue, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::Json;
use serde_json::{json, Value};

#[derive(Debug)]
pub(crate) struct ApiError {
    pub(crate) status: StatusCode,
    pub(crate) message: String,
    pub(crate) retry_after_seconds: Option<u64>,
    pub(crate) details: Option<Value>,
    /// 结构化错误码（credential_revoked / stale_session_generation），
    /// 供执行程序按码分类处理（区别于裸 401/403 判断）。
    pub(crate) code: Option<&'static str>,
}

impl ApiError {
    pub(crate) fn bad_request(message: impl Into<String>) -> Self {
        Self {
            status: StatusCode::BAD_REQUEST,
            message: message.into(),
            retry_after_seconds: None,
            details: None,
            code: None,
        }
    }

    pub(crate) fn unauthorized(message: impl Into<String>) -> Self {
        Self {
            status: StatusCode::UNAUTHORIZED,
            message: message.into(),
            retry_after_seconds: None,
            details: None,
            code: None,
        }
    }

    pub(crate) fn forbidden(message: impl Into<String>) -> Self {
        Self {
            status: StatusCode::FORBIDDEN,
            message: message.into(),
            retry_after_seconds: None,
            details: None,
            code: None,
        }
    }

    pub(crate) fn conflict(message: impl Into<String>) -> Self {
        Self {
            status: StatusCode::CONFLICT,
            message: message.into(),
            retry_after_seconds: None,
            details: None,
            code: None,
        }
    }

    pub(crate) fn gone(message: impl Into<String>) -> Self {
        Self {
            status: StatusCode::GONE,
            message: message.into(),
            retry_after_seconds: None,
            details: None,
            code: None,
        }
    }

    pub(crate) fn not_found(message: impl Into<String>) -> Self {
        Self {
            status: StatusCode::NOT_FOUND,
            message: message.into(),
            retry_after_seconds: None,
            details: None,
            code: None,
        }
    }

    pub(crate) fn internal(message: impl Into<String>) -> Self {
        Self {
            status: StatusCode::INTERNAL_SERVER_ERROR,
            message: message.into(),
            retry_after_seconds: None,
            details: None,
            code: None,
        }
    }

    pub(crate) fn bad_gateway(message: impl Into<String>) -> Self {
        Self {
            status: StatusCode::BAD_GATEWAY,
            message: message.into(),
            retry_after_seconds: None,
            details: None,
            code: None,
        }
    }

    pub(crate) fn service_unavailable(message: impl Into<String>) -> Self {
        Self {
            status: StatusCode::SERVICE_UNAVAILABLE,
            message: message.into(),
            retry_after_seconds: None,
            details: None,
            code: None,
        }
    }

    pub(crate) fn gateway_timeout(message: impl Into<String>) -> Self {
        Self {
            status: StatusCode::GATEWAY_TIMEOUT,
            message: message.into(),
            retry_after_seconds: None,
            details: None,
            code: None,
        }
    }

    pub(crate) fn too_many_requests(message: impl Into<String>, retry_after_seconds: u64) -> Self {
        Self {
            status: StatusCode::TOO_MANY_REQUESTS,
            message: message.into(),
            retry_after_seconds: Some(retry_after_seconds),
            details: None,
            code: None,
        }
    }

    pub(crate) fn payload_too_large(message: impl Into<String>) -> Self {
        Self {
            status: StatusCode::PAYLOAD_TOO_LARGE,
            message: message.into(),
            retry_after_seconds: None,
            details: None,
            code: None,
        }
    }

    pub(crate) fn requires_secret_grants(requirements: Vec<SecretGrantRequirementDto>) -> Self {
        Self {
            status: StatusCode::from_u16(428).unwrap_or(StatusCode::BAD_REQUEST),
            message: "the Agent requires additional Secret Grants".into(),
            retry_after_seconds: None,
            details: Some(json!({ "secret_grants_required": requirements })),
            code: None,
        }
    }

    pub(crate) fn with_code(mut self, code: &'static str) -> Self {
        self.code = Some(code);
        self
    }
}

impl IntoResponse for ApiError {
    fn into_response(self) -> Response {
        let mut body = json!({ "error": self.message });
        if let Some(code) = self.code {
            body["code"] = Value::String(code.to_string());
        }
        if let Some(details) = self.details {
            body["details"] = details;
        }
        let mut response = (self.status, Json(body)).into_response();
        if let Some(retry_after_seconds) = self.retry_after_seconds {
            if let Ok(value) = HeaderValue::from_str(&retry_after_seconds.to_string()) {
                response.headers_mut().insert(header::RETRY_AFTER, value);
            }
        }
        response
    }
}

impl From<sqlx::Error> for ApiError {
    fn from(value: sqlx::Error) -> Self {
        tracing::error!(error = %value, "database error");
        ApiError::internal("database error")
    }
}
