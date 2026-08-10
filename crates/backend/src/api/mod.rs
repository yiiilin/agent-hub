//! HTTP API 领域模块。
//!
//! 从巨型 main.rs 按领域拆分：每个模块包含该领域的 handler、
//! 私有辅助函数与（阶段 B 的）测试；`pub use *` 统一 re-export，
//! 使 main.rs 的 `use api::*` 保持全部调用不变。

pub mod admin;
pub mod agents;
pub mod auth;
pub mod automations;
pub mod integrations;
pub mod models;
pub mod runtimes;
pub mod secrets;
pub mod sessions;
pub mod skills;
pub mod support;

pub use admin::*;
pub use agents::*;
pub use auth::*;
pub use automations::*;
pub use integrations::*;
pub use models::*;
pub use runtimes::*;
pub use secrets::*;
pub use sessions::*;
pub use skills::*;
pub use support::*;
