//! 通用小工具：角色判定与 JSON 规范化。

use serde_json::Value;

pub(crate) fn is_admin_role(role: &str) -> bool {
    matches!(role, "admin" | "super_admin")
}

pub(crate) fn compact_json(value: &Value) -> String {
    let text = serde_json::to_string(value).unwrap_or_else(|_| "{}".into());
    let mut chars = text.chars();
    let truncated = chars.by_ref().take(1000).collect::<String>();
    if chars.next().is_some() {
        format!("{truncated}...")
    } else {
        text
    }
}

pub(crate) fn canonical_json(value: &Value) -> String {
    fn write(value: &Value, output: &mut String) {
        match value {
            Value::Null => output.push_str("null"),
            Value::Bool(value) => output.push_str(if *value { "true" } else { "false" }),
            Value::Number(value) => output.push_str(&value.to_string()),
            Value::String(value) => output.push_str(
                &serde_json::to_string(value).expect("JSON strings are always serializable"),
            ),
            Value::Array(values) => {
                output.push('[');
                for (index, value) in values.iter().enumerate() {
                    if index > 0 {
                        output.push(',');
                    }
                    write(value, output);
                }
                output.push(']');
            }
            Value::Object(values) => {
                output.push('{');
                let mut keys = values.keys().collect::<Vec<_>>();
                keys.sort_unstable();
                for (index, key) in keys.into_iter().enumerate() {
                    if index > 0 {
                        output.push(',');
                    }
                    output.push_str(
                        &serde_json::to_string(key).expect("JSON object keys are serializable"),
                    );
                    output.push(':');
                    write(&values[key], output);
                }
                output.push('}');
            }
        }
    }

    let mut output = String::new();
    write(value, &mut output);
    output
}
