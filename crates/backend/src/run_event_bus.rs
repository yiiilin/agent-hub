use std::{
    collections::{HashMap, HashSet, VecDeque},
    sync::Mutex,
};

use agent_hub_shared::RunEventDto;
use uuid::Uuid;

/// Synthetic sequence base for events that are streamed live but not
/// persisted. Real `run_events.seq` values are small positive BIGSERIAL
/// numbers, so a large base keeps live deltas unique and ordered without
/// ever colliding with persisted rows. The Hub will not realistically reach
/// one trillion persisted events.
pub const DELTA_SEQ_BASE: i64 = 1_000_000_000_000;

/// A single event delivered to live subscribers. `persisted` marks rows
/// that were written to `run_events` (their `seq` is the database anchor);
/// streaming deltas are delivered only here.
#[derive(Debug, Clone)]
pub struct RunEventBusItem {
    pub event: RunEventDto,
    pub persisted: bool,
}

/// Fan-out for live Run events. The in-memory implementation is the default;
/// a Redis pub/sub implementation can replace it for multi-instance Hub
/// deployments without touching the HTTP or SSE layers.
pub trait RunEventBus: Send + Sync {
    /// Allocates a monotonic stream sequence for a non-persisted delta.
    fn next_stream_seq(&self, run_id: Uuid) -> i64;
    /// Broadcasts one event to the Run's live subscribers.
    fn publish(&self, run_id: Uuid, event: RunEventDto, persisted: bool);
    /// Subscribes to the Run's live event stream.
    fn subscribe(&self, run_id: Uuid) -> tokio::sync::broadcast::Receiver<RunEventBusItem>;
}

struct InMemoryEntry {
    sender: tokio::sync::broadcast::Sender<RunEventBusItem>,
    delta_seq: i64,
    recent_event_ids: VecDeque<Uuid>,
    recent_event_id_set: HashSet<Uuid>,
}

const RECENT_EVENT_ID_LIMIT: usize = 512;

#[derive(Default)]
pub struct InMemoryRunEventBus {
    inner: Mutex<HashMap<Uuid, InMemoryEntry>>,
}

impl InMemoryRunEventBus {
    fn entry(&self) -> std::sync::MutexGuard<'_, HashMap<Uuid, InMemoryEntry>> {
        self.inner
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
    }
}

impl RunEventBus for InMemoryRunEventBus {
    fn next_stream_seq(&self, run_id: Uuid) -> i64 {
        let mut inner = self.entry();
        let entry = inner.entry(run_id).or_insert_with(|| InMemoryEntry {
            sender: tokio::sync::broadcast::channel(8192).0,
            delta_seq: 0,
            recent_event_ids: VecDeque::new(),
            recent_event_id_set: HashSet::new(),
        });
        entry.delta_seq += 1;
        DELTA_SEQ_BASE + entry.delta_seq
    }

    fn publish(&self, run_id: Uuid, event: RunEventDto, persisted: bool) {
        let mut inner = self.entry();
        if let Some(entry) = inner.get_mut(&run_id) {
            // Idempotent fan-out: a retried upload reuses the same event id,
            // so dedupe in memory before broadcasting. Persisted phase/message
            // rows carry the same id and also pass through here once.
            if !entry.recent_event_id_set.insert(event.event_id) {
                return;
            }
            entry.recent_event_ids.push_back(event.event_id);
            if entry.recent_event_ids.len() > RECENT_EVENT_ID_LIMIT {
                if let Some(evicted) = entry.recent_event_ids.pop_front() {
                    entry.recent_event_id_set.remove(&evicted);
                }
            }
            if entry.sender.receiver_count() > 0 {
                let _ = entry.sender.send(RunEventBusItem { event, persisted });
                return;
            }
        }
        // No live subscriber: drop the channel so finished Runs do not
        // accumulate in-memory state.
        inner.remove(&run_id);
    }

    fn subscribe(&self, run_id: Uuid) -> tokio::sync::broadcast::Receiver<RunEventBusItem> {
        let mut inner = self.entry();
        let entry = inner.entry(run_id).or_insert_with(|| InMemoryEntry {
            sender: tokio::sync::broadcast::channel(8192).0,
            delta_seq: 0,
            recent_event_ids: VecDeque::new(),
            recent_event_id_set: HashSet::new(),
        });
        entry.sender.subscribe()
    }
}

/// True for events that exist only to stream incremental output and are
/// replaced by the completed phase/message row.
pub fn is_streaming_delta(event_type: &str, payload: &serde_json::Value) -> bool {
    if event_type == "message_delta" {
        return true;
    }
    if event_type == "item" {
        return matches!(
            payload.get("phase").and_then(serde_json::Value::as_str),
            Some("summary_delta") | Some("output_delta")
        );
    }
    false
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn event(seq: i64) -> RunEventDto {
        RunEventDto {
            seq,
            event_id: Uuid::new_v4(),
            run_id: Uuid::new_v4(),
            event_type: "item".into(),
            role: Some("assistant".into()),
            content: None,
            payload: json!({}),
            created_at: chrono::Utc::now(),
        }
    }

    #[test]
    fn in_memory_bus_sequences_and_broadcasts_deltas() {
        let bus = InMemoryRunEventBus::default();
        let run_id = Uuid::new_v4();
        let seq_a = bus.next_stream_seq(run_id);
        let seq_b = bus.next_stream_seq(run_id);
        assert!(seq_a >= DELTA_SEQ_BASE && seq_b > seq_a);
        let mut rx = bus.subscribe(run_id);
        bus.publish(run_id, event(seq_a), false);
        bus.publish(run_id, event(5), true);
        let first = rx.blocking_recv().unwrap();
        let second = rx.blocking_recv().unwrap();
        assert!(!first.persisted);
        assert!(second.persisted);
    }

    #[test]
    fn delta_detection_matches_streaming_phases() {
        assert!(is_streaming_delta("message_delta", &json!({})));
        assert!(is_streaming_delta(
            "item",
            &json!({ "phase": "summary_delta" })
        ));
        assert!(is_streaming_delta(
            "item",
            &json!({ "phase": "output_delta" })
        ));
        assert!(!is_streaming_delta(
            "item",
            &json!({ "phase": "completed" })
        ));
        assert!(!is_streaming_delta("message", &json!({})));
    }
}
