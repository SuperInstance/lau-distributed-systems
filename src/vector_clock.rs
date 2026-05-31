use serde::{Deserialize, Serialize};
use std::collections::HashMap;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct VectorClock {
    pub clock: HashMap<String, u64>,
}

impl VectorClock {
    pub fn new() -> Self {
        Self { clock: HashMap::new() }
    }

    pub fn increment(&mut self, node_id: &str) {
        *self.clock.entry(node_id.to_string()).or_insert(0) += 1;
    }

    pub fn get(&self, node_id: &str) -> u64 {
        self.clock.get(node_id).copied().unwrap_or(0)
    }

    pub fn merge(&self, other: &VectorClock) -> VectorClock {
        let mut result = self.clock.clone();
        for (key, value) in &other.clock {
            let entry = result.entry(key.clone()).or_insert(0);
            *entry = (*entry).max(*value);
        }
        VectorClock { clock: result }
    }

    /// Returns Ordering but we use custom for partial order.
    /// - Some(true) if self happened before other
    /// - Some(false) if other happened before self
    /// - None if concurrent
    pub fn compare(&self, other: &VectorClock) -> Option<bool> {
        let all_keys: std::collections::HashSet<_> = self.clock.keys().chain(other.clock.keys()).collect();
        let mut self_less = false;
        let mut other_less = false;
        for key in &all_keys {
            let sv = self.clock.get(*key).copied().unwrap_or(0);
            let ov = other.clock.get(*key).copied().unwrap_or(0);
            if sv < ov { self_less = true; }
            if sv > ov { other_less = true; }
        }
        if self_less && !other_less { return Some(true); }
        if other_less && !self_less { return Some(false); }
        if !self_less && !other_less { return Some(false); } // equal
        None // concurrent
    }

    pub fn is_concurrent_with(&self, other: &VectorClock) -> bool {
        self.compare(other).is_none()
    }

    pub fn happened_before(&self, other: &VectorClock) -> bool {
        matches!(self.compare(other), Some(true))
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ConflictEntry {
    pub key: String,
    pub values: Vec<(String, VectorClock)>, // (value, clock)
}

/// Resolve conflicts using last-writer-wins (highest counter sum).
pub fn resolve_lww(conflict: &ConflictEntry) -> String {
    conflict.values
        .iter()
        .max_by_key(|(_, vc)| vc.clock.values().sum::<u64>())
        .map(|(v, _)| v.clone())
        .unwrap_or_default()
}

/// Resolve conflicts by merging all vector clocks and picking first.
pub fn resolve_merge(conflict: &ConflictEntry) -> (String, VectorClock) {
    let merged = conflict.values.iter()
        .fold(VectorClock::new(), |acc, (_, vc)| acc.merge(vc));
    let value = conflict.values.first().map(|(v, _)| v.clone()).unwrap_or_default();
    (value, merged)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_increment() {
        let mut vc = VectorClock::new();
        vc.increment("a");
        vc.increment("a");
        vc.increment("b");
        assert_eq!(vc.get("a"), 2);
        assert_eq!(vc.get("b"), 1);
        assert_eq!(vc.get("c"), 0);
    }

    #[test]
    fn test_merge() {
        let mut vc1 = VectorClock::new();
        vc1.increment("a");
        let mut vc2 = VectorClock::new();
        vc2.increment("b");
        let merged = vc1.merge(&vc2);
        assert_eq!(merged.get("a"), 1);
        assert_eq!(merged.get("b"), 1);
    }

    #[test]
    fn test_happened_before() {
        let mut vc1 = VectorClock::new();
        vc1.increment("a");
        let mut vc2 = vc1.clone();
        vc2.increment("a");
        assert!(vc1.happened_before(&vc2));
        assert!(!vc2.happened_before(&vc1));
    }

    #[test]
    fn test_concurrent() {
        let mut vc1 = VectorClock::new();
        vc1.increment("a");
        let mut vc2 = VectorClock::new();
        vc2.increment("b");
        assert!(vc1.is_concurrent_with(&vc2));
    }

    #[test]
    fn test_lww_resolution() {
        let mut vc1 = VectorClock::new();
        vc1.increment("a");
        let mut vc2 = VectorClock::new();
        vc2.increment("a");
        vc2.increment("b");
        let conflict = ConflictEntry {
            key: "k".into(),
            values: vec![
                ("old".into(), vc1),
                ("new".into(), vc2),
            ],
        };
        assert_eq!(resolve_lww(&conflict), "new");
    }

    #[test]
    fn test_equal_clocks() {
        let mut vc1 = VectorClock::new();
        vc1.increment("a");
        let mut vc2 = VectorClock::new();
        vc2.increment("a");
        assert!(!vc1.is_concurrent_with(&vc2));
        assert!(!vc1.happened_before(&vc2));
    }
}
