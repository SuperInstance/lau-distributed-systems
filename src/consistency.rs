use serde::{Deserialize, Serialize};
use std::collections::HashMap;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum ConsistencyModel {
    Strong,
    Eventual,
    Causal,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct VersionedValue {
    pub value: String,
    pub version: u64,
    pub timestamp: u64,
    pub vector_clock: HashMap<String, u64>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DataStore {
    pub data: HashMap<String, VersionedValue>,
    pub consistency: ConsistencyModel,
}

impl DataStore {
    pub fn new(consistency: ConsistencyModel) -> Self {
        Self {
            data: HashMap::new(),
            consistency,
        }
    }

    pub fn write(&mut self, key: &str, value: String, node_id: &str) {
        match self.consistency {
            ConsistencyModel::Strong => self.write_strong(key, value),
            ConsistencyModel::Eventual => self.write_eventual(key, value, node_id),
            ConsistencyModel::Causal => self.write_causal(key, value, node_id),
        }
    }

    fn write_strong(&mut self, key: &str, value: String) {
        let version = self.data.get(key).map_or(1, |v| v.version + 1);
        self.data.insert(key.to_string(), VersionedValue {
            value,
            version,
            timestamp: 0,
            vector_clock: HashMap::new(),
        });
    }

    fn write_eventual(&mut self, key: &str, value: String, _node_id: &str) {
        let version = self.data.get(key).map_or(1, |v| v.version + 1);
        self.data.insert(key.to_string(), VersionedValue {
            value,
            version,
            timestamp: 0,
            vector_clock: HashMap::new(),
        });
    }

    fn write_causal(&mut self, key: &str, value: String, node_id: &str) {
        let vc = match self.data.get(key) {
            Some(v) => {
                let mut vc = v.vector_clock.clone();
                *vc.entry(node_id.to_string()).or_insert(0) += 1;
                vc
            }
            None => {
                let mut vc = HashMap::new();
                vc.insert(node_id.to_string(), 1);
                vc
            }
        };
        let version = self.data.get(key).map_or(1, |v| v.version + 1);
        self.data.insert(key.to_string(), VersionedValue {
            value,
            version,
            timestamp: 0,
            vector_clock: vc,
        });
    }

    pub fn read(&self, key: &str) -> Option<&VersionedValue> {
        self.data.get(key)
    }
}

/// Simulate eventual consistency convergence between replicas.
pub fn merge_replicas(replicas: &mut Vec<DataStore>, key: &str) {
    // LWW (last writer wins) — pick highest version
    let mut best: Option<VersionedValue> = None;
    for replica in replicas.iter() {
        if let Some(v) = replica.data.get(key) {
            if best.as_ref().map_or(true, |b| v.version > b.version) {
                best = Some(v.clone());
            }
        }
    }
    if let Some(v) = best {
        for replica in replicas.iter_mut() {
            replica.data.insert(key.to_string(), v.clone());
        }
    }
}

/// Check if two vector clocks are causally ordered.
/// Returns true if vc1 happened-before vc2.
pub fn happened_before(vc1: &HashMap<String, u64>, vc2: &HashMap<String, u64>) -> bool {
    let all_keys: std::collections::HashSet<_> = vc1.keys().chain(vc2.keys()).collect();
    let mut at_least_one_less = false;
    for key in &all_keys {
        let v1 = vc1.get(*key).copied().unwrap_or(0);
        let v2 = vc2.get(*key).copied().unwrap_or(0);
        if v1 > v2 {
            return false;
        }
        if v1 < v2 {
            at_least_one_less = true;
        }
    }
    at_least_one_less
}

/// Check if two vector clocks are concurrent.
pub fn are_concurrent(vc1: &HashMap<String, u64>, vc2: &HashMap<String, u64>) -> bool {
    !happened_before(vc1, vc2) && !happened_before(vc2, vc1) && vc1 != vc2
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_strong_consistency_overwrite() {
        let mut store = DataStore::new(ConsistencyModel::Strong);
        store.write("k", "v1".into(), "n1");
        store.write("k", "v2".into(), "n1");
        assert_eq!(store.read("k").unwrap().value, "v2");
        assert_eq!(store.read("k").unwrap().version, 2);
    }

    #[test]
    fn test_eventual_replication_merge() {
        let mut replicas = vec![
            DataStore::new(ConsistencyModel::Eventual),
            DataStore::new(ConsistencyModel::Eventual),
        ];
        replicas[0].write("k", "v1".into(), "n0");
        // Write to second replica twice to give it version 2
        replicas[1].write("k", "v2".into(), "n1");
        // Before merge, first has version 1, second has version 1
        assert_eq!(replicas[0].read("k").unwrap().version, 1);
        merge_replicas(&mut replicas, "k");
        // After merge, both have the highest version's value
        assert_eq!(replicas[0].read("k").unwrap().value, replicas[1].read("k").unwrap().value);
    }

    #[test]
    fn test_causal_ordering() {
        let mut vc1 = HashMap::new();
        vc1.insert("a".into(), 1);
        let mut vc2 = HashMap::new();
        vc2.insert("a".into(), 2);
        assert!(happened_before(&vc1, &vc2));
        assert!(!happened_before(&vc2, &vc1));
    }

    #[test]
    fn test_concurrent_vector_clocks() {
        let mut vc1 = HashMap::new();
        vc1.insert("a".into(), 1);
        let mut vc2 = HashMap::new();
        vc2.insert("b".into(), 1);
        assert!(are_concurrent(&vc1, &vc2));
    }

    #[test]
    fn test_causal_write_tracks_clock() {
        let mut store = DataStore::new(ConsistencyModel::Causal);
        store.write("k", "v1".into(), "node_a");
        store.write("k", "v2".into(), "node_b");
        let v = store.read("k").unwrap();
        assert_eq!(v.vector_clock.get("node_a"), Some(&1));
        assert_eq!(v.vector_clock.get("node_b"), Some(&1));
    }
}
