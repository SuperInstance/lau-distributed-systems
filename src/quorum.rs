use serde::{Deserialize, Serialize};
use std::collections::HashMap;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct QuorumConfig {
    pub n: usize,          // total replicas
    pub read_quorum: usize,
    pub write_quorum: usize,
}

impl QuorumConfig {
    pub fn new(n: usize, read_quorum: usize, write_quorum: usize) -> Self {
        Self { n, read_quorum, write_quorum }
    }

    /// Standard: R + W > N ensures consistency
    pub fn is_valid(&self) -> bool {
        self.read_quorum + self.write_quorum > self.n
    }

    /// Maximum failures tolerable for reads
    pub fn read_fault_tolerance(&self) -> usize {
        self.n - self.read_quorum
    }

    /// Maximum failures tolerable for writes
    pub fn write_fault_tolerance(&self) -> usize {
        self.n - self.write_quorum
    }

    /// Can tolerate f failures and still serve both reads and writes?
    pub fn tolerates_failures(&self, f: usize) -> bool {
        self.read_fault_tolerance() >= f && self.write_fault_tolerance() >= f
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct QuorumNode {
    pub id: String,
    pub data: HashMap<String, (String, u64)>, // key -> (value, version)
    pub alive: bool,
}

impl QuorumNode {
    pub fn new(id: String) -> Self {
        Self { id, data: HashMap::new(), alive: true }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct QuorumSystem {
    pub nodes: Vec<QuorumNode>,
    pub config: QuorumConfig,
}

impl QuorumSystem {
    pub fn new(n: usize) -> Self {
        let w = n / 2 + 1;
        let r = n / 2 + 1;
        Self {
            nodes: (0..n).map(|i| QuorumNode::new(format!("n{}", i))).collect(),
            config: QuorumConfig::new(n, r, w),
        }
    }

    pub fn with_quorums(n: usize, r: usize, w: usize) -> Self {
        Self {
            nodes: (0..n).map(|i| QuorumNode::new(format!("n{}", i))).collect(),
            config: QuorumConfig::new(n, r, w),
        }
    }

    /// Write to a quorum of nodes.
    pub fn write(&mut self, key: &str, value: &str) -> bool {
        let version = self.nodes.iter()
            .filter_map(|n| n.data.get(key).map(|(_, v)| *v))
            .max()
            .unwrap_or(0) + 1;
        let mut written = 0;
        for node in &mut self.nodes {
            if node.alive {
                node.data.insert(key.to_string(), (value.to_string(), version));
                written += 1;
                if written >= self.config.write_quorum {
                    return true;
                }
            }
        }
        false
    }

    /// Read from a quorum of nodes, returning the latest version.
    pub fn read(&self, key: &str) -> Option<String> {
        let mut responses: Vec<(String, u64)> = Vec::new();
        for node in &self.nodes {
            if node.alive {
                if let Some((value, version)) = node.data.get(key) {
                    responses.push((value.clone(), *version));
                } else {
                    responses.push(("".to_string(), 0));
                }
                if responses.len() >= self.config.read_quorum {
                    break;
                }
            }
        }
        if responses.len() < self.config.read_quorum {
            return None;
        }
        responses.into_iter().max_by_key(|(_, v)| *v).map(|(v, _)| v)
    }

    pub fn kill_node(&mut self, idx: usize) {
        if idx < self.nodes.len() {
            self.nodes[idx].alive = false;
        }
    }

    pub fn alive_count(&self) -> usize {
        self.nodes.iter().filter(|n| n.alive).count()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_quorum_validity() {
        let config = QuorumConfig::new(5, 3, 3);
        assert!(config.is_valid()); // 3 + 3 = 6 > 5
    }

    #[test]
    fn test_quorum_invalid() {
        let config = QuorumConfig::new(5, 2, 2);
        assert!(!config.is_valid()); // 2 + 2 = 4 <= 5
    }

    #[test]
    fn test_read_write_basic() {
        let mut system = QuorumSystem::new(5);
        assert!(system.write("k", "v1"));
        assert_eq!(system.read("k"), Some("v1".to_string()));
    }

    #[test]
    fn test_quorum_intersection() {
        // R + W > N means any read quorum intersects any write quorum
        let config = QuorumConfig::new(3, 2, 2);
        assert!(config.is_valid());
    }

    #[test]
    fn test_fault_tolerance() {
        let config = QuorumConfig::new(5, 3, 3);
        assert!(config.tolerates_failures(2));
        assert!(!config.tolerates_failures(3));
    }

    #[test]
    fn test_survives_one_failure() {
        let mut system = QuorumSystem::new(3);
        system.kill_node(0);
        assert!(system.write("k", "v1"));
        assert_eq!(system.read("k"), Some("v1".to_string()));
    }

    #[test]
    fn test_read_after_overwrite() {
        let mut system = QuorumSystem::new(5);
        system.write("k", "v1");
        system.write("k", "v2");
        assert_eq!(system.read("k"), Some("v2".to_string()));
    }

    #[test]
    fn test_cannot_write_with_too_many_failures() {
        let mut system = QuorumSystem::new(3);
        system.kill_node(0);
        system.kill_node(1);
        assert!(!system.write("k", "v1"));
    }
}
