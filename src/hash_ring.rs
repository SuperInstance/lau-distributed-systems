use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::hash::{Hash, Hasher};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct VirtualNode {
    pub id: usize,
    pub physical_node: String,
    pub position: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HashRing {
    pub nodes: Vec<VirtualNode>,
    pub vnodes_per_node: usize,
}

impl HashRing {
    pub fn new(vnodes_per_node: usize) -> Self {
        Self {
            nodes: Vec::new(),
            vnodes_per_node,
        }
    }

    pub fn add_node(&mut self, name: &str) {
        for i in 0..self.vnodes_per_node {
            let position = self.hash_vnode(name, i);
            self.nodes.push(VirtualNode {
                id: i,
                physical_node: name.to_string(),
                position,
            });
        }
        self.nodes.sort_by_key(|n| n.position);
    }

    pub fn remove_node(&mut self, name: &str) {
        self.nodes.retain(|n| n.physical_node != name);
    }

    pub fn get_node(&self, key: &str) -> Option<&str> {
        if self.nodes.is_empty() {
            return None;
        }
        let hash = self.hash_key(key);
        let idx = self.nodes.partition_point(|n| n.position < hash);
        if idx >= self.nodes.len() {
            Some(&self.nodes[0].physical_node)
        } else {
            Some(&self.nodes[idx].physical_node)
        }
    }

    fn hash_key(&self, key: &str) -> u64 {
        let mut hasher = std::collections::hash_map::DefaultHasher::new();
        key.hash(&mut hasher);
        hasher.finish()
    }

    fn hash_vnode(&self, name: &str, index: usize) -> u64 {
        let mut hasher = std::collections::hash_map::DefaultHasher::new();
        format!("{}:{}", name, index).hash(&mut hasher);
        hasher.finish()
    }

    /// Analyze key distribution across nodes for a set of keys.
    pub fn distribution(&self, keys: &[&str]) -> HashMap<String, usize> {
        let mut dist: HashMap<String, usize> = HashMap::new();
        for key in keys {
            if let Some(node) = self.get_node(key) {
                *dist.entry(node.to_string()).or_insert(0) += 1;
            }
        }
        dist
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_add_node() {
        let mut ring = HashRing::new(100);
        ring.add_node("node_a");
        assert_eq!(ring.nodes.len(), 100);
    }

    #[test]
    fn test_remove_node() {
        let mut ring = HashRing::new(50);
        ring.add_node("node_a");
        ring.add_node("node_b");
        assert_eq!(ring.nodes.len(), 100);
        ring.remove_node("node_a");
        assert_eq!(ring.nodes.len(), 50);
    }

    #[test]
    fn test_key_assignment() {
        let mut ring = HashRing::new(100);
        ring.add_node("node_a");
        ring.add_node("node_b");
        // Same key always maps to same node
        let n1 = ring.get_node("my_key");
        let n2 = ring.get_node("my_key");
        assert_eq!(n1, n2);
    }

    #[test]
    fn test_distribution_balanced() {
        let mut ring = HashRing::new(150);
        ring.add_node("a");
        ring.add_node("b");
        ring.add_node("c");
        let keys: Vec<String> = (0..1000).map(|i| format!("key_{}", i)).collect();
        let key_refs: Vec<&str> = keys.iter().map(|s| s.as_str()).collect();
        let dist = ring.distribution(&key_refs);
        for (_, count) in &dist {
            assert!(*count > 100, "Distribution too skewed: {}", count);
        }
    }

    #[test]
    fn test_consistent_hashing_on_remove() {
        let mut ring = HashRing::new(100);
        ring.add_node("a");
        ring.add_node("b");
        ring.add_node("c");
        let before: Vec<String> = (0..100)
            .map(|i| ring.get_node(&format!("key_{}", i)).unwrap_or("").to_string())
            .collect();
        ring.remove_node("b");
        let after: Vec<String> = (0..100)
            .map(|i| ring.get_node(&format!("key_{}", i)).unwrap_or("").to_string())
            .collect();
        let mut unchanged = 0;
        for i in 0..100 {
            if before[i] == after[i] {
                unchanged += 1;
            }
        }
        assert!(unchanged >= 40);
    }

    #[test]
    fn test_empty_ring_returns_none() {
        let ring = HashRing::new(100);
        assert!(ring.get_node("key").is_none());
    }
}
