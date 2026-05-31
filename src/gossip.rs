use serde::{Deserialize, Serialize};
use std::collections::HashMap;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GossipNode {
    pub id: String,
    pub known_rumors: HashMap<String, u64>, // rumor_id -> version
    pub round: u64,
}

impl GossipNode {
    pub fn new(id: String) -> Self {
        Self {
            id,
            known_rumors: HashMap::new(),
            round: 0,
        }
    }

    pub fn add_rumor(&mut self, rumor_id: &str) {
        self.known_rumors.entry(rumor_id.to_string()).or_insert(1);
    }

    pub fn has_rumor(&self, rumor_id: &str) -> bool {
        self.known_rumors.contains_key(rumor_id)
    }

    /// Exchange rumors with another node (push-pull).
    pub fn exchange(&mut self, other: &mut GossipNode) -> (usize, usize) {
        self.round += 1;
        other.round += 1;
        let mut sent = 0;
        let mut received = 0;
        // Push to other
        for (rumor, ver) in &self.known_rumors {
            if !other.known_rumors.contains_key(rumor) {
                other.known_rumors.insert(rumor.clone(), *ver);
                sent += 1;
            }
        }
        // Pull from other
        for (rumor, ver) in &other.known_rumors {
            if !self.known_rumors.contains_key(rumor) {
                self.known_rumors.insert(rumor.clone(), *ver);
                received += 1;
            }
        }
        (sent, received)
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GossipNetwork {
    pub nodes: Vec<GossipNode>,
    pub fanout: usize,
}

impl GossipNetwork {
    pub fn new(node_count: usize, fanout: usize) -> Self {
        Self {
            nodes: (0..node_count).map(|i| GossipNode::new(format!("n{}", i))).collect(),
            fanout,
        }
    }

    /// Seed a rumor at a specific node.
    pub fn seed_rumor(&mut self, node_idx: usize, rumor_id: &str) {
        self.nodes[node_idx].add_rumor(rumor_id);
    }

    /// Run one round of gossip: each node exchanges with `fanout` random peers.
    /// Returns the number of nodes that know all rumors.
    pub fn round(&mut self) -> usize {
        let n = self.nodes.len();
        let mut pairs: Vec<(usize, usize)> = Vec::new();
        for i in 0..n {
            for _ in 0..self.fanout {
                let j = (i + 1 + (self.nodes[i].round as usize + 1) % (n - 1).max(1)) % n;
                if i != j {
                    pairs.push((i, j));
                }
            }
        }
        for (i, j) in &pairs {
            if i == j {
                continue;
            }
            // Collect exchanges as transfers to avoid double mutable borrow
            let new_for_i: Vec<(String, u64)> = self.nodes[*j].known_rumors.iter()
                .filter(|(k, _)| !self.nodes[*i].known_rumors.contains_key(*k))
                .map(|(k, v)| (k.clone(), *v))
                .collect();
            let new_for_j: Vec<(String, u64)> = self.nodes[*i].known_rumors.iter()
                .filter(|(k, _)| !self.nodes[*j].known_rumors.contains_key(*k))
                .map(|(k, v)| (k.clone(), *v))
                .collect();
            for (k, v) in new_for_i {
                self.nodes[*i].known_rumors.insert(k, v);
            }
            for (k, v) in new_for_j {
                self.nodes[*j].known_rumors.insert(k, v);
            }
            self.nodes[*i].round += 1;
            self.nodes[*j].round += 1;
        }
        // Count fully informed nodes
        let total_rumors: usize = self.nodes.iter()
            .flat_map(|n| n.known_rumors.keys())
            .collect::<std::collections::HashSet<_>>()
            .len();
        self.nodes.iter()
            .filter(|n| n.known_rumors.len() >= total_rumors)
            .count()
    }

    /// Run gossip until convergence (all nodes know all rumors) or max rounds.
    pub fn run_until_convergence(&mut self, max_rounds: usize) -> usize {
        for r in 0..max_rounds {
            let informed = self.round();
            if informed == self.nodes.len() {
                return r + 1;
            }
        }
        max_rounds
    }

    pub fn convergence_ratio(&self) -> f64 {
        let total_rumors: usize = self.nodes.iter()
            .flat_map(|n| n.known_rumors.keys())
            .collect::<std::collections::HashSet<_>>()
            .len();
        if total_rumors == 0 {
            return 1.0;
        }
        let informed = self.nodes.iter()
            .filter(|n| n.known_rumors.len() >= total_rumors)
            .count();
        informed as f64 / self.nodes.len() as f64
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_single_rumor_spread() {
        let mut net = GossipNetwork::new(10, 3);
        net.seed_rumor(0, "rumor1");
        let rounds = net.run_until_convergence(50);
        assert!(rounds <= 50);
        for node in &net.nodes {
            assert!(node.has_rumor("rumor1"));
        }
    }

    #[test]
    fn test_multiple_rumors() {
        let mut net = GossipNetwork::new(10, 2);
        net.seed_rumor(0, "r1");
        net.seed_rumor(3, "r2");
        net.seed_rumor(7, "r3");
        net.run_until_convergence(100);
        for node in &net.nodes {
            assert!(node.has_rumor("r1"));
            assert!(node.has_rumor("r2"));
            assert!(node.has_rumor("r3"));
        }
    }

    #[test]
    fn test_convergence_ratio() {
        let mut net = GossipNetwork::new(5, 2);
        net.seed_rumor(0, "r1");
        assert!(net.convergence_ratio() < 1.0);
        net.run_until_convergence(50);
        assert!((net.convergence_ratio() - 1.0).abs() < 0.01);
    }

    #[test]
    fn test_exchange_bidirectional() {
        let mut a = GossipNode::new("a".into());
        let mut b = GossipNode::new("b".into());
        a.add_rumor("r1");
        b.add_rumor("r2");
        a.exchange(&mut b);
        assert!(a.has_rumor("r2"));
        assert!(b.has_rumor("r1"));
    }

    #[test]
    fn test_no_duplicate_exchange() {
        let mut a = GossipNode::new("a".into());
        let mut b = GossipNode::new("b".into());
        a.add_rumor("r1");
        a.exchange(&mut b);
        let (sent, _) = a.exchange(&mut b);
        assert_eq!(sent, 0);
    }
}
