use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum NodeState {
    Alive,
    Dead,
    InElection,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BullyNode {
    pub id: usize,
    pub alive: bool,
    pub coordinator: Option<usize>,
}

impl BullyNode {
    pub fn new(id: usize) -> Self {
        Self { id, alive: true, coordinator: None }
    }

    /// Bully algorithm: the node with highest ID among alive nodes becomes coordinator.
    pub fn start_election(nodes: &mut Vec<BullyNode>, initiator: usize) -> Option<usize> {
        if !nodes[initiator].alive {
            return None;
        }
        // Find all alive nodes with higher ID
        let higher: Vec<usize> = nodes.iter()
            .filter(|n| n.alive && n.id > nodes[initiator].id)
            .map(|n| n.id)
            .collect();

        if higher.is_empty() {
            // This node is the bully (highest alive)
            let coord = nodes[initiator].id;
            for node in nodes.iter_mut() {
                if node.alive {
                    node.coordinator = Some(coord);
                }
            }
            Some(coord)
        } else {
            // Higher nodes exist; one of them will take over
            // Find highest alive node
            let coord = nodes.iter()
                .filter(|n| n.alive)
                .map(|n| n.id)
                .max()?;
            for node in nodes.iter_mut() {
                if node.alive {
                    node.coordinator = Some(coord);
                }
            }
            Some(coord)
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RingNode {
    pub id: usize,
    pub alive: bool,
    pub next_id: Option<usize>,
    pub coordinator: Option<usize>,
}

impl RingNode {
    pub fn new(id: usize) -> Self {
        Self { id, alive: true, next_id: None, coordinator: None }
    }

    /// Ring algorithm: pass election message around the ring; highest ID wins.
    pub fn start_election(nodes: &mut Vec<RingNode>, initiator: usize) -> Option<usize> {
        if !nodes[initiator].alive {
            return None;
        }
        let n = nodes.len();
        let mut participants: Vec<usize> = vec![nodes[initiator].id];
        let mut idx = (initiator + 1) % n;
        while idx != initiator {
            if nodes[idx].alive {
                participants.push(nodes[idx].id);
            }
            idx = (idx + 1) % n;
        }
        let coord = *participants.iter().max()?;
        for node in nodes.iter_mut() {
            if node.alive {
                node.coordinator = Some(coord);
            }
        }
        Some(coord)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_bully_highest_alive_wins() {
        let mut nodes: Vec<BullyNode> = (0..5).map(|i| BullyNode::new(i)).collect();
        let coord = BullyNode::start_election(&mut nodes, 0);
        assert_eq!(coord, Some(4));
        assert_eq!(nodes[0].coordinator, Some(4));
    }

    #[test]
    fn test_bully_skips_dead_nodes() {
        let mut nodes: Vec<BullyNode> = (0..5).map(|i| BullyNode::new(i)).collect();
        nodes[4].alive = false;
        nodes[3].alive = false;
        let coord = BullyNode::start_election(&mut nodes, 0);
        assert_eq!(coord, Some(2));
    }

    #[test]
    fn test_bully_mid_node_initiates() {
        let mut nodes: Vec<BullyNode> = (0..5).map(|i| BullyNode::new(i)).collect();
        let coord = BullyNode::start_election(&mut nodes, 2);
        assert_eq!(coord, Some(4));
    }

    #[test]
    fn test_ring_election() {
        let mut nodes: Vec<RingNode> = (0..5).map(|i| RingNode::new(i)).collect();
        let coord = RingNode::start_election(&mut nodes, 0);
        assert_eq!(coord, Some(4));
    }

    #[test]
    fn test_ring_with_dead_nodes() {
        let mut nodes: Vec<RingNode> = (0..5).map(|i| RingNode::new(i)).collect();
        nodes[4].alive = false;
        let coord = RingNode::start_election(&mut nodes, 0);
        assert_eq!(coord, Some(3));
    }

    #[test]
    fn test_ring_single_node() {
        let mut nodes = vec![RingNode::new(0)];
        let coord = RingNode::start_election(&mut nodes, 0);
        assert_eq!(coord, Some(0));
    }

    #[test]
    fn test_bully_single_node() {
        let mut nodes = vec![BullyNode::new(0)];
        let coord = BullyNode::start_election(&mut nodes, 0);
        assert_eq!(coord, Some(0));
    }
}
