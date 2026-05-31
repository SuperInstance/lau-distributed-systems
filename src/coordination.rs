use serde::{Deserialize, Serialize};
use std::collections::HashMap;

use crate::consensus::raft::{RaftNode, simulate_election, replicate_entry};
use crate::consistency::{DataStore, ConsistencyModel, merge_replicas};
use crate::vector_clock::VectorClock;

/// An agent in a distributed fleet.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Agent {
    pub id: String,
    pub state: HashMap<String, String>,
    pub vector_clock: VectorClock,
    pub role: AgentRole,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum AgentRole {
    Leader,
    Follower,
    Candidate,
}

impl Agent {
    pub fn new(id: String) -> Self {
        Self {
            id,
            state: HashMap::new(),
            vector_clock: VectorClock::new(),
            role: AgentRole::Follower,
        }
    }

    pub fn set(&mut self, key: &str, value: &str) {
        self.vector_clock.increment(&self.id);
        self.state.insert(key.to_string(), value.to_string());
    }

    pub fn get(&self, key: &str) -> Option<&str> {
        self.state.get(key).map(|s| s.as_str())
    }
}

/// A fleet of agents coordinated via consensus.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AgentFleet {
    pub agents: Vec<Agent>,
    pub raft_nodes: Vec<RaftNode>,
    pub data_stores: Vec<DataStore>,
}

impl AgentFleet {
    pub fn new(size: usize) -> Self {
        let ids: Vec<String> = (0..size).map(|i| format!("agent_{}", i)).collect();
        let agents: Vec<Agent> = ids.iter().map(|id| Agent::new(id.clone())).collect();
        let raft_nodes: Vec<RaftNode> = ids.iter().map(|id| {
            let peers: Vec<String> = ids.iter().filter(|p| *p != id).cloned().collect();
            RaftNode::new(id.clone(), peers)
        }).collect();
        let data_stores = (0..size).map(|_| DataStore::new(ConsistencyModel::Causal)).collect();
        Self { agents, raft_nodes, data_stores }
    }

    /// Elect a leader among the agents.
    pub fn elect_leader(&mut self, candidate_idx: usize) {
        simulate_election(&mut self.raft_nodes, candidate_idx);
        for (i, raft) in self.raft_nodes.iter().enumerate() {
            self.agents[i].role = match raft.role {
                crate::consensus::raft::NodeRole::Leader => AgentRole::Leader,
                crate::consensus::raft::NodeRole::Candidate => AgentRole::Candidate,
                crate::consensus::raft::NodeRole::Follower => AgentRole::Follower,
            };
        }
    }

    /// Propose a state change through the leader.
    pub fn propose_state(&mut self, key: &str, value: &str) -> bool {
        let leader_idx = self.raft_nodes.iter().position(|n| 
            n.role == crate::consensus::raft::NodeRole::Leader
        );
        if let Some(li) = leader_idx {
            replicate_entry(&mut self.raft_nodes, li, &format!("{}={}", key, value));
            // Update all agent states
            for agent in &mut self.agents {
                agent.set(key, value);
            }
            true
        } else {
            false
        }
    }

    /// Merge state across all agents for eventual consistency.
    pub fn synchronize(&mut self, key: &str) {
        merge_replicas(&mut self.data_stores, key);
    }

    pub fn leader(&self) -> Option<&Agent> {
        self.agents.iter().find(|a| a.role == AgentRole::Leader)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_fleet_creation() {
        let fleet = AgentFleet::new(5);
        assert_eq!(fleet.agents.len(), 5);
        assert_eq!(fleet.raft_nodes.len(), 5);
    }

    #[test]
    fn test_leader_election() {
        let mut fleet = AgentFleet::new(3);
        fleet.elect_leader(0);
        assert!(fleet.leader().is_some());
        assert_eq!(fleet.leader().unwrap().role, AgentRole::Leader);
    }

    #[test]
    fn test_state_proposal() {
        let mut fleet = AgentFleet::new(3);
        fleet.elect_leader(0);
        assert!(fleet.propose_state("x", "42"));
        for agent in &fleet.agents {
            assert_eq!(agent.get("x"), Some("42"));
        }
    }

    #[test]
    fn test_no_proposal_without_leader() {
        let mut fleet = AgentFleet::new(3);
        // No election, no leader
        assert!(!fleet.propose_state("x", "42"));
    }

    #[test]
    fn test_agent_vector_clocks() {
        let mut fleet = AgentFleet::new(3);
        fleet.elect_leader(0);
        fleet.propose_state("x", "1");
        // Each agent should have incremented its clock
        for agent in &fleet.agents {
            assert!(agent.vector_clock.get(&agent.id) >= 1);
        }
    }

    #[test]
    fn test_multiple_proposals() {
        let mut fleet = AgentFleet::new(5);
        fleet.elect_leader(2);
        fleet.propose_state("a", "1");
        fleet.propose_state("b", "2");
        fleet.propose_state("c", "3");
        for agent in &fleet.agents {
            assert_eq!(agent.get("a"), Some("1"));
            assert_eq!(agent.get("b"), Some("2"));
            assert_eq!(agent.get("c"), Some("3"));
        }
    }
}
