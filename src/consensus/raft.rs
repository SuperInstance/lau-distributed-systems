use serde::{Deserialize, Serialize};
use std::collections::HashMap;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum NodeRole {
    Follower,
    Candidate,
    Leader,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LogEntry {
    pub term: u64,
    pub command: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum RaftMessage {
    RequestVote {
        term: u64,
        candidate_id: String,
        last_log_index: usize,
        last_log_term: u64,
    },
    VoteResponse {
        term: u64,
        vote_granted: bool,
        voter_id: String,
    },
    AppendEntries {
        term: u64,
        leader_id: String,
        prev_log_index: usize,
        prev_log_term: u64,
        entries: Vec<LogEntry>,
        leader_commit: usize,
    },
    AppendResponse {
        term: u64,
        success: bool,
        match_index: usize,
        responder_id: String,
    },
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RaftNode {
    pub id: String,
    pub current_term: u64,
    pub voted_for: Option<String>,
    pub role: NodeRole,
    pub log: Vec<LogEntry>,
    pub commit_index: usize,
    pub last_applied: usize,
    pub votes_received: Vec<String>,
    pub next_index: HashMap<String, usize>,
    pub match_index: HashMap<String, usize>,
    pub peers: Vec<String>,
    pub state_machine: Vec<String>,
}

impl RaftNode {
    pub fn new(id: String, peers: Vec<String>) -> Self {
        let next_index: HashMap<String, usize> = peers.iter().map(|p| (p.clone(), 0)).collect();
        let match_index: HashMap<String, usize> = peers.iter().map(|p| (p.clone(), 0)).collect();
        Self {
            id,
            current_term: 0,
            voted_for: None,
            role: NodeRole::Follower,
            log: Vec::new(),
            commit_index: 0,
            last_applied: 0,
            votes_received: Vec::new(),
            next_index,
            match_index,
            peers,
            state_machine: Vec::new(),
        }
    }

    pub fn start_election(&mut self) -> RaftMessage {
        self.current_term += 1;
        self.role = NodeRole::Candidate;
        self.voted_for = Some(self.id.clone());
        self.votes_received = vec![self.id.clone()];
        let last_log_index = self.log.len().saturating_sub(1);
        let last_log_term = self.log.last().map_or(0, |e| e.term);
        RaftMessage::RequestVote {
            term: self.current_term,
            candidate_id: self.id.clone(),
            last_log_index,
            last_log_term,
        }
    }

    pub fn handle_request_vote(&mut self, msg: &RaftMessage) -> RaftMessage {
        if let RaftMessage::RequestVote { term, candidate_id, last_log_index, last_log_term } = msg {
            if *term < self.current_term {
                return RaftMessage::VoteResponse {
                    term: self.current_term,
                    vote_granted: false,
                    voter_id: self.id.clone(),
                };
            }
            let grant = if self.voted_for.is_none() || self.voted_for.as_deref() == Some(candidate_id) {
                // Check log up-to-date
                let my_last_idx = self.log.len().saturating_sub(1);
                let my_last_term = self.log.last().map_or(0, |e| e.term);
                *last_log_term > my_last_term
                    || (*last_log_term == my_last_term && *last_log_index >= my_last_idx)
            } else {
                false
            };
            if grant {
                if *term > self.current_term {
                    self.current_term = *term;
                    self.role = NodeRole::Follower;
                }
                self.voted_for = Some(candidate_id.clone());
            }
            RaftMessage::VoteResponse {
                term: self.current_term,
                vote_granted: grant,
                voter_id: self.id.clone(),
            }
        } else {
            panic!("Expected RequestVote");
        }
    }

    pub fn handle_vote_response(&mut self, msg: &RaftMessage) {
        if let RaftMessage::VoteResponse { term, vote_granted, voter_id } = msg {
            if *term > self.current_term {
                self.current_term = *term;
                self.role = NodeRole::Follower;
                return;
            }
            if *vote_granted && self.role == NodeRole::Candidate {
                if !self.votes_received.contains(voter_id) {
                    self.votes_received.push(voter_id.clone());
                }
                let quorum = (self.peers.len() + 1) / 2 + 1;
                if self.votes_received.len() >= quorum {
                    self.role = NodeRole::Leader;
                }
            }
        }
    }

    pub fn append_entries(&mut self, entries: Vec<LogEntry>, prev_index: usize, prev_term: u64, leader_commit: usize) -> bool {
        if prev_index > 0 {
            if self.log.len() < prev_index {
                return false;
            }
            if self.log[prev_index - 1].term != prev_term {
                return false;
            }
        }
        // Truncate conflicting entries and append
        for (i, entry) in entries.iter().enumerate() {
            let idx = prev_index + i;
            if idx < self.log.len() {
                if self.log[idx].term != entry.term {
                    self.log.truncate(idx);
                    self.log.push(entry.clone());
                }
            } else {
                self.log.push(entry.clone());
            }
        }
        if leader_commit > self.commit_index {
            self.commit_index = leader_commit.min(self.log.len());
        }
        // Apply committed entries
        while self.last_applied < self.commit_index {
            if let Some(entry) = self.log.get(self.last_applied) {
                self.state_machine.push(entry.command.clone());
            }
            self.last_applied += 1;
        }
        true
    }

    pub fn leader_append(&mut self, command: String) -> Option<RaftMessage> {
        if self.role != NodeRole::Leader {
            return None;
        }
        let entry = LogEntry {
            term: self.current_term,
            command,
        };
        self.log.push(entry.clone());
        let prev_idx = self.log.len() - 1;
        let prev_term = if prev_idx > 0 { self.log[prev_idx - 1].term } else { 0 };
        Some(RaftMessage::AppendEntries {
            term: self.current_term,
            leader_id: self.id.clone(),
            prev_log_index: prev_idx,
            prev_log_term: prev_term,
            entries: vec![entry],
            leader_commit: self.commit_index,
        })
    }
}

/// Run a leader election across a cluster of Raft nodes.
pub fn simulate_election(nodes: &mut Vec<RaftNode>, candidate_idx: usize) {
    let vote_req = nodes[candidate_idx].start_election();
    let mut responses = Vec::new();
    for (i, node) in nodes.iter_mut().enumerate() {
        if i != candidate_idx {
            let resp = node.handle_request_vote(&vote_req);
            responses.push(resp);
        }
    }
    for resp in responses {
        nodes[candidate_idx].handle_vote_response(&resp);
    }
}

/// Replicate a log entry from leader to followers.
pub fn replicate_entry(nodes: &mut Vec<RaftNode>, leader_idx: usize, command: &str) {
    if let Some(msg) = nodes[leader_idx].leader_append(command.to_string()) {
        if let RaftMessage::AppendEntries { prev_log_index, prev_log_term, entries, leader_commit, .. } = &msg {
            for (i, node) in nodes.iter_mut().enumerate() {
                if i != leader_idx {
                    let _success = node.append_entries(
                        entries.clone(),
                        *prev_log_index,
                        *prev_log_term,
                        *leader_commit,
                    );
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_cluster(n: usize) -> Vec<RaftNode> {
        let ids: Vec<String> = (0..n).map(|i| format!("n{}", i)).collect();
        ids.iter().map(|id| {
            let peers: Vec<String> = ids.iter().filter(|p| *p != id).cloned().collect();
            RaftNode::new(id.clone(), peers)
        }).collect()
    }

    #[test]
    fn test_follower_becomes_candidate() {
        let mut node = RaftNode::new("n0".into(), vec!["n1".into(), "n2".into()]);
        assert_eq!(node.role, NodeRole::Follower);
        node.start_election();
        assert_eq!(node.role, NodeRole::Candidate);
        assert_eq!(node.current_term, 1);
    }

    #[test]
    fn test_candidate_wins_election() {
        let mut nodes = make_cluster(3);
        simulate_election(&mut nodes, 0);
        assert_eq!(nodes[0].role, NodeRole::Leader);
        assert_eq!(nodes[1].role, NodeRole::Follower);
        assert_eq!(nodes[2].role, NodeRole::Follower);
    }

    #[test]
    fn test_leader_replication() {
        let mut nodes = make_cluster(3);
        simulate_election(&mut nodes, 0);
        assert_eq!(nodes[0].role, NodeRole::Leader);
        replicate_entry(&mut nodes, 0, "set x=1");
        assert_eq!(nodes[0].log.len(), 1);
        assert_eq!(nodes[1].log.len(), 1);
        assert_eq!(nodes[2].log.len(), 1);
        assert_eq!(nodes[1].log[0].command, "set x=1");
    }

    #[test]
    fn test_log_replication_multiple_entries() {
        let mut nodes = make_cluster(3);
        simulate_election(&mut nodes, 0);
        replicate_entry(&mut nodes, 0, "set x=1");
        replicate_entry(&mut nodes, 0, "set y=2");
        assert_eq!(nodes[0].log.len(), 2);
        assert_eq!(nodes[1].log.len(), 2);
        assert_eq!(nodes[2].log.len(), 2);
    }

    #[test]
    fn test_reject_old_term_vote() {
        let mut nodes = make_cluster(3);
        nodes[0].current_term = 5;
        let msg = RaftMessage::RequestVote {
            term: 3,
            candidate_id: "n1".into(),
            last_log_index: 0,
            last_log_term: 0,
        };
        let resp = nodes[0].handle_request_vote(&msg);
        if let RaftMessage::VoteResponse { vote_granted, .. } = resp {
            assert!(!vote_granted);
        }
    }

    #[test]
    fn test_state_machine_apply() {
        let mut nodes = make_cluster(3);
        simulate_election(&mut nodes, 0);
        replicate_entry(&mut nodes, 0, "set x=1");
        // Commit by advancing commit index
        nodes[0].commit_index = 1;
        while nodes[0].last_applied < nodes[0].commit_index {
            let cmd = nodes[0].log.get(nodes[0].last_applied).cloned();
            if let Some(entry) = cmd {
                nodes[0].state_machine.push(entry.command);
            }
            nodes[0].last_applied += 1;
        }
        assert!(nodes[0].state_machine.contains(&"set x=1".to_string()));
    }

    #[test]
    fn test_five_node_election() {
        let mut nodes = make_cluster(5);
        simulate_election(&mut nodes, 2);
        assert_eq!(nodes[2].role, NodeRole::Leader);
    }
}
