use serde::{Deserialize, Serialize};
use std::collections::HashMap;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct Proposal {
    pub number: u64,
    pub value: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum PaxosMessage {
    Prepare { proposal_number: u64, proposer_id: String },
    Promise { proposal_number: u64, accepted_proposal: Option<Proposal>, acceptor_id: String },
    Accept { proposal: Proposal, proposer_id: String },
    Accepted { proposal_number: u64, acceptor_id: String },
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AcceptorState {
    pub id: String,
    pub promised_number: u64,
    pub accepted_proposal: Option<Proposal>,
}

impl AcceptorState {
    pub fn new(id: String) -> Self {
        Self {
            id,
            promised_number: 0,
            accepted_proposal: None,
        }
    }

    pub fn receive_prepare(&mut self, proposal_number: u64) -> PaxosMessage {
        if proposal_number > self.promised_number {
            self.promised_number = proposal_number;
            PaxosMessage::Promise {
                proposal_number,
                accepted_proposal: self.accepted_proposal.clone(),
                acceptor_id: self.id.clone(),
            }
        } else {
            PaxosMessage::Promise {
                proposal_number: self.promised_number,
                accepted_proposal: self.accepted_proposal.clone(),
                acceptor_id: self.id.clone(),
            }
        }
    }

    pub fn receive_accept(&mut self, proposal: &Proposal) -> bool {
        if proposal.number >= self.promised_number {
            self.promised_number = proposal.number;
            self.accepted_proposal = Some(proposal.clone());
            true
        } else {
            false
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ProposerState {
    pub id: String,
    pub proposal_number: u64,
    pub proposed_value: Option<String>,
    pub promises_received: Vec<PaxosMessage>,
    pub acceptors: usize,
}

impl ProposerState {
    pub fn new(id: String, acceptors: usize) -> Self {
        Self {
            id,
            proposal_number: 0,
            proposed_value: None,
            promises_received: Vec::new(),
            acceptors,
        }
    }

    pub fn prepare(&mut self, value: String) -> PaxosMessage {
        self.proposal_number += 1;
        self.proposed_value = Some(value);
        self.promises_received.clear();
        PaxosMessage::Prepare {
            proposal_number: self.proposal_number,
            proposer_id: self.id.clone(),
        }
    }

    pub fn receive_promise(&mut self, msg: &PaxosMessage) -> Option<PaxosMessage> {
        self.promises_received.push(msg.clone());
        let quorum = self.acceptors / 2 + 1;
        if self.promises_received.len() >= quorum {
            // Find highest accepted value among promises
            let mut highest_accepted: Option<Proposal> = None;
            for p in &self.promises_received {
                if let PaxosMessage::Promise { accepted_proposal, .. } = p {
                    if let Some(ref prop) = accepted_proposal {
                        if highest_accepted.as_ref().map_or(true, |h| prop.number > h.number) {
                            highest_accepted = Some(prop.clone());
                        }
                    }
                }
            }
            let value = highest_accepted
                .and_then(|p| p.value)
                .or_else(|| self.proposed_value.take())
                .unwrap_or_default();
            self.proposed_value = Some(value.clone());
            Some(PaxosMessage::Accept {
                proposal: Proposal {
                    number: self.proposal_number,
                    value: Some(value),
                },
                proposer_id: self.id.clone(),
            })
        } else {
            None
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PaxosNode {
    pub proposer: ProposerState,
    pub acceptor: AcceptorState,
    pub learned_value: Option<String>,
    pub messages: Vec<PaxosMessage>,
}

impl PaxosNode {
    pub fn new(id: String, total_acceptors: usize) -> Self {
        Self {
            proposer: ProposerState::new(id.clone(), total_acceptors),
            acceptor: AcceptorState::new(id),
            learned_value: None,
            messages: Vec::new(),
        }
    }

    pub fn propose(&mut self, value: String) -> PaxosMessage {
        self.proposer.prepare(value)
    }

    pub fn receive(&mut self, msg: &PaxosMessage) -> Vec<PaxosMessage> {
        let mut responses = Vec::new();
        match msg {
            PaxosMessage::Prepare { proposal_number, .. } => {
                let resp = self.acceptor.receive_prepare(*proposal_number);
                responses.push(resp);
            }
            PaxosMessage::Accept { proposal, .. } => {
                if self.acceptor.receive_accept(proposal) {
                    responses.push(PaxosMessage::Accepted {
                        proposal_number: proposal.number,
                        acceptor_id: self.acceptor.id.clone(),
                    });
                }
            }
            PaxosMessage::Promise { .. } => {
                if let Some(accept_msg) = self.proposer.receive_promise(msg) {
                    responses.push(accept_msg);
                }
            }
            PaxosMessage::Accepted { proposal_number: _, .. } => {
                // Simple learning: just accept the first value we see accepted
                if self.learned_value.is_none() {
                    if let Some(ref v) = self.proposer.proposed_value {
                        self.learned_value = Some(v.clone());
                    }
                }
            }
        }
        self.messages.extend_from_slice(&responses);
        responses
    }
}

/// Simulate a full Paxos run with N nodes, one proposer proposing a value.
pub fn run_paxos(nodes: &mut Vec<PaxosNode>, proposer_idx: usize, value: &str) {
    let prepare = nodes[proposer_idx].propose(value.to_string());

    // Phase 1: Send prepare to all acceptors
    let mut promises = Vec::new();
    for (i, node) in nodes.iter_mut().enumerate() {
        if i != proposer_idx {
            let resps = node.receive(&prepare);
            promises.extend(resps);
        }
    }

    // Phase 1b: Proposer receives promises
    let mut accept_msgs = Vec::new();
    for promise in &promises {
        let resps = nodes[proposer_idx].receive(promise);
        accept_msgs.extend(resps);
    }

    // Phase 2: Send accept to all acceptors
    let mut accepted_msgs = Vec::new();
    for accept_msg in &accept_msgs {
        for (i, node) in nodes.iter_mut().enumerate() {
            if i != proposer_idx {
                let resps = node.receive(accept_msg);
                accepted_msgs.extend(resps);
            }
        }
    }

    // Phase 2b: Learn
    for msg in &accepted_msgs {
        for node in nodes.iter_mut() {
            node.receive(msg);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_acceptor_rejects_lower_proposal() {
        let mut a = AcceptorState::new("a1".into());
        a.receive_prepare(5);
        let resp = a.receive_prepare(3);
        // Should not have promised 3
        assert_eq!(a.promised_number, 5);
        if let PaxosMessage::Promise { proposal_number, .. } = resp {
            assert_eq!(proposal_number, 5);
        }
    }

    #[test]
    fn test_acceptor_accepts_higher_proposal() {
        let mut a = AcceptorState::new("a1".into());
        a.receive_prepare(3);
        let resp = a.receive_prepare(7);
        assert_eq!(a.promised_number, 7);
        if let PaxosMessage::Promise { proposal_number, .. } = resp {
            assert_eq!(proposal_number, 7);
        }
    }

    #[test]
    fn test_accept_rejected_if_lower_than_promised() {
        let mut a = AcceptorState::new("a1".into());
        a.receive_prepare(10);
        let accepted = a.receive_accept(&Proposal { number: 5, value: Some("v".into()) });
        assert!(!accepted);
    }

    #[test]
    fn test_accept_accepted_if_equal_or_higher() {
        let mut a = AcceptorState::new("a1".into());
        a.receive_prepare(5);
        let accepted = a.receive_accept(&Proposal { number: 5, value: Some("v".into()) });
        assert!(accepted);
        assert_eq!(a.accepted_proposal.as_ref().unwrap().value, Some("v".to_string()));
    }

    #[test]
    fn test_full_paxos_consensus() {
        let mut nodes = vec![
            PaxosNode::new("n0".into(), 3),
            PaxosNode::new("n1".into(), 3),
            PaxosNode::new("n2".into(), 3),
        ];
        run_paxos(&mut nodes, 0, "hello");
        // At least the proposer should have learned the value
        assert_eq!(nodes[0].learned_value, Some("hello".to_string()));
    }

    #[test]
    fn test_paxos_safety_no_two_values() {
        // If a value is chosen, no other value can be chosen
        let mut nodes = vec![
            PaxosNode::new("n0".into(), 5),
            PaxosNode::new("n1".into(), 5),
            PaxosNode::new("n2".into(), 5),
            PaxosNode::new("n3".into(), 5),
            PaxosNode::new("n4".into(), 5),
        ];
        run_paxos(&mut nodes, 0, "value_a");
        // All nodes that learned should have learned "value_a"
        for node in &nodes {
            if let Some(ref v) = node.learned_value {
                assert_eq!(v, "value_a");
            }
        }
    }

    #[test]
    fn test_proposer_quorum() {
        let mut proposer = ProposerState::new("p1".into(), 5);
        let _ = proposer.prepare("test".to_string());
        // Need 3 promises for quorum (5/2 + 1)
        assert!(proposer.receive_promise(&PaxosMessage::Promise {
            proposal_number: 1,
            accepted_proposal: None,
            acceptor_id: "a1".into(),
        }).is_none());
        assert!(proposer.receive_promise(&PaxosMessage::Promise {
            proposal_number: 1,
            accepted_proposal: None,
            acceptor_id: "a2".into(),
        }).is_none());
        assert!(proposer.receive_promise(&PaxosMessage::Promise {
            proposal_number: 1,
            accepted_proposal: None,
            acceptor_id: "a3".into(),
        }).is_some());
    }
}
