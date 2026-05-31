use serde::{Deserialize, Serialize};
use nalgebra::DMatrix;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum CapChoice {
    Consistency,
    Availability,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CapSimulation {
    pub network_partition: bool,
    pub latency_ms: f64,
    pub choice: CapChoice,
    pub consistency_score: f64, // 0..1
    pub availability_score: f64, // 0..1
}

impl CapSimulation {
    pub fn new(choice: CapChoice) -> Self {
        Self {
            network_partition: false,
            latency_ms: 0.0,
            choice,
            consistency_score: 1.0,
            availability_score: 1.0,
        }
    }

    pub fn simulate_partition(&mut self) {
        self.network_partition = true;
        match self.choice {
            CapChoice::Consistency => {
                self.availability_score = 0.3;
                self.consistency_score = 1.0;
            }
            CapChoice::Availability => {
                self.availability_score = 1.0;
                self.consistency_score = 0.3;
            }
        }
    }

    pub fn simulate_latency(&mut self, latency_ms: f64) {
        self.latency_ms = latency_ms;
        // Higher latency reduces both, but affects consistency more if chosen
        let latency_factor = (100.0 / (100.0 + latency_ms)).min(1.0);
        match self.choice {
            CapChoice::Consistency => {
                self.consistency_score *= latency_factor.max(0.5);
                self.availability_score = latency_factor;
            }
            CapChoice::Availability => {
                self.availability_score *= latency_factor.max(0.5);
                self.consistency_score = latency_factor * 0.5;
            }
        }
    }
}

/// Generate a latency matrix for a distributed system.
pub fn latency_matrix(nodes: usize, base_latency_ms: f64) -> DMatrix<f64> {
    let mut data = vec![0.0; nodes * nodes];
    for i in 0..nodes {
        for j in 0..nodes {
            if i != j {
                // Simulate some variance
                let distance = ((i as f64 - j as f64).abs()).min(nodes as f64 - (i as f64 - j as f64).abs());
                data[i * nodes + j] = base_latency_ms * (1.0 + distance * 0.1);
            }
        }
    }
    DMatrix::from_vec(nodes, nodes, data)
}

/// Analyze the tradeoff between latency and consistency.
pub fn analyze_tradeoff(_nodes: usize, base_latency_ms: f64, choice: CapChoice) -> CapSimulation {
    let mut sim = CapSimulation::new(choice);
    sim.simulate_latency(base_latency_ms);
    sim
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_cap_consistency_during_partition() {
        let mut sim = CapSimulation::new(CapChoice::Consistency);
        sim.simulate_partition();
        assert_eq!(sim.consistency_score, 1.0);
        assert!(sim.availability_score < 1.0);
    }

    #[test]
    fn test_cap_availability_during_partition() {
        let mut sim = CapSimulation::new(CapChoice::Availability);
        sim.simulate_partition();
        assert_eq!(sim.availability_score, 1.0);
        assert!(sim.consistency_score < 1.0);
    }

    #[test]
    fn test_latency_affects_scores() {
        let mut sim = CapSimulation::new(CapChoice::Consistency);
        sim.simulate_latency(500.0);
        assert!(sim.availability_score < 1.0);
        assert!(sim.consistency_score > 0.0);
    }

    #[test]
    fn test_latency_matrix_dimensions() {
        let m = latency_matrix(4, 10.0);
        assert_eq!(m.nrows(), 4);
        assert_eq!(m.ncols(), 4);
    }

    #[test]
    fn test_latency_matrix_diagonal_zero() {
        let m = latency_matrix(4, 10.0);
        for i in 0..4 {
            assert_eq!(m[(i, i)], 0.0);
        }
    }

    #[test]
    fn test_latency_matrix_nonzero_off_diagonal() {
        let m = latency_matrix(4, 10.0);
        for i in 0..4 {
            for j in 0..4 {
                if i != j {
                    assert!(m[(i, j)] > 0.0);
                }
            }
        }
    }
}
