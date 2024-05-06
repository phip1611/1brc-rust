use likely_stable::unlikely;
use std::cmp::{max_by, min_by};

#[derive(Debug, PartialEq)]
pub struct AggregatedData {
    pub min: f32,
    pub max: f32,
    sum: f32,
    sample_count: usize,
}

impl Default for AggregatedData {
    fn default() -> Self {
        Self {
            min: f32::MAX,
            max: f32::MIN,
            sum: 0.0,
            sample_count: 0,
        }
    }
}

impl AggregatedData {
    #[cfg(test)]
    pub fn new(min: f32, max: f32, sum: f32, sample_count: usize) -> Self {
        Self {
            min,
            max,
            sum,
            sample_count,
        }
    }

    #[allow(clippy::collapsible_else_if)]
    pub fn add_datapoint(&mut self, measurement: f32) {
        if unlikely(self.empty()) {
            self.min = measurement;
            self.max = measurement;
        } else {
            if measurement < self.min {
                self.min = measurement
            } else if measurement > self.max {
                self.max = measurement
            }
        }

        self.sum += measurement;
        self.sample_count += 1;
    }

    /// Merge the data with another instance.
    pub fn merge(&mut self, other: &Self) {
        self.max = max_by(self.max, other.max, |a, b| a.partial_cmp(b).unwrap());
        self.min = min_by(self.min, other.min, |a, b| a.partial_cmp(b).unwrap());
        self.sum += other.sum;
        self.sample_count += other.sample_count;
    }

    pub fn avg(&self) -> f32 {
        self.sum / self.sample_count as f32
    }

    /// Hasn't received a data point so far.
    fn empty(&self) -> bool {
        self.max == f32::MIN
    }
}
