use likely_stable::unlikely;
use std::cmp::{max_by, min_by};

/// Aggregated data per station. The temperature is encoded as integer
/// multiplied by 10. `-15.7 => -157`. The corresponding getters return the real
/// value.
#[derive(Debug, Clone, PartialEq)]
pub struct AggregatedData {
    min: i16,
    max: i16,
    sum: i64,
    sample_count: u32,
    name: String,
}

impl Default for AggregatedData {
    fn default() -> Self {
        Self {
            min: i16::MAX,
            max: i16::MIN,
            sum: 0,
            sample_count: 0,
            name: String::new(),
        }
    }
}

impl AggregatedData {
    #[cfg(test)]
    pub fn new(min: i16, max: i16, sum: i64, sample_count: u32) -> Self {
        Self {
            min,
            max,
            sum,
            sample_count,
            ..Default::default()
        }
    }

    pub fn init(&mut self, name: &str) {
        assert!(self.name.is_empty(), "must only be initialized once");
        self.name.push_str(name);
    }

    #[allow(clippy::collapsible_else_if)]
    pub fn add_datapoint(&mut self, measurement: i16) {
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

        self.sum += measurement as i64;
        self.sample_count += 1;
    }

    /// Merge the data with another instance.
    pub fn merge(&mut self, other: &Self) {
        self.max = max_by(self.max, other.max, |a, b| a.partial_cmp(b).unwrap());
        self.min = min_by(self.min, other.min, |a, b| a.partial_cmp(b).unwrap());
        self.sum += other.sum;
        self.sample_count += other.sample_count;
    }

    pub fn name(&self) -> &str {
        &self.name
    }


    pub fn avg(&self) -> f32 {
        self.sum as f32 / ((self.sample_count * 10) as f32)
    }

    pub fn max(&self) -> f32 {
        self.max as f32 / 10.0
    }

    pub fn min(&self) -> f32 {
        self.min as f32 / 10.0
    }

    /// Hasn't received a data point so far.
    fn empty(&self) -> bool {
        self.max == i16::MIN
    }
}

#[cfg(test)]
mod tests {
    use crate::aggregated_data::AggregatedData;
    use std::mem::size_of;

    #[test]
    fn layout() {
        assert_eq!(size_of::<AggregatedData>(), 16);
    }
}
