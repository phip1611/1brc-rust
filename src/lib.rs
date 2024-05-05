use std::collections::HashMap;
use std::fs::File;
use std::io::{BufRead, BufReader};
use std::path::Path;
use std::str::FromStr;

const READ_BUFFER_SIZE: usize = 0x4000000 /* 64 Mib */;
const AVG_BYTES_PER_LINE: u64 = 15;

#[derive(Copy, Clone, Debug)]
struct AggregatedWeatherData {
    min: f32,
    max: f32,
    sum: f32,
    sample_count: usize,
}

impl Default for AggregatedWeatherData {
    fn default() -> Self {
        Self {
            min: f32::MAX,
            max: f32::MIN,
            sum: 0.0,
            sample_count: 0,
        }
    }
}

impl AggregatedWeatherData {
    fn add_datapoint(&mut self, measurement: f32) {
        if measurement < self.min {
            self.min = measurement
        }
        if measurement > self.max {
            self.max = measurement
        }
        self.sum += measurement;
        self.sample_count += 1;
    }

    fn avg(&self) -> f32 {
        self.sum / self.sample_count as f32
    }
}

/// Processes all data according to the 1brc challenge and prints the data
/// to `<path>.processed.txt` in `{Abha=-23.0/18.0/59.2, Abidjan=-16.2/...`
/// format, where the value of each key is <min>/<mean>/<max>.
///
/// I didn't do specific "extreme" fine-tuning or testing of ideal buffer
/// sizes and intermediate buffer sizes. This is a best-effort approach for a
/// trade-off between readability, simplicity, and performance.
pub fn process_and_print(path: impl AsRef<Path> + Clone) {
    let file = File::open(path).unwrap();
    let mut reader = BufReader::with_capacity(READ_BUFFER_SIZE, file);

    let mut line_read_buf = String::with_capacity((AVG_BYTES_PER_LINE * 2) as usize);

    let mut stats = HashMap::new();

    // let mut workload_lines_per_thread = vec![0_u8; estimated_capacity_per_thread_buf];
    while let Ok(n) = reader.read_line(&mut line_read_buf) {
        if n == 0 {
            break;
        }
        // remove trailing "newline"
        let line = &line_read_buf[..line_read_buf.len() - 1];

        let (station, measurement) = line.split_once(';').unwrap();
        let measurement = f32::from_str(measurement).unwrap();

        let weather_data = stats
            .entry(station.to_string())
            .or_insert(AggregatedWeatherData::default());
        weather_data.add_datapoint(measurement);

        // Clear for next iteration.
        line_read_buf.clear();
    }

    // sort in a vec: quicker than in a btreemap
    let mut stats = stats.into_iter().collect::<Vec<_>>();
    stats.sort_unstable_by(|(station_a, _), (station_b, _)| {
        station_a.partial_cmp(station_b).unwrap()
    });

    print_results(stats.into_iter());
}

fn print_results(stats: impl ExactSizeIterator<Item = (String, AggregatedWeatherData)>) {
    print!("{{");
    let n = stats.len();
    stats
        .enumerate()
        .map(|(index, x)| (index == n - 1, x))
        .for_each(|(is_last, (city, measurements))| {
            print!(
                "{city}={:.1}/{:.1}/{:.1}",
                measurements.min,
                measurements.avg(),
                measurements.max
            );
            if !is_last {
                print!(", ");
            }
        });
    println!("}}");
}
