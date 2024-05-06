use fnv::FnvHashMap as HashMap;
use likely_stable::{likely, unlikely};
use std::fs::File;
use std::io::{BufRead, BufReader};
use std::path::Path;
use std::str::FromStr;

const READ_BUFFER_SIZE: usize = 0x4000000 /* 64 Mib */;
/// Obtained from `wc -L ./measurements.txt`
const MAX_LINE_LEN: usize = 32 + 1 /* newline */;
const CITIES_IN_DATASET: usize = 416;

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
    let mut line_read_buf = Vec::with_capacity(MAX_LINE_LEN);
    let mut stats = HashMap::with_capacity_and_hasher(CITIES_IN_DATASET, Default::default());

    while let Ok(n1) = reader.read_until(b';', &mut line_read_buf) {
        if unlikely(n1 == 0) {
            break;
        }
        let n2 = reader.read_until(b'\n', &mut line_read_buf).unwrap();
        debug_assert!(
            n2 > 0,
            "Malformed data. Data must always have a newline after a ;"
        );

        let station = unsafe { core::str::from_utf8_unchecked(&line_read_buf[0..n1]) };
        // remove trailing ';'
        let station = &station[0..station.len() - 1];

        let measurement = unsafe { core::str::from_utf8_unchecked(&line_read_buf[n1..]) };
        // remove trailing '\n'
        let measurement = &measurement[0..measurement.len() - 1];

        let measurement = f32::from_str(measurement).unwrap();

        // In the data set, there aren't that many different entries.
        let data = stats.get_mut(station);
        if likely(data.is_some()) {
            let data: &mut AggregatedWeatherData = data.unwrap();
            data.add_datapoint(measurement);
        } else {
            let mut data = AggregatedWeatherData::default();
            data.add_datapoint(measurement);
            stats.insert(station.to_string(), data);
        }

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
