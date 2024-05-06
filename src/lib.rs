use fnv::FnvHashMap as HashMap;
use memmap::MmapOptions;
use std::fs::File;
use std::path::Path;
use std::str::FromStr;

const CITIES_IN_DATASET: usize = 416;

#[derive(Copy, Clone, Debug)]
pub struct AggregatedWeatherData {
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
///
/// Returns a sorted vector with the aggregated results.
pub fn process(
    path: impl AsRef<Path> + Clone,
) -> (memmap::Mmap, Vec<(&'static str, AggregatedWeatherData)>) {
    let file = File::open(path).unwrap();
    let mmap = unsafe { MmapOptions::new().map(&file).unwrap() };
    // Hack: actually only valid as long as "mmap" lives
    let file_bytes: &'static [u8] =
        unsafe { core::slice::from_raw_parts(mmap.as_ptr(), mmap.len()) };

    let mut stats = HashMap::with_capacity_and_hasher(CITIES_IN_DATASET, Default::default());

    // In each iteration, I read a line in two dedicated steps:
    // 1.) read city name
    // 2.) read value
    let mut consumed_bytes_count = 0;
    while consumed_bytes_count < file_bytes.len() {
        // Remaining bytes for this loop iteration.
        let remaining_bytes = &file_bytes[consumed_bytes_count..];

        let n1 = memchr::memchr(b';', &remaining_bytes).unwrap();
        let station = &remaining_bytes[0..n1];
        let station = unsafe { core::str::from_utf8_unchecked(station) };

        // +1: skip "\n"
        let search_begin_i = n1 + 1;
        let n2 = memchr::memchr(b'\n', &remaining_bytes[search_begin_i..])
            .map(|pos| pos + search_begin_i)
            .unwrap();

        // +1: skip ";'
        let measurement = &remaining_bytes[(n1 + 1)..n2];
        let measurement = unsafe { core::str::from_utf8_unchecked(measurement) };
        let measurement = f32::from_str(measurement).unwrap();

        // +1: skip "\n"
        consumed_bytes_count += n2 + 1;

        // In the data set, there aren't that many different entries.
        stats
            .entry(station)
            .and_modify(|data: &mut AggregatedWeatherData| data.add_datapoint(measurement))
            .or_insert_with(|| {
                let mut data = AggregatedWeatherData::default();
                data.add_datapoint(measurement);
                data
            });
    }

    // sort in a vec: quicker than in a btreemap
    let mut stats = stats.into_iter().collect::<Vec<_>>();
    stats.sort_unstable_by(|(station_a, _), (station_b, _)| {
        station_a.partial_cmp(station_b).unwrap()
    });
    (mmap, stats)
}

pub fn print_results<'a>(stats: impl ExactSizeIterator<Item = (&'a str, AggregatedWeatherData)>) {
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
