use std::cmp::{max_by, min, min_by};
use fnv::FnvHashMap as HashMap;
use memmap::MmapOptions;
use std::fs::File;
use std::path::Path;
use std::str::FromStr;
use std::thread::JoinHandle;

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

/// Processes all data according to the 1brc challenge and returns them in a
/// sorted vector.
pub fn process(
    path: impl AsRef<Path> + Clone,
) -> (memmap::Mmap, Vec<(&'static str, AggregatedWeatherData)>) {
    let file = File::open(path).unwrap();
    let mmap = unsafe { MmapOptions::new().map(&file).unwrap() };
    // Hack: actually only valid as long as "mmap" lives
    let file_bytes: &'static [u8] =
        unsafe { core::slice::from_raw_parts(mmap.as_ptr(), mmap.len()) };


    let thread_count: usize = std::thread::available_parallelism().unwrap().into();
    let bytes_per_thread = file_bytes.len().div_ceil(thread_count);

    let mut thread_handles = vec![];

    // In each iteration, I read a line in two dedicated steps:
    // 1.) read city name
    // 2.) read value
    let mut consumed_bytes_count = 0;

    // Spawn `n` worker threads with the correct data set for each.
    while consumed_bytes_count < file_bytes.len() {
        // Remaining bytes for this loop iteration.
        let remaining_bytes = &file_bytes[consumed_bytes_count..];

        // The inclusive end of the block where the search for the actual end (newline)
        // begins.
        let block_end_begin_search_index = min(bytes_per_thread, remaining_bytes.len());

        // Prepare a data block for the next thread; ensures it ends with a
        // newline
        let data_for_thread_end_index = memchr::memchr(b'\n', &remaining_bytes[block_end_begin_search_index - 1..])
            .map(|pos| pos + block_end_begin_search_index).unwrap();
        let data_for_thread = &remaining_bytes[0..data_for_thread_end_index];
        consumed_bytes_count += data_for_thread_end_index + 1;

        let handle = spawn_worker_thread(data_for_thread);
        thread_handles.push(handle);
    }

    // Ensure that we really created the right amount of threads.
    debug_assert_eq!(thread_handles.len(), thread_count);

    // Aggregate all thread results in the main thread. Combine them in a map.
    // This takes ~300µs on my machine with 16 cores. It is not worth it to
    // distribute that work onto threads in a (reverse-)pyramid-like structure.
    let stats = thread_handles.into_iter().map(|h| h.join().unwrap())
        .reduce(|mut acc, next| {
            next.into_iter().for_each(|(station, new_data)| {
                acc.entry(station)
                    .and_modify(|data| {
                        data.max = max_by(data.max, new_data.max, |a, b| a.partial_cmp(b).unwrap());
                        data.min = min_by(data.min, new_data.min, |a, b| a.partial_cmp(b).unwrap());
                        data.sum += new_data.sum;
                        data.sample_count += new_data.sample_count;
                    })
                    .or_insert(new_data);
            });
            acc
        }).unwrap();

    // sort in a vec: quicker than in a btreemap
    let mut stats = stats.into_iter().collect::<Vec<_>>();
    stats.sort_unstable_by(|(station_a, _), (station_b, _)| {
        station_a.partial_cmp(station_b).unwrap()
    });
    (mmap, stats)
}

/// Spawns a worker thread. Each thread operates on its dedicated slice of the
/// file. Each chunk per thread is guaranteed to end with a newline.
fn spawn_worker_thread(bytes: &'static [u8]) -> JoinHandle<HashMap<&'static str, AggregatedWeatherData>> {
    let mut consumed_bytes_count = 0;
    std::thread::spawn(move || {
        let mut stats = HashMap::with_capacity_and_hasher(CITIES_IN_DATASET, Default::default());

        while consumed_bytes_count < bytes.len() {
            let remaining_bytes = &bytes[consumed_bytes_count..];

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
        stats
    })
}

pub fn print_results<'a>(stats: impl ExactSizeIterator<Item=(&'a str, AggregatedWeatherData)>) {
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
