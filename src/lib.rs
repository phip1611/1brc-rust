//! Implements a highly optimized variant for the 1BRC.
//! Look at [`process_file_chunk`], which is the heart of the implementation.
//!
//! All convenience around it, such as allocating a few helpers, is negligible
//! from my testing.

mod aggregated_data;
mod chunk_iter;

use crate::chunk_iter::ChunkIter;
use aggregated_data::AggregatedData;
use fnv::FnvHashMap as HashMap;
use memmap::{Mmap, MmapOptions};
use std::fs::File;
use std::hint::black_box;
use std::path::Path;
use std::str::FromStr;
use std::thread::available_parallelism;
use std::{slice, thread};

const CITIES_IN_DATASET: usize = 416;

/// Processes all data according to the 1brc challenge by using a
/// single-threaded implementation.
pub fn process_single_threaded(path: impl AsRef<Path> + Clone, print: bool) {
    let (_mmap, bytes) = unsafe { open_file(path) };

    let stats = process_file_chunk(bytes);

    finalize([stats].into_iter(), print);
}

/// Processes all data according to the 1brc challenge by using a
/// multi-threaded implementation. This spawns `n-1` worker threads. The main
/// thread also performs one workload and finally collects and combines all
/// results.
pub fn process_multi_threaded(path: impl AsRef<Path> + Clone, print: bool) {
    let (_mmap, bytes) = unsafe { open_file(path) };

    let cpus: usize = if bytes.len() < 10000 {
        1
    } else {
        available_parallelism().unwrap().into()
    };

    let mut thread_handles = Vec::with_capacity(cpus);

    let mut iter = ChunkIter::new(bytes, cpus);
    let main_thread_chunk = iter.next().unwrap();

    for chunk in iter {
        // Hack to move that data to the thread. This is safe as we join the
        // threads before the reference becomes invalid.

        // Spawning the threads is negligible cheap.
        // TODO it surprises me that rustc won't force me to transmute `chunk`
        //  to a &static lifetime.
        let handle = thread::spawn(move || process_file_chunk(chunk));
        thread_handles.push(handle);
    }

    let stats = process_file_chunk(main_thread_chunk);

    debug_assert_eq!(
        thread_handles.len(),
        cpus - 1,
        "must have 1-n worker threads"
    );

    let thread_results_iter = thread_handles
        .into_iter()
        .map(|handle| handle.join().unwrap())
        .chain(core::iter::once(stats));

    finalize(thread_results_iter, print);
}

/// Opens the file by mapping it via mmap into the address space of the program.
///
/// # Safety
/// The returned buffer is only valid as long as the returned `Mmap` lives.
unsafe fn open_file<'a>(path: impl AsRef<Path>) -> (Mmap, &'a [u8]) {
    let file = File::open(path).unwrap();
    let mmap = unsafe { MmapOptions::new().map(&file).unwrap() };
    // Only valid as long as `mmap` lives.
    let file_bytes: &[u8] = unsafe { slice::from_raw_parts(mmap.as_ptr(), mmap.len()) };

    (mmap, file_bytes)
}

/// Processes a chunk of the file. A chunk begins with the first byte of a line
/// and ends with a newline (`\n`).
///
/// The contained loop is the highly optimized hot path of the data processing.
/// There are no allocations, no unnecessary buffers, no unnecessary copies, no
/// unnecessary comparisons. I optimized the shit out of this :D
///
/// The returned data structure is not sorted.
fn process_file_chunk(bytes: &[u8]) -> HashMap<&str, AggregatedData> {
    assert!(!bytes.is_empty());
    let &last_byte = bytes.last().unwrap();
    assert_eq!(last_byte, b'\n');

    let mut stats = HashMap::with_capacity_and_hasher(CITIES_IN_DATASET, Default::default());

    // In each iteration, I read a line in two dedicated steps:
    // 1.) read city name
    // 2.) read value
    let mut consumed_bytes_count = 0;
    while consumed_bytes_count < bytes.len() {
        // Remaining bytes for this loop iteration.
        let remaining_bytes = &bytes[consumed_bytes_count..];

        // Look for station
        let n1 = memchr::memchr(b';', remaining_bytes).unwrap();
        let station = &remaining_bytes[0..n1];
        let station = unsafe { core::str::from_utf8_unchecked(station) };

        // Look for measurement
        // +1: skip "\n"
        let search_begin_i = n1 + 1;
        let n2 = memchr::memchr(b'\n', &remaining_bytes[search_begin_i..])
            .map(|pos| pos + search_begin_i)
            .unwrap();
        // +1: skip ";'
        let measurement = &remaining_bytes[(n1 + 1)..n2];
        let measurement = unsafe { core::str::from_utf8_unchecked(measurement) };

        // The costs of this function are negligible cheap.
        let measurement = f32::from_str(measurement).unwrap();

        // Ensure the next iteration works on the next line.
        // +1: skip "\n"
        consumed_bytes_count += n2 + 1;

        // In the data set, there aren't that many different entries. So
        // most of the time, we take the `and_modify` branch.
        stats
            .entry(station)
            .and_modify(|data: &mut AggregatedData| data.add_datapoint(measurement))
            .or_insert_with(|| {
                let mut data = AggregatedData::default();
                data.add_datapoint(measurement);
                data
            });
    }
    stats
}

/// Aggregates the results and, optionally, prints them.
fn finalize<'a>(stats: impl Iterator<Item = HashMap<&'a str, AggregatedData>>, print: bool) {
    // This reduce step is surprisingly negligible cheap.
    let stats = stats
        .reduce(|mut acc, next| {
            next.into_iter().for_each(|(station, new_data)| {
                acc.entry(station)
                    .and_modify(|data| {
                        data.merge(&new_data);
                    })
                    .or_insert(new_data);
            });
            acc
        })
        .unwrap();

    // Sort everything into a vector. The costs of this are negligible cheap.
    let mut stats = stats.into_iter().collect::<Vec<_>>();
    stats.sort_unstable_by(|(station_a, _), (station_b, _)| {
        station_a.partial_cmp(station_b).unwrap()
    });

    if print {
        print_results(stats.into_iter())
    } else {
        // black-box: prevent the compiler from optimizing any calculations away
        let _x = black_box(stats);
    }
}

/// Prints the results. The costs of this function are negligible cheap.
fn print_results<'a>(stats: impl ExactSizeIterator<Item = (&'a str, AggregatedData)>) {
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_process_file_chunk() {
        let input = "Berlin;10.0\nHamburg;-12.7\nNew York;21.75\nBerlin;-15.7\n";
        let actual = process_file_chunk(input.as_bytes());
        let stats = actual.into_iter().collect::<Vec<_>>();

        // Order here is not relevant. I stick to the order from the HashMap
        // implementation.
        let hamburg = &stats[0];
        let berlin = &stats[1];
        let new_york = &stats[2];

        assert_eq!(hamburg.0, "Hamburg");
        assert_eq!(berlin.0, "Berlin");
        assert_eq!(new_york.0, "New York");

        let hamburg = &hamburg.1;
        let berlin = &berlin.1;
        let new_york = &new_york.1;

        assert_eq!(hamburg, &AggregatedData::new(-12.7, -12.7, -12.7, 1));
        assert_eq!(berlin, &AggregatedData::new(-15.7, 10.0, -5.7, 2));
        assert_eq!(new_york, &AggregatedData::new(21.75, 21.75, 21.75, 1));

        assert_eq!(hamburg.avg(), -12.7);
        assert_eq!(berlin.avg(), -2.85);
        assert_eq!(new_york.avg(), 21.75);
    }
}
