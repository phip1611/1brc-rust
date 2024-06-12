//! Implements a highly optimized variant for the 1BRC.
//! Look at [`process_file_chunk`], which is the heart of the implementation.
//!
//! All convenience around it, such as allocating a few helpers, is negligible
//! from my testing.

mod aggregated_data;
mod chunk_iter;

use crate::chunk_iter::ChunkIter;
use crate::data_set_properties::{MIN_MEASUREMENT_LEN, MIN_STATION_LEN, STATIONS_IN_DATASET};
use aggregated_data::AggregatedData;
use gxhash::HashMap;
use memmap::{Mmap, MmapOptions};
use std::fs::File;
use std::hint::black_box;
use std::path::Path;
use std::str::from_utf8_unchecked;
use std::thread::available_parallelism;
use std::{slice, thread};

/// Some characteristics specifically to the [1BRC data set](https://github.com/gunnarmorling/1brc/blob/db064194be375edc02d6dbcd21268ad40f7e2869/src/main/java/dev/morling/onebrc/CreateMeasurements.java).
mod data_set_properties {
    /// The amount of distinct weather stations (cities).
    pub const STATIONS_IN_DATASET: usize = 413;
    /// The minimum station name length (for example: `Jos`).
    pub const MIN_STATION_LEN: usize = 3;
    /// The minimum measurement (str) len (for example: `6.6`).
    pub const MIN_MEASUREMENT_LEN: usize = 3;
}

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

    let cpus = cpu_count(bytes.len());

    let mut thread_handles = Vec::with_capacity(cpus);

    let mut iter = ChunkIter::new(bytes, cpus);
    let main_thread_chunk = iter.next().unwrap();

    for chunk in iter {
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
/// and ends with a newline (`\n`), but contains an arbitrary amount of lines.
///
/// The contained loop is the highly optimized hot path of the data processing.
/// There are no allocations, no unnecessary buffers, no unnecessary copies, no
/// unnecessary comparisons, no not-inlined function calls.
///
/// The returned data structure is not sorted.
fn process_file_chunk(bytes: &[u8]) -> HashMap<&str, AggregatedData> {
    assert!(!bytes.is_empty());
    let &last_byte = bytes.last().unwrap();
    assert_eq!(last_byte, b'\n');

    let mut stats = HashMap::with_capacity_and_hasher(STATIONS_IN_DATASET, Default::default());

    let mut consumed_bytes_count = 0;
    while consumed_bytes_count < bytes.len() {
        let remaining_bytes = &bytes[consumed_bytes_count..];
        let (station, measurement) = process_line(remaining_bytes, &mut consumed_bytes_count);
        insert_measurement(&mut stats, station, measurement);
    }
    stats
}

/// Reads a line from the bytes and processes it. This expects that `bytes[0]`
/// is the beginning of a new line. It returns the processed data and updates
/// the `consumed_bytes_count` so that the next iteration can begin at the
/// beginning of a new line.
#[inline(always)]
fn process_line<'a>(bytes: &'a [u8], consumed_bytes_count: &mut usize) -> (&'a str, i16) {
    // Look for ";", and skip irrelevant bytes beforehand.
    let search_offset = MIN_STATION_LEN;
    let delimiter = memchr::memchr(b';', &bytes[search_offset..])
        .map(|pos| pos + search_offset)
        .unwrap();
    // Look for "\n", and skip irrelevant bytes beforehand.
    let search_offset = delimiter + 1 + MIN_MEASUREMENT_LEN;
    let newline = memchr::memchr(b'\n', &bytes[search_offset..])
        .map(|pos| pos + search_offset)
        .unwrap();

    let station = unsafe { from_utf8_unchecked(&bytes[0..delimiter]) };
    let measurement = unsafe { from_utf8_unchecked(&bytes[delimiter + 1..newline]) };

    let measurement = fast_f32_parse_encoded(measurement);

    // Ensure the next iteration works on the next line.
    *consumed_bytes_count += newline + 1;

    (station, measurement)
}

#[inline(always)]
fn insert_measurement<'a>(
    stats: &mut HashMap<&'a str, AggregatedData>,
    station: &'a str,
    measurement: i16,
) {
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

fn cpu_count(size: usize) -> usize {
    if size < 10000 {
        1
    } else {
        available_parallelism().unwrap().into()
    }
}

/// Optimized fast decimal number parsing that encodes a float in an integer,
/// which is multiplied by 10.
///
/// This benefits from the fact that we know that all input data has exactly 1
/// decimal place.
///
/// - `15.5` -> `155`
/// - `-7.1` -> `-71`
///
/// The range of possible values is within `-99.9..=99.9`.
///
/// To get back to the actual floating point value, one has to convert the value
/// to float and divide it by 10.
fn fast_f32_parse_encoded(input: &str) -> i16 {
    let mut bytes = input.as_bytes();

    let negative = bytes[0] == b'-';

    if negative {
        // Only parse digits.
        bytes = &bytes[1..];
    }

    let mut val = 0;
    for &byte in bytes {
        if byte == b'.' {
            continue;
        }
        let digit = (byte - b'0') as i16;
        val = val * 10 + digit;
    }

    if negative {
        -val
    } else {
        val
    }
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
                measurements.min(),
                measurements.avg(),
                measurements.max()
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
        let input = "Berlin;10.0\nHamburg;-12.7\nNew York;21.5\nBerlin;-15.7\n";
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

        assert_eq!(hamburg, &AggregatedData::new(-127, -127, -127, 1));
        assert_eq!(berlin, &AggregatedData::new(-157, 100, -57, 2));
        assert_eq!(new_york, &AggregatedData::new(215, 215, 215, 1));

        assert_eq!(hamburg.avg(), -12.7);
        assert_eq!(berlin.avg(), -2.85);
        assert_eq!(new_york.avg(), 21.5);
    }

    #[test]
    fn test_fast_f32_parse() {
        assert_eq!(fast_f32_parse_encoded("0.0"), 00);
        assert_eq!(fast_f32_parse_encoded("5.0"), 50);
        assert_eq!(fast_f32_parse_encoded("5.7"), 57);
        assert_eq!(fast_f32_parse_encoded("-5.7"), -57);
        assert_eq!(fast_f32_parse_encoded("-99.9"), -999);
    }
}
