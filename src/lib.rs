use std::collections::BTreeMap;
use std::fs::File;
use std::io::Read;
use std::ops::Deref;
use std::path::Path;
use std::str::FromStr;
use std::sync::mpsc::Receiver;
use std::thread::JoinHandle;
use std::time::Instant;

const READ_BUFFER_SIZE: usize = 0x10000000 /* 512 Mib */;

#[derive(Debug, Default)]
struct Stats(BTreeMap<String /* City */, Vec<f32> /* Measurements */>);

impl Stats {
    pub fn add_record(&mut self, city: &str, temp: &str) {
        let temp = f32::from_str(temp).unwrap();

        if self.0.contains_key(city) {
            let measurements = self.0.get_mut(city).unwrap();
            measurements.push(temp);
        } else {
            self.0.insert(city.to_string(), vec![temp]);
        }
    }
    pub fn merge_measurements(&mut self, city: &str, new_measurements: &[f32]) {
        if self.0.contains_key(city) {
            let measurements = self.0.get_mut(city).unwrap();
            measurements.extend(new_measurements);
        } else {
            self.0.insert(city.to_string(), new_measurements.to_vec());
        }
    }

    pub fn into_inner(self) -> BTreeMap<String /* City */, Vec<f32> /* Measurements */> {
        self.0
    }
}

impl Deref for Stats {
    type Target = BTreeMap<String, Vec<f32>>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

enum ThreadMessage {
    Workload(Vec<u8>),
    Stop,
}

/// Processes all data according to the 1brc challenge and prints the data
/// to `<path>.processed.txt` in `{Abha=-23.0/18.0/59.2, Abidjan=-16.2/...`
/// format, where the value of each key is <min>/<mean>/<max>.
pub fn process_and_print(path: impl AsRef<Path>) {
    let mut file = File::open(path).unwrap();

    let mut thread_handles = Vec::with_capacity(64);

    // We will face situations where we don't entirely read the last line
    // entirely in a read operation. So, these bytes are saved so they can be
    // prepended to the new read data.
    let mut remaining_bytes_from_previous_iter =
        Vec::<u8>::with_capacity(32 /* refers to the line length  */);

    // Main threads read data in a loop.
    let begin = Instant::now();

    loop {
        // Allocations here are cheap compared to the intensive workloads
        // in the other thread.
        let mut read_buf = vec![0; READ_BUFFER_SIZE + remaining_bytes_from_previous_iter.len()];

        // Prepend partial data that is left from previous iteration.
        for (i, &byte) in remaining_bytes_from_previous_iter.iter().enumerate() {
            read_buf[i] = byte;
        }
        // Read buf slice: don't override the just prepended bytes
        let read_buf_slice = &mut read_buf[remaining_bytes_from_previous_iter.len()..];
        remaining_bytes_from_previous_iter.clear();

        // Note that .read() facilitates internal state which increases the
        // internal file pointer.
        let n = file.read(read_buf_slice).unwrap();

        if n == 0 {
            break;
        }

        // ensure vector shows true length
        read_buf.truncate(n);

        let (real, remaining) = split_unfinished_line_from_slice(&read_buf);
        remaining_bytes_from_previous_iter.clear();
        remaining_bytes_from_previous_iter.extend(remaining);

        // ensure vector shows true length
        read_buf.truncate(real.len());

        let (sender, receiver) = std::sync::mpsc::channel::<ThreadMessage>();
        let handle = spawn_worker_thread(receiver);
        thread_handles.push(handle);
        // These allocations won't happen too often. Relatively cheap.
        sender.send(ThreadMessage::Workload(read_buf)).unwrap();
        sender.send(ThreadMessage::Stop).unwrap();
    }
    assert!(
        remaining_bytes_from_previous_iter.is_empty(),
        "measurements.txt must end with newline"
    );

    eprintln!("time in IO thread: {:?}", begin.elapsed());

    // Combine all stats from all threads.
    let stats = thread_handles
        .into_iter()
        .map(|h| h.join().unwrap())
        .reduce(|mut l, r| {
            r.iter()
                .for_each(|(city, measurements)| l.merge_measurements(&city, measurements));
            l
        })
        .unwrap();

    print!("{{");
    stats
        .into_inner()
        .into_iter()
        .for_each(|(city, mut measurements)| {
            measurements.sort_unstable_by(|a, b| a.partial_cmp(b).unwrap());
            let sum = measurements.iter().fold(0.0, |acc, next| acc + next);
            let avg = sum / measurements.len() as f32;
            let min = measurements[0];
            let max = measurements[measurements.len() - 1];
            print!("{city}={min:.1}/{avg:.1}/{max:.1}, ")
        });
    println!("}}");
}

fn spawn_worker_thread(receiver: Receiver<ThreadMessage>) -> JoinHandle<Stats> {
    std::thread::spawn(move || {
        let begin = Instant::now();
        let mut stats = Stats::default();

        while let ThreadMessage::Workload(vec) = receiver.recv().unwrap() {
            let data = unsafe { core::str::from_utf8_unchecked(&vec) };
            data.lines().for_each(|line| {
                let (city, measurement) = line.split_once(';').unwrap();
                stats.add_record(city, measurement)
            });
        }

        eprintln!("time in calculation thread: {:?}", begin.elapsed());

        stats
    })
}

fn split_unfinished_line_from_slice(
    bytes: &[u8],
) -> (
    &[u8], /* only full lines */
    &[u8], /* remaining begin line */
) {
    let position_of_last_newline = bytes.iter().rev().position(|&byte| byte == b'\n').unwrap();
    let position_of_last_newline = bytes.len() - 1 - position_of_last_newline;
    // includes the newline as last character
    let slice_with_full_lines = &bytes[0..=position_of_last_newline];
    // Doesn't include the newline
    let remaining_bytes = &bytes[position_of_last_newline + 1..];

    (slice_with_full_lines, remaining_bytes)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_split_unfinished_line_from_slice() {
        let input = "foobar\nremaining";
        let (full, remaining) = split_unfinished_line_from_slice(input.as_bytes());
        assert_eq!("foobar\n".as_bytes(), full);
        assert_eq!("remaining".as_bytes(), remaining);
    }
}
