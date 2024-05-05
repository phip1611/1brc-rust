use std::collections::BTreeMap;
use std::fs::File;
use std::io::Read;
use std::ops::Deref;
use std::path::Path;
use std::str::FromStr;
use std::sync::mpsc::Receiver;
use std::thread::JoinHandle;
use std::time::Instant;

const READ_BUFFER_SIZE: usize = 0x10000000 /* 256 Mib */;

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

    let (sender, receiver) = std::sync::mpsc::channel::<ThreadMessage>();

    let calculation_thread = spawn_worker_thread(receiver);

    let mut read_buf = vec![0; READ_BUFFER_SIZE];

    // Main threads read data in a loop.
    let begin = Instant::now();
    loop {
        let n = file.read(&mut read_buf).unwrap();

        if n == 0 {
            break;
        }

        // ensure vector shows true length
        read_buf.truncate(n);

        // These allocations won't happen too often. Relatively cheap.
        let buf_copy = read_buf.clone();
        sender.send(ThreadMessage::Workload(buf_copy)).unwrap()
    }
    eprintln!("time in IO thread: {:?}", begin.elapsed());
    sender.send(ThreadMessage::Stop).unwrap();
    let stats = calculation_thread.join().unwrap();

    print!("{{");
    stats.into_inner().into_iter().for_each(|(city, mut measurements)| {
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

        let mut remaining_bytes_for_next_iter = Vec::new();

        while let ThreadMessage::Workload(vec) = receiver.recv().unwrap() {
            // Workload for this iteration is now the remaining bytes from
            // the previous iteration plus the new data.
            remaining_bytes_for_next_iter.extend(vec);
            let vec = remaining_bytes_for_next_iter;

            let (real, remaining) = split_unfinished_line_from_slice(&vec);
            let real = unsafe { core::str::from_utf8_unchecked(real) };

            real.lines().for_each(|line| {
                let (city, measurement) = line.split_once(';').unwrap();
                stats.add_record(city, measurement)
            });

            // Save remaining bytes for next iteration.
            remaining_bytes_for_next_iter = remaining.to_vec();
        }
        assert!(
            remaining_bytes_for_next_iter.is_empty(),
            "measurements.txt must end with newline"
        );

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
