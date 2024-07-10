#![deny(
    clippy::all,
    clippy::cargo,
    clippy::nursery,
    clippy::must_use_candidate,
    // clippy::restriction,
    // clippy::pedantic
)]
// now allow a few rules which are denied by the above statement
// --> they are ridiculous and not necessary
#![allow(
    clippy::suboptimal_flops,
    clippy::redundant_pub_crate,
    clippy::fallible_impl_from
)]
// I can't do anything about this; fault of the dependencies
#![allow(clippy::multiple_crate_versions)]
// allow: required because of derive macro.. :(
#![allow(clippy::use_self)]
// Not needed here. We only need this for the library!
// #![deny(missing_docs)]
#![deny(missing_debug_implementations)]
#![deny(rustdoc::all)]

use std::io::{BufRead, BufReader};
use std::process::{Command, Stdio};
use std::time::Instant;

/// Public CLI is: `[/path/to/measurements.txt]`.
/// Internal CLI is: `/path/to/measurements.txt is_worker`.
fn main() {
    let begin = Instant::now();
    let mut args_iter = std::env::args();
    let program = args_iter.next().unwrap();
    let file = args_iter
        .next()
        .unwrap_or_else(|| "./measurements.txt".to_string());
    let is_worker = args_iter.next().unwrap_or_default() == "is_worker";

    // Unmapping the whole file is expensive (roughly 200ms on my machine). As
    // unmapping the file from the address space is part of the normal Linux
    // destruction process, we can't just use `drop(mmaped_file)` and are good
    // to go. A workaround to prevent the big overhead of unmapping is to use a
    // child process and do the unmapping there. The main process exits as soon
    // as the child performed its work.
    if is_worker {
        // mmap (and unmap) happens in child.
        phips_1brc::process_multi_threaded(file, true);
    } else {
        // Child has no drop implementation, and we don't manually wait for it.
        // We are not blocked on in.
        let mut child = Command::new(program)
            .args([file, "is_worker".to_string()])
            .stdout(Stdio::piped())
            .spawn()
            .unwrap();

        let child_stdout = child.stdout.take().unwrap();
        let mut stdout_reader = BufReader::new(child_stdout);
        let mut worker_output = String::new();
        // Synchronization point.
        //
        // We don't read until EOF but just this one line, which is the target
        // output of the challenge. Then, we don't care about the child, which
        // performs the very expensive mmap cleanup in background.
        stdout_reader.read_line(&mut worker_output).unwrap();
        println!("{worker_output}");
        println!("took {:?}", begin.elapsed());
    }
}
