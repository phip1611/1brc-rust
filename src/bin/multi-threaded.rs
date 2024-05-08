use std::fs;
use std::io::Read;
use std::os::unix::net::UnixListener;
use std::process::{Command, Stdio};
use std::time::Instant;

/// Public CLI is: `[/path/to/measurements.txt]`.
/// Internal CLI is: `/path/to/measurements.txt is_worker`.
fn main() {
    let begin = Instant::now();
    let mut args_iter = std::env::args();
    let program = args_iter.next().unwrap();
    let file = args_iter.next().unwrap_or("./measurements.txt".to_string());
    let is_worker = args_iter.next().unwrap_or("".to_string()) == "is_worker";

    // Unmapping the whole file is expensive (roughly 200ms on my machine). As
    // unmapping the file from the address space is part of the normal Linux
    // destruction process, we can't just use `drop(mmaped_file)` and are good
    // to go. A workaround to prevent the big overhead of unmapping is to use a
    // child process and do the unmapping there. The main process exits as soon
    // as the child performed its work.
    //
    // The worker currently notifies the main process that it is done via a
    // UNIX domain socket. The overhead is negligible compared to the
    // performance save.
    if is_worker {
        // mmap (and unmap) happens in child.
        phips_1brc::process_multi_threaded(file, true);
    } else {
        // Child has no drop implementation, and we don't manually wait for it.
        // We are not blocked on in.
        let _child = Command::new(program)
            .args([file, "is_worker".to_string()])
            .stdout(Stdio::inherit())
            .spawn()
            .unwrap();

        // TODO is there a more lightweight notification mechanism?
        let socket = UnixListener::bind("/tmp/1brc-notify-socket").unwrap();
        // eprintln!("waiting for socket info");
        socket.incoming().take(1).for_each(|x| {
            let mut s = x.unwrap();
            s.read(&mut [0]).unwrap();
        });
        // eprintln!("got notified via socket");
        println!("took {:?}", begin.elapsed());
        fs::remove_file("/tmp/1brc-notify-socket").unwrap();
    }
}
