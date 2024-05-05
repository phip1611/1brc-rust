# 1BRC - 1 Billion Row Challenge (My Attempt with Rust)

This is my attempt for the [1 billion row challenge](https://github.com/gunnarmorling/1brc).
I didn't spend too much time on it, but looked what I can achieve in an
afternoon.


## How to Run

- `cargo run --release`

The build script will automatically init the Git submodule, build the Maven
project, and run the script that generates the test data. This takes quite a
few minutes, as one billion data rows are generated. If you want to accelerate
that process, place your own `measurements.txt` in the root of the project.
