# 1BRC - 1 Billion Row Challenge (My Attempt with Rust)

This is my attempt for the [1 billion row challenge](https://github.com/gunnarmorling/1brc).

I especially put focus on a highly optimized single-threaded solution. I learned
a lot about performance optimizations and the costs of certain `std`
abstractions and implementations.


## How to Run

- `cargo run --release`

The build script will automatically init the Git submodule, build the Maven
project, and run the script that generates the test data. This takes quite a
few minutes, as one billion data rows are generated. If you want to accelerate
that process, place your own `measurements.txt` in the root of the project.

## Outcome

In the end, I could process 1 billion rows in ~28 seconds on my laptop with
the following properties:
- AMD Ryzen 7 7840U w/ Radeon  780M Graphics
- Caches (sum of all):\
  L1d:                    256 KiB (8 instances)\
  L1i:                    256 KiB (8 instances)\
  L2:                     8 MiB (8 instances)\
  L3:                     16 MiB (1 instance)
- 32GB of RAM
