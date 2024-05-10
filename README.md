# 1BRC - 1 Billion Row Challenge (My Attempt with Rust)

This is my attempt for the [1 billion row challenge](https://github.com/gunnarmorling/1brc). In the meantime,
this went through multiple iterations and I optimized it. A fun challenge!

## TLDR; Results

To process 1 billion rows, a file of roughly 14 GB in size, my code ran:

**Single Threaded**: ~19.9 seconds

**Multi Threaded** : ~2.3 seconds (16 threads)

_These timings include everything, from reading the file, processing it, and
printing it to stdout. However, as I'm using mmap and have lots of RAM, Linux
has the whole file in the in-kernel FS cache._

## About my Solution (⚠️ SPOILER ALERT)

My algorithmic choices are specifically trimmed to the official [data set generator](https://github.com/gunnarmorling/1brc/blob/db064194be375edc02d6dbcd21268ad40f7e2869/src/main/java/dev/morling/onebrc/CreateMeasurements.java).

At first, I especially put focus on a highly optimized single-threaded solution.
I learned a lot about performance optimizations and the costs of certain `std`
abstractions and implementations. I got some inspiration by other solutions
out there and had interesting discussions as well as findings.

My takeaways for a performant solution (⚠️ **SPOILER ALERT**):

- `mmap` whole file into address space
  - no more user-space buffering necessary
  - I didn't do however an excessive evaluation regarding the performance of
    an mmap'ed file
- use `memchr` instead of Rust iterator API to find the position of a certain
  byte
  - uses optimized AVX instructions, which rustc surprisingly can't use, even
    with `RUSTFLAGS=-C target-cpu=native`
- encode all measurements as integers multiplied by ten
  - `-15.7` -> `-157`
  - --> no f32 on hot path
- never iterate any data twice / more than necessary
  - first look for `;`, then for `\n`
- **NO** allocations on the hot path (use pre-allocated buffers where necessary)
- there are faster hashing algorithms than the one from the standard library,
  which are good enough for this challenge
- no unnecessary buffering/copying

My multithreaded approach is a wrapper around the logic I came up with for the
single-threaded solution. Each thread gets an equal chunk of memory. The only
challenge here is that each chunk must end with a newline. I wrote an testable
iterator that helped me solving this.

Creating the `n` threads (one per CPU) is negligible, as well as collecting and
aggregating the result in the main thread. I was surprised by that, but that's
what I measured.

## How to Run

- `cargo run --release --bin single-threaded [-- <path to measurements.txt>]`, or
- `cargo run --release --bin multi-threaded [-- <path to measurements.txt>]`

The build script will automatically init the Git submodule, build the Maven
project, and run the script that generates the test data, if not present yet.
This takes quite a few minutes, as one billion data rows are generated. The
resulting file is roughly 14GB in size. If you want to accelerate that process,
place your own `measurements.txt` in the root of the project.

## My Machine

- Framework 13 Laptop
- AMD Ryzen 7 7840U w/ Radeon  780M Graphics (16 cpus)
- Caches (sum of all):\
  L1d:                    256 KiB (8 instances)\
  L1i:                    256 KiB (8 instances)\
  L2:                     8 MiB (8 instances)\
  L3:                     16 MiB (1 instance)
- 32GB of RAM
- WD Black SN850X NVMe (6 GB/s read)
