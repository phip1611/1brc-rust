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

use std::time::Instant;

fn main() {
    let begin = Instant::now();
    let file = std::env::args()
        .nth(1)
        .unwrap_or_else(|| "./measurements.txt".to_string());
    phips_1brc::process_single_threaded(file, true);
    println!("took {:?}", begin.elapsed());
}
