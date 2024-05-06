use std::time::Instant;

fn main() {
    let begin = Instant::now();
    let file = std::env::args()
        .nth(1)
        .unwrap_or("./measurements.txt".to_string());
    let stats = phips_1brc::process(file);
    phips_1brc::print_results(stats.into_iter());
    println!("took {:?}", begin.elapsed());
}
