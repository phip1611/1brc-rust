use std::time::Instant;

fn main() {
    let begin = Instant::now();
    let file = std::env::args()
        .nth(1)
        .unwrap_or("./measurements.txt".to_string());
    phips_1brc::process_and_print(file);
    println!("took {:?}", begin.elapsed());
}
