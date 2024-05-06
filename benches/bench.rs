use criterion::{criterion_group, criterion_main, Criterion};

fn criterion_benchmark(c: &mut Criterion) {
    c.bench_function("1brc (100 entries)", |b| {
        b.iter(|| {
            phips_1brc::process_single_threaded("./measurements_100.txt", false);
        })
    });
    c.bench_function("1brc (1000000 entries )", |b| {
        b.iter(|| {
            phips_1brc::process_single_threaded("./measurements_1000000.txt", false);
        })
    });
}

criterion_group!(benches, criterion_benchmark);
criterion_main!(benches);
