use criterion::{criterion_group, criterion_main, Criterion};

fn single_threaded_benchmarks(c: &mut Criterion) {
    c.bench_function("single: 1brc (100 entries)", |b| {
        b.iter(|| {
            phips_1brc::process_single_threaded("./measurements_100.txt", false);
        })
    });
    c.bench_function("single: 1brc (1000000 entries )", |b| {
        b.iter(|| {
            phips_1brc::process_single_threaded("./measurements_1000000.txt", false);
        })
    });
}

fn multi_threaded_benchmarks(c: &mut Criterion) {
    c.bench_function("multi: 1brc (100 entries)", |b| {
        b.iter(|| {
            phips_1brc::process_multi_threaded("./measurements_100.txt", false);
        })
    });
    c.bench_function("multi: 1brc (1000000 entries )", |b| {
        b.iter(|| {
            phips_1brc::process_multi_threaded("./measurements_1000000.txt", false);
        })
    });
}

criterion_group!(
    benches,
    single_threaded_benchmarks,
    multi_threaded_benchmarks
);
criterion_main!(benches);
