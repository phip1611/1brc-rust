use criterion::{black_box, criterion_group, criterion_main, Criterion};

fn criterion_benchmark(c: &mut Criterion) {
    c.bench_function("1brc (100 entries)", |b| {
        b.iter(|| {
            let stats = phips_1brc::process("./measurements_100.txt");
            let _x = black_box(stats);
        })
    });
    c.bench_function("1brc (10000 entries )", |b| {
        b.iter(|| {
            let stats = phips_1brc::process("./measurements_10000.txt");
            let _x = black_box(stats);
        })
    });
}

criterion_group!(benches, criterion_benchmark);
criterion_main!(benches);
