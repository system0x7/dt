use criterion::{black_box, criterion_group, criterion_main, Criterion};
use data_transform::{parse, Executor};

fn benchmark_parse(c: &mut Criterion) {
    c.bench_function("parse simple pipeline", |b| {
        b.iter(|| {
            parse(black_box("read('data.csv') | select($1, $2, $3) | filter(age > 25)"))
        });
    });
}

criterion_group!(benches, benchmark_parse);
criterion_main!(benches);
