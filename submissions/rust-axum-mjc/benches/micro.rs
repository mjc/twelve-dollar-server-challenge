mod common;

use common::{AuthFixture, DbFixture};

fn main() {
    divan::main();
}

#[divan::bench]
fn auth_hs256(bencher: divan::Bencher) {
    let fixture = AuthFixture::new();
    bencher.bench_local(|| fixture.verify());
}

#[divan::bench]
fn sqlite_health(bencher: divan::Bencher) {
    let fixture = DbFixture::new();
    bencher.bench_local(|| fixture.health());
}

#[divan::bench]
fn sqlite_feed(bencher: divan::Bencher) {
    let fixture = DbFixture::new();
    bencher.bench_local(|| fixture.feed());
}

#[divan::bench]
fn sqlite_post(bencher: divan::Bencher) {
    let fixture = DbFixture::new();
    bencher.bench_local(|| fixture.post());
}

#[divan::bench]
fn sqlite_duplicate_like(bencher: divan::Bencher) {
    let fixture = DbFixture::new();
    bencher.bench_local(|| fixture.duplicate_like());
}
