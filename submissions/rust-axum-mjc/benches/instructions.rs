mod common;

use common::{AuthFixture, DbFixture};
use gungraun::client_requests::callgrind;
use gungraun::{
    Callgrind, EntryPoint, LibraryBenchmarkConfig, OutputFormat, library_benchmark,
    library_benchmark_group, main,
};

// Return ownership so fixture destruction is also outside the measured function.
#[library_benchmark(setup = AuthFixture::new)]
fn auth_hs256(fixture: AuthFixture) -> AuthFixture {
    callgrind::start_instrumentation();
    fixture.verify();
    callgrind::stop_instrumentation();
    fixture
}

#[library_benchmark(setup = DbFixture::new)]
fn sqlite_health(fixture: DbFixture) -> DbFixture {
    callgrind::start_instrumentation();
    fixture.health();
    callgrind::stop_instrumentation();
    fixture
}

#[library_benchmark(setup = DbFixture::new)]
fn sqlite_feed(fixture: DbFixture) -> DbFixture {
    callgrind::start_instrumentation();
    fixture.feed();
    callgrind::stop_instrumentation();
    fixture
}

#[library_benchmark(setup = DbFixture::new)]
fn sqlite_post(fixture: DbFixture) -> DbFixture {
    callgrind::start_instrumentation();
    fixture.post();
    callgrind::stop_instrumentation();
    fixture
}

#[library_benchmark(setup = DbFixture::new)]
fn sqlite_duplicate_like(fixture: DbFixture) -> DbFixture {
    callgrind::start_instrumentation();
    fixture.duplicate_like();
    callgrind::stop_instrumentation();
    fixture
}

library_benchmark_group!(
    name = requests,
    max_parallel = 1,
    benchmarks = [
        auth_hs256,
        sqlite_health,
        sqlite_feed,
        sqlite_post,
        sqlite_duplicate_like
    ]
);

main!(
    // The default per-function collection toggle would miss the SQLite worker.
    // Global instrumentation bounds include it while excluding fixture setup.
    config = LibraryBenchmarkConfig::default()
        .tool(
            Callgrind::with_args(["--collect-atstart=yes", "--instr-atstart=no"])
                .entry_point(EntryPoint::None)
        )
        .output_format(OutputFormat::default().show_intermediate(true)),
    library_benchmark_groups = requests
);
