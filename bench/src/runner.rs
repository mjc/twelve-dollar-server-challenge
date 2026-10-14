use super::{load_tokens, parse_source_ips, passes, setting, Samples, ENDPOINTS};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use std::{
    collections::BTreeSet,
    env,
    error::Error,
    fs::{self, File},
    io::{BufReader, Read},
    net::IpAddr,
    os::unix::process::CommandExt,
    path::{Path, PathBuf},
    process::{Child, Command, ExitStatus, Stdio},
    sync::{
        atomic::{AtomicBool, Ordering},
        mpsc, Arc,
    },
    thread,
    time::{Duration, Instant},
};

type Result<T> = std::result::Result<T, Box<dyn Error>>;

#[derive(Clone, Serialize)]
struct Worker {
    name: String,
    users: usize,
    user_offset: usize,
    remote_dir: Option<String>,
    exit_code: Option<i32>,
    error: Option<String>,
}

#[derive(Deserialize)]
struct Report {
    maximum_users: usize,
    total_users: usize,
    started_at: DateTime<Utc>,
    finished_at: DateTime<Utc>,
    hold_started_at: Option<DateTime<Utc>>,
    hold_finished_at: Option<DateTime<Utc>>,
    planned_hold_s: f64,
    hosts: BTreeSet<String>,
    source_ips: Vec<IpAddr>,
    runtime_errors: usize,
    completed: bool,
    aggregate: Value,
}

// Quote every argument for the remote POSIX shell, including user-supplied flags.
fn quote(value: &str) -> String {
    format!("'{}'", value.replace('\'', "'\\''"))
}

fn ssh(user: &str, ip: &str) -> Command {
    let mut cmd = Command::new("ssh");
    cmd.args([
        "-o",
        "BatchMode=yes",
        "-o",
        "ConnectTimeout=10",
        "-o",
        "ServerAliveInterval=15",
        "-o",
        "ServerAliveCountMax=3",
        "-l",
        user,
        "--",
        ip,
    ]);
    cmd
}

fn destination(user: &str, ip: &str, remote_dir: &str) -> String {
    let host = if ip.contains(':') {
        format!("[{ip}]")
    } else {
        ip.into()
    };
    format!("{user}@{host}:{remote_dir}")
}

fn scp() -> Command {
    let mut cmd = Command::new("scp");
    cmd.args(["-q", "-o", "BatchMode=yes", "-o", "ConnectTimeout=10"]);
    cmd
}

fn checked(cmd: &mut Command, cancelled: &AtomicBool) -> Result<()> {
    let mut child = cmd
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::piped())
        .spawn()?;
    let mut stderr = child.stderr.take().unwrap();
    let (sender, receiver) = mpsc::channel();
    thread::spawn(move || {
        let mut message = String::new();
        let _ = stderr.read_to_string(&mut message);
        let _ = sender.send(message);
    });
    let status = wait_child(&mut child, cancelled, Some(Duration::from_secs(120)));
    // A proxy descendant may retain stderr after its parent was terminated.
    let message = receiver
        .recv_timeout(Duration::from_secs(1))
        .unwrap_or_default();
    let status = status?;
    if !status.success() {
        return Err(format!("command failed ({}): {}", status, message.trim()).into());
    }
    Ok(())
}

fn execute(cmd: &mut Command, cancelled: &AtomicBool) -> Result<ExitStatus> {
    // The coordinator forwards one SIGINT; terminal Ctrl-C must not also reach Goose.
    let mut child = cmd.stdin(Stdio::null()).process_group(0).spawn()?;
    wait_child(&mut child, cancelled, None)
}

fn interrupt_child(child: &Child) -> Result<()> {
    // SAFETY: this is our live, unreaped child; its PID cannot be reused.
    let result = unsafe { libc::kill(child.id() as libc::pid_t, libc::SIGINT) };
    if result != 0 {
        return Err(std::io::Error::last_os_error().into());
    }
    Ok(())
}

fn wait_child(
    child: &mut Child,
    cancelled: &AtomicBool,
    timeout: Option<Duration>,
) -> Result<ExitStatus> {
    let started = Instant::now();
    let mut stopping = None;
    loop {
        if let Some(status) = child.try_wait()? {
            return Ok(status);
        }
        if cancelled.load(Ordering::Relaxed) {
            if timeout.is_none() && stopping.is_none() {
                interrupt_child(child)?;
                stopping = Some(Instant::now());
            }
            if timeout.is_some()
                || stopping.is_some_and(|at: Instant| at.elapsed() >= Duration::from_secs(10))
            {
                child.kill()?;
                return Ok(child.wait()?);
            }
        }
        if timeout.is_some_and(|limit| started.elapsed() >= limit) {
            child.kill()?;
            child.wait()?;
            return Err("transfer/control command timed out".into());
        }
        thread::sleep(Duration::from_millis(100));
    }
}

pub(super) fn supervise(control: &Path, args: &[String]) -> Result<u8> {
    fs::write(control.with_extension("ready"), "ready\n")?;
    let deadline = Instant::now() + Duration::from_secs(120);
    while !control.with_extension("start").exists() {
        if control.exists() || Instant::now() >= deadline {
            fs::write(control.with_extension("done"), "not started\n")?;
            return Err("coordinated start cancelled or timed out".into());
        }
        thread::sleep(Duration::from_millis(100));
    }
    let start_at: i64 = fs::read_to_string(control.with_extension("start"))?
        .trim()
        .parse()?;
    if Utc::now().timestamp_millis() - start_at > 250 {
        fs::write(control.with_extension("done"), "missed start\n")?;
        return Err(
            "missed coordinated start; check SSH delay and host clock synchronization".into(),
        );
    }
    let mut child = Command::new(env::current_exe()?)
        .args(args)
        .env("BENCH_START_AT", start_at.to_string())
        .stdin(Stdio::null())
        .spawn()?;
    let mut stopping = None;
    let status = loop {
        if let Some(status) = child.try_wait()? {
            break status;
        }
        if control.exists() && stopping.is_none() {
            interrupt_child(&child)?;
            stopping = Some(Instant::now());
        }
        if stopping.is_some_and(|started| started.elapsed() >= Duration::from_secs(10)) {
            child.kill()?;
            break child.wait()?;
        }
        thread::sleep(Duration::from_millis(100));
    };
    fs::write(control.with_extension("done"), format!("{status}\n"))?;
    Ok(status.code().unwrap_or(2) as u8)
}

pub(super) fn ready_supervised(control: &Path) -> Result<u8> {
    let deadline = Instant::now() + Duration::from_secs(30);
    while !control.with_extension("ready").exists() {
        if Instant::now() >= deadline {
            return Err("remote generator did not become ready".into());
        }
        thread::sleep(Duration::from_millis(100));
    }
    Ok(0)
}

pub(super) fn start_supervised(control: &Path, timestamp: &str) -> Result<u8> {
    let _: i64 = timestamp.parse()?;
    let temporary = control.with_extension("start.tmp");
    fs::write(&temporary, timestamp)?;
    fs::rename(temporary, control.with_extension("start"))?;
    Ok(0)
}

pub(super) fn stop_supervised(control: &Path) -> Result<u8> {
    fs::write(control, "cancel\n")?;
    let deadline = Instant::now() + Duration::from_secs(15);
    while !control.with_extension("done").exists() {
        if Instant::now() >= deadline {
            return Err("remote supervisor did not confirm shutdown".into());
        }
        thread::sleep(Duration::from_millis(100));
    }
    Ok(0)
}

fn split_users(total: usize, count: usize) -> Result<Vec<(usize, usize)>> {
    if count == 0 || total < count {
        return Err("VUS must be at least the number of generator hosts".into());
    }
    let mut offset = 0;
    Ok((0..count)
        .map(|index| {
            let users = total / count + usize::from(index < total % count);
            let assignment = (users, offset);
            offset += users;
            assignment
        })
        .collect())
}

fn collect(output: &Path, workers: &mut [Worker]) -> Value {
    let mut samples: [Samples; 4] = std::array::from_fn(|_| Samples::default());
    let mut maximum_users = 0;
    let mut total_users = 0;
    let mut runtime_errors = 0;
    let mut started: Option<DateTime<Utc>> = None;
    let mut finished: Option<DateTime<Utc>> = None;
    let mut hosts = BTreeSet::new();
    let mut source_ips = BTreeSet::new();
    let mut completed = true;
    let mut hold_start = None;
    let mut hold_end = None;
    let mut required_hold = 0.0_f64;
    for worker in workers.iter_mut() {
        let parsed = (|| -> Result<(Report, [Samples; 4])> {
            let dir = output.join(&worker.name);
            let report: Report =
                serde_json::from_reader(BufReader::new(File::open(dir.join("summary.json"))?))?;
            let raw: [Samples; 4] =
                serde_json::from_reader(BufReader::new(File::open(dir.join("samples.json"))?))?;
            if raw.iter().any(|s| {
                s.failed > s.latency_ms.len()
                    || s.latency_ms.iter().any(|ms| !ms.is_finite() || *ms < 0.0)
            }) {
                return Err("invalid latency samples or failure counts".into());
            }
            let requests: usize = raw.iter().map(|s| s.latency_ms.len()).sum();
            let failed: usize = raw.iter().map(|s| s.failed).sum();
            if report.aggregate["requests"].as_u64() != Some(requests as u64)
                || report.aggregate["failed"].as_u64() != Some(failed as u64)
                || report.finished_at < report.started_at
                || (worker.remote_dir.is_some()
                    && (report.maximum_users != worker.users || report.total_users != worker.users))
            {
                return Err("summary does not match raw samples or timestamps".into());
            }
            Ok((report, raw))
        })();
        match parsed {
            Ok((report, raw)) => {
                completed &= report.completed && matches!(worker.exit_code, Some(0 | 1));
                maximum_users += report.maximum_users;
                total_users += report.total_users;
                runtime_errors += report.runtime_errors;
                started = Some(started.map_or(report.started_at, |s| s.min(report.started_at)));
                finished = Some(finished.map_or(report.finished_at, |s| s.max(report.finished_at)));
                hosts.extend(report.hosts);
                source_ips.extend(report.source_ips);
                if worker.remote_dir.is_some() && report.planned_hold_s > 0.0 {
                    required_hold = required_hold.max(report.planned_hold_s);
                    if let (Some(begin), Some(end)) =
                        (report.hold_started_at, report.hold_finished_at)
                    {
                        hold_start =
                            Some(hold_start.map_or(begin, |s: DateTime<Utc>| s.max(begin)));
                        hold_end = Some(hold_end.map_or(end, |s: DateTime<Utc>| s.min(end)));
                    } else {
                        completed = false;
                    }
                }
                for (combined, mut received) in samples.iter_mut().zip(raw) {
                    combined.latency_ms.append(&mut received.latency_ms);
                    combined.failed += received.failed;
                    combined.checks_failed += received.checks_failed;
                }
            }
            Err(error) => {
                completed = false;
                worker.error.get_or_insert_with(|| error.to_string());
            }
        }
    }
    completed &= workers.iter().all(|worker| worker.error.is_none());
    let common_hold_s = hold_start
        .zip(hold_end)
        .and_then(|(s, f)| (f - s).to_std().ok())
        .map(|d| d.as_secs_f64());
    if required_hold > 0.0 {
        completed &= common_hold_s.is_some_and(|s| s + 0.25 >= required_hold);
    }
    let mut aggregate = Samples::default();
    let mut endpoints = std::collections::BTreeMap::new();
    for (endpoint, sample) in ENDPOINTS.iter().zip(samples.iter_mut()) {
        aggregate.latency_ms.extend_from_slice(&sample.latency_ms);
        aggregate.failed += sample.failed;
        aggregate.checks_failed += sample.checks_failed;
        endpoints.insert(*endpoint, sample.summary());
    }
    let summary = aggregate.summary();
    json!({
        "generator": "goose 0.18.1", "workers": workers,
        "completed": completed, "passed": completed && passes(&summary, runtime_errors),
        "runtime_errors": runtime_errors, "maximum_users": maximum_users,
        "total_users": total_users, "hosts": hosts, "source_ips": source_ips,
        "started_at": started, "finished_at": finished,
        "common_hold_s": common_hold_s, "required_hold_s": required_hold,
        // An elapsed interval across all workers, not a sum of their durations.
        "duration_s": started.zip(finished).and_then(|(s, f)| (f-s).to_std().ok()).map(|d| d.as_secs_f64()),
        "latency": "client wall time through complete response body, including connection setup",
        "percentiles": "k6 linear interpolation over all workers' unrounded samples",
        "thresholds": { "p95_ms_lt": 500, "p99_ms_lt": 1000, "failure_rate_lt": 0.01 },
        "aggregate": summary, "endpoints": endpoints,
    })
}

pub(super) fn run(args: &[String], cancelled: Arc<AtomicBool>) -> Result<u8> {
    let mut username = None;
    let mut output_dir = None;
    let mut options = Vec::new();
    let mut args = args.iter();
    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--user" => username = Some(args.next().ok_or("--user requires a username")?.clone()),
            "--output-dir" => {
                output_dir = Some(PathBuf::from(
                    args.next().ok_or("--output-dir requires a path")?,
                ))
            }
            _ => options.push(arg.clone()),
        }
    }
    let binary = env::current_exe()?;
    if options.iter().any(|arg| {
        matches!(
            arg.as_str(),
            "--help" | "-h" | "--version" | "-V" | "--list" | "-l" | "--scenarios-list"
        )
    }) {
        return Ok(Command::new(binary)
            .args(options)
            .status()?
            .code()
            .unwrap_or(2) as u8);
    }
    let ips = parse_source_ips(&setting("SOURCE_IPS", ""))?;
    let users: usize = setting("VUS", "100").parse()?;
    if users == 0 {
        return Err("VUS must be positive".into());
    }
    let tokens = fs::canonicalize(setting("TOKENS", "../seed/tokens.json"))?;
    load_tokens(&tokens)?;
    let output = output_dir.unwrap_or_else(|| {
        PathBuf::from(format!(
            "../results/run-{}-{}",
            Utc::now().timestamp_millis(),
            std::process::id()
        ))
    });
    fs::create_dir_all(
        output
            .parent()
            .filter(|p| !p.as_os_str().is_empty())
            .unwrap_or(Path::new(".")),
    )?;
    // Refuse reuse, so stale reports can never make a failed run pass.
    fs::create_dir(&output)?;
    let output = fs::canonicalize(output)?;
    println!("Results: {}", output.display());
    fs::write(
        output.join("aggregate.json"),
        "{\"completed\":false,\"passed\":false,\"status\":\"running\"}\n",
    )?;
    let mut workers = Vec::new();
    if let Some(user) = &username {
        if user.is_empty()
            || user.starts_with('-')
            || !user
                .chars()
                .all(|c| c.is_ascii_alphanumeric() || "_.-".contains(c))
        {
            return Err("invalid SSH username".into());
        }
        if ips.iter().collect::<BTreeSet<_>>().len() != ips.len() {
            return Err("SSH generator addresses must be unique".into());
        }
        // A custom Goose test plan/users setting would bypass the VUS split.
        if options.iter().any(|arg| {
            let flag = arg.split('=').next().unwrap();
            [
                "--test-plan",
                "--users",
                "--iterations",
                "--run-time",
                "--startup-time",
                "--hatch-rate",
                "--no-autostart",
            ]
            .contains(&flag)
                || ["-u", "-r", "-s", "-t"]
                    .iter()
                    .any(|prefix| arg.starts_with(prefix))
        }) {
            return Err("in SSH mode use VUS, RAMP_UP and DURATION; per-host user/test-plan overrides are unsupported".into());
        }
        for (index, (ip, (count, offset))) in
            ips.iter().zip(split_users(users, ips.len())?).enumerate()
        {
            let name = format!("host-{:03}-{}", index + 1, ip.to_string().replace(':', "_"));
            let dir = output.join(&name);
            fs::create_dir(&dir)?;
            let remote_dir = format!(
                "/tmp/feed-load-{}-{}-{}",
                Utc::now().timestamp_millis(),
                std::process::id(),
                index
            );
            let mut worker = Worker {
                name,
                users: count,
                user_offset: offset,
                remote_dir: Some(remote_dir.clone()),
                exit_code: None,
                error: None,
            };
            let upload = (|| -> Result<()> {
                if cancelled.load(Ordering::Relaxed) {
                    return Err("interrupted".into());
                }
                checked(
                    ssh(user, &ip.to_string()).arg(format!(
                        "umask 077; mkdir -- {} && mkdir -- {}",
                        quote(&remote_dir),
                        quote(&format!("{remote_dir}/results"))
                    )),
                    &cancelled,
                )?;
                checked(
                    scp().arg("--").arg(&binary).arg(destination(
                        user,
                        &ip.to_string(),
                        &format!("{remote_dir}/feed-load"),
                    )),
                    &cancelled,
                )?;
                checked(
                    scp().arg("--").arg(&tokens).arg(destination(
                        user,
                        &ip.to_string(),
                        &format!("{remote_dir}/tokens.json"),
                    )),
                    &cancelled,
                )?;
                // Check the copied executable before launching any load on any host.
                checked(
                    ssh(user, &ip.to_string()).arg(format!(
                        "{} --version",
                        quote(&format!("{remote_dir}/feed-load"))
                    )),
                    &cancelled,
                )?;
                Ok(())
            })();
            if let Err(error) = upload {
                worker.error = Some(error.to_string());
            }
            workers.push(worker);
        }
        // Upload all hosts first; a failed copy must not start a partial load.
        if workers.iter().all(|worker| worker.error.is_none()) {
            let controls: Vec<_> = workers
                .iter()
                .zip(&ips)
                .map(|(worker, ip)| (ip.to_string(), worker.remote_dir.clone().unwrap()))
                .collect();
            let mut joins = Vec::new();
            for (worker, ip) in workers.into_iter().zip(ips) {
                let output = output.clone();
                let user = user.clone();
                let options = options.clone();
                let cancelled = cancelled.clone();
                let base_url = setting("BASE_URL", "http://127.0.0.1:3000");
                let ramp = setting("RAMP_UP", "60s");
                let duration = setting("DURATION", "5m");
                joins.push(thread::spawn(move || {
                    let mut worker = worker;
                    let dir = output.join(&worker.name);
                    let remote_dir = worker.remote_dir.as_ref().unwrap();
                    let result = (|| -> Result<()> {
                        let assignments = [
                            format!("BASE_URL={base_url}"),
                            format!("VUS={}", worker.users),
                            format!("RAMP_UP={ramp}"),
                            format!("DURATION={duration}"),
                            format!("BENCH_USER_OFFSET={}", worker.user_offset),
                            "TOKENS=../tokens.json".into(),
                            "SUMMARY=summary.json".into(),
                            "BENCH_SAMPLES=samples.json".into(),
                            "SOURCE_IPS=".into(),
                        ];
                        let command = format!(
                            "cd {} || exit 2; trap '' HUP; exec env {} ../feed-load --supervise ../cancel {}",
                            quote(&format!("{remote_dir}/results")),
                            assignments
                                .iter()
                                .map(|v| quote(v))
                                .collect::<Vec<_>>()
                                .join(" "),
                            options
                                .iter()
                                .map(|v| quote(v))
                                .collect::<Vec<_>>()
                                .join(" ")
                        );
                        let log = File::create(dir.join("output.log"))?;
                        let status = execute(
                            ssh(&user, &ip.to_string())
                                .arg(command)
                                .stdout(Stdio::from(log.try_clone()?))
                                .stderr(Stdio::from(log)),
                            &cancelled,
                        )?;
                        worker.exit_code = status.code();
                        if cancelled.load(Ordering::Relaxed) || !matches!(worker.exit_code, Some(0 | 1)) {
                            checked(ssh(&user, &ip.to_string()).arg(format!("{} --stop-supervised {}", quote(&format!("{remote_dir}/feed-load")), quote(&format!("{remote_dir}/cancel")))), &AtomicBool::new(false))?;
                        }

                        // Collect reports even when the benchmark failed a threshold.
                        let remote = destination(&user, &ip.to_string(), remote_dir);
                        checked(
                            scp()
                                .arg("-r")
                                .arg("--")
                                .arg(format!("{remote}/results/."))
                                .arg(&dir),
                            &AtomicBool::new(false),
                        )?;
                        if !matches!(worker.exit_code, Some(0 | 1)) {
                            return Err(format!("remote execution failed: {status}").into());
                        }
                        Ok(())
                    })();
                    if let Err(error) = result {
                        worker.error = Some(error.to_string());
                    }
                    worker
                }));
            }
            let coordination = (|| -> Result<()> {
                for (ip, dir) in &controls {
                    checked(
                        ssh(user, ip).arg(format!(
                            "{} --ready-supervised {}",
                            quote(&format!("{dir}/feed-load")),
                            quote(&format!("{dir}/cancel"))
                        )),
                        &cancelled,
                    )?;
                }
                // All SSH sessions are ready before any generator starts.
                let start_at = (Utc::now().timestamp_millis() + 5000).to_string();
                let publications = thread::scope(|scope| {
                    let mut handles = Vec::new();
                    for (ip, dir) in &controls {
                        let start_at = &start_at;
                        let cancelled = &cancelled;
                        handles.push(scope.spawn(move || {
                            checked(
                                ssh(user, ip).arg(format!(
                                    "{} --start-supervised {} {}",
                                    quote(&format!("{dir}/feed-load")),
                                    quote(&format!("{dir}/cancel")),
                                    quote(start_at)
                                )),
                                cancelled,
                            )
                            .map_err(|error| error.to_string())
                        }));
                    }
                    handles
                        .into_iter()
                        .map(|handle| {
                            handle
                                .join()
                                .unwrap_or_else(|_| Err("start publication panicked".into()))
                        })
                        .collect::<Vec<_>>()
                });
                for result in publications {
                    result?;
                }
                Ok(())
            })();
            if let Err(error) = coordination {
                eprintln!("coordinated start failed: {error}");
                cancelled.store(true, Ordering::Relaxed);
            }
            workers = joins
                .into_iter()
                .map(|join| join.join().expect("worker thread panicked"))
                .collect();
        }
    } else {
        let dir = output.join("local");
        fs::create_dir(&dir)?;
        let log = File::create(dir.join("output.log"))?;
        let status = execute(
            Command::new(&binary)
                .args(options)
                .current_dir(&dir)
                .env("TOKENS", tokens)
                .env("SUMMARY", dir.join("summary.json"))
                .env("BENCH_SAMPLES", dir.join("samples.json"))
                .env("BENCH_USER_OFFSET", "0")
                .env_remove("BENCH_START_AT")
                .stdout(Stdio::from(log.try_clone()?))
                .stderr(Stdio::from(log)),
            &cancelled,
        );
        let (exit_code, error) = match status {
            Ok(status) => (
                status.code(),
                if matches!(status.code(), Some(0 | 1)) {
                    None
                } else {
                    Some(format!("local execution failed: {status}"))
                },
            ),
            Err(error) => (None, Some(error.to_string())),
        };
        workers.push(Worker {
            name: "local".into(),
            users,
            user_offset: 0,
            remote_dir: None,
            exit_code,
            error,
        });
    }
    let mut aggregate = collect(&output, &mut workers);
    if cancelled.load(Ordering::Relaxed) {
        aggregate["completed"] = json!(false);
        aggregate["passed"] = json!(false);
        aggregate["interrupted"] = json!(true);
    }
    fs::write(
        output.join("aggregate.json"),
        format!("{}\n", serde_json::to_string_pretty(&aggregate)?),
    )?;
    // Keep the previous SUMMARY destination available alongside the run directory.
    if let Some(legacy) = env::var("SUMMARY").ok().filter(|s| !s.is_empty()) {
        let path = Path::new(&legacy);
        if let Some(parent) = path.parent().filter(|p| !p.as_os_str().is_empty()) {
            fs::create_dir_all(parent)?;
        }
        fs::write(
            path,
            format!("{}\n", serde_json::to_string_pretty(&aggregate)?),
        )?;
    }
    println!(
        "{} — {}",
        if aggregate["passed"] == true {
            "PASS"
        } else {
            "FAIL"
        },
        output.join("aggregate.json").display()
    );
    if workers.iter().any(|worker| worker.error.is_some()) {
        return Ok(2);
    }
    Ok(if aggregate["passed"] == true { 0 } else { 1 })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn fixture(name: &str, samples: Vec<f64>, offset: i64) -> (PathBuf, Worker) {
        let dir =
            env::temp_dir().join(format!("feed-load-aggregate-{name}-{}", std::process::id()));
        fs::create_dir_all(dir.join(name)).unwrap();
        let mut raw: [Samples; 4] = std::array::from_fn(|_| Samples::default());
        raw[0].latency_ms = samples;
        let summary = raw[0].summary();
        let start = DateTime::parse_from_rfc3339("2026-01-01T00:00:00Z")
            .unwrap()
            .with_timezone(&Utc)
            + chrono::TimeDelta::seconds(offset);
        fs::write(
            dir.join(name).join("summary.json"),
            json!({
                "maximum_users": 1, "total_users": 1,
                "started_at": start, "finished_at": start + chrono::TimeDelta::seconds(10),
                "hold_started_at": start, "hold_finished_at": start + chrono::TimeDelta::seconds(5),
                "planned_hold_s": 5, "hosts": ["http://127.0.0.1:3000"], "source_ips": [],
                "runtime_errors": 0, "completed": true, "aggregate": summary,
            })
            .to_string(),
        )
        .unwrap();
        serde_json::to_writer(
            File::create(dir.join(name).join("samples.json")).unwrap(),
            &raw,
        )
        .unwrap();
        (
            dir,
            Worker {
                name: name.into(),
                users: 1,
                user_offset: 0,
                remote_dir: Some("/tmp/fixture".into()),
                exit_code: Some(1),
                error: None,
            },
        )
    }

    #[test]
    fn combined_percentiles_weight_samples_and_require_overlapping_hold() {
        let (dir, a) = fixture("weighted-a", vec![10.0; 99], 0);
        let (other, b) = fixture("weighted-b", vec![1000.0], 0);
        fs::rename(other.join(&b.name), dir.join(&b.name)).unwrap();
        let mut workers = vec![a, b];
        let result = collect(&dir, &mut workers);
        assert_eq!(result["passed"], true); // the slow host's own p99 fails, combined p99 passes
        assert!((result["aggregate"]["p99_ms"].as_f64().unwrap() - 19.9).abs() < 1e-9);
        assert_eq!(result["aggregate"]["requests"], 100);
        assert_eq!(result["duration_s"], 10.0);
        assert_eq!(result["common_hold_s"], 5.0);
        let (late, c) = fixture("weighted-c", vec![10.0], 20);
        fs::rename(late.join(&c.name), dir.join(&c.name)).unwrap();
        workers.push(c);
        let result = collect(&dir, &mut workers);
        assert_eq!(result["passed"], false);
        assert_eq!(result["completed"], false);
        assert_eq!(result["duration_s"], 30.0);
        fs::remove_dir_all(dir).unwrap();
        fs::remove_dir(other).unwrap();
        fs::remove_dir(late).unwrap();
    }

    #[test]
    fn missing_or_inconsistent_worker_data_cannot_pass() {
        let (dir, mut worker) = fixture("invalid", vec![10.0], 0);
        worker.users = 2;
        let result = collect(&dir, std::slice::from_mut(&mut worker));
        assert_eq!(result["passed"], false);
        assert!(worker.error.is_some());
        worker.users = 1;
        worker.error = None;
        fs::remove_file(dir.join(&worker.name).join("samples.json")).unwrap();
        let result = collect(&dir, std::slice::from_mut(&mut worker));
        assert_eq!(result["completed"], false);
        assert!(worker.error.is_some());
        fs::remove_dir_all(dir).unwrap();
    }
    #[test]
    fn total_users_split_evenly_with_disjoint_token_offsets() {
        assert_eq!(split_users(10, 3).unwrap(), [(4, 0), (3, 4), (3, 7)]);
        assert!(split_users(2, 3).is_err());
        assert!(split_users(100, 0).is_err());
    }
    #[test]
    fn remote_arguments_are_posix_quoted_and_ipv6_scp_is_bracketed() {
        assert_eq!(quote("a'b $(id)"), "'a'\\''b $(id)'");
        assert_eq!(
            destination("user", "::1", "/tmp/run"),
            "user@[::1]:/tmp/run"
        );
    }

    #[test]
    fn stderr_inherited_by_descendants_does_not_block_command_completion() {
        let started = Instant::now();
        checked(
            Command::new("sh").args(["-c", "sleep 5 >&2 &"]),
            &AtomicBool::new(false),
        )
        .unwrap();
        assert!(started.elapsed() < Duration::from_secs(3));
    }

    #[test]
    fn cancelled_control_children_are_stopped_without_generator_grace() {
        let mut child = Command::new("sh")
            .args(["-c", "exec sleep 30"])
            .spawn()
            .unwrap();
        let started = Instant::now();
        let status = wait_child(
            &mut child,
            &AtomicBool::new(true),
            Some(Duration::from_secs(120)),
        )
        .unwrap();
        assert!(!status.success());
        assert!(started.elapsed() < Duration::from_secs(1));
    }
}
