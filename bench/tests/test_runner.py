"""Exercise the runner with local tool stand-ins; no downloads or load traffic."""
import json
import os
from pathlib import Path
import shlex
import shutil
import subprocess
import sys
import tempfile
import unittest

RUNNER = Path(__file__).resolve().parents[1] / "run.sh"
DRIVER = r'''
import json, os, pathlib, shlex, sys
args = sys.argv[1:]
name = pathlib.Path(sys.argv[0]).name
root = pathlib.Path(os.environ["FIXTURE"])
with (root / "calls").open("a") as log:
    log.write(json.dumps([name, *args]) + "\n")
if args and args[0] == "--install-rustup":
    bindir = pathlib.Path(os.environ["CARGO_HOME"]) / "bin"
    bindir.mkdir(parents=True, exist_ok=True)
    (bindir / "rustup").symlink_to(root / "driver")
elif args and args[0] == "--run-load":
    (root / "result").write_text(json.dumps({
        "cwd": os.getcwd(), "host": os.environ.get("BASE_URL"),
        "tokens": os.environ.get("TOKENS"), "summary": os.environ.get("SUMMARY"),
        "source_ips": os.environ.get("SOURCE_IPS"), "args": args[1:],
    }))
    sys.exit(7)
elif name == "git":
    print(root / "main" / ".git")
elif name == "curl":
    print("exec " + shlex.join([sys.executable, str(root / "driver"), "--install-rustup"]))
elif name == "rustc" or args == ["--version"]:
    version = "1.99.0" if os.environ["MODE"] == "native" else "1.98.0"
    print(name + " " + version + " (fixture)")
    if args == ["-vV"]:
        print("host: fixture-host")
else:
    if name == "rustup":
        if args[:2] == ["toolchain", "install"]:
            (root / "installed").touch()
            sys.exit(0)
        assert args[:2] == ["run", "1.99.0"], args
        if args[2] == "rustc":
            if args[3:] == ["-vV"]:
                print("rustc 1.99.0 (fixture)\nhost: fixture-host")
            sys.exit(0 if (root / "installed").exists() else 1)
        assert args[2] == "cargo", args
        args = args[3:]
    assert args[0] == "build", args
    assert "--locked" in args and "--release" in args
    assert pathlib.Path(args[args.index("--manifest-path") + 1]).is_file()
    assert args[args.index("--target") + 1] == "fixture-host", args
    target = pathlib.Path(args[args.index("--target-dir") + 1]) / "fixture-host" / "release"
    target.mkdir(parents=True, exist_ok=True)
    binary = target / "feed-load"
    binary.write_text("#!" + os.environ["BASH"] + "\nexec " +
        shlex.join([sys.executable, str(root / "driver"), "--run-load"]) + ' "$@"\n')
    binary.chmod(0o755)
'''


class RunnerTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory(prefix="goose runner ")
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.bench = self.root / "checkout" / "bench"
        self.bench.mkdir(parents=True)
        shutil.copyfile(RUNNER, self.bench / "run.sh")
        (self.bench / "Cargo.toml").touch()
        (self.root / "other").mkdir()
        (self.root / "main" / "seed").mkdir(parents=True)
        (self.root / "main" / ".git").mkdir()
        (self.root / "main" / "seed" / "tokens.json").write_text("[]")
        driver = self.root / "driver"
        driver.write_text("#!" + sys.executable + "\n" + DRIVER)
        driver.chmod(0o755)
        bindir = self.root / "bin"
        bindir.mkdir()
        for tool in ["bash", "sh", "dirname"]:
            (bindir / tool).symlink_to(shutil.which(tool))
        for tool in ["cargo", "rustc", "git", "curl"]:
            (bindir / tool).symlink_to(driver)
        self.env = dict(os.environ, PATH=str(bindir), HOME=str(self.root),
                        CARGO_HOME=str(self.root / "cargo home"), MODE="native",
                        FIXTURE=str(self.root), BASH=shutil.which("bash"),
                        CARGO_BUILD_TARGET="other-target")
        for name in ["TOKENS", "SUMMARY", "BASE_URL", "SOURCE_IPS"]:
            self.env.pop(name, None)

    def run_runner(self, cwd, *args):
        return subprocess.run([self.env["BASH"], str(self.bench / "run.sh"), *args],
                              cwd=cwd, env=self.env, capture_output=True, text=True)

    def calls(self):
        return [json.loads(line) for line in (self.root / "calls").read_text().splitlines()]

    def test_target_paths_flags_and_exit_status_from_any_directory(self):
        stale = self.bench / "target" / "release" / "feed-load"
        stale.parent.mkdir(parents=True)
        stale.write_text("#!" + self.env["BASH"] + "\nexit 99\n")
        stale.chmod(0o755)
        for cwd, target, host in [
            (self.bench.parent, ["127.0.0.1", "3000"], "http://127.0.0.1:3000"),
            (self.bench, ["::1", "8080"], "http://[::1]:8080"),
            (self.bench, ["[::1]", "8080"], "http://[::1]:8080"),
            (self.root / "other", [], "http://example.test:9000"),
        ]:
            with self.subTest(cwd=cwd):
                self.env.update(BASE_URL="http://example.test:9000", SOURCE_IPS="127.0.0.[1-5]",
                                SUMMARY="../results/custom.json")
                run = self.run_runner(cwd, *target, "--request-log", "file with spaces")
                self.assertEqual(run.returncode, 7, run.stderr)
                result = json.loads((self.root / "result").read_text())
                self.assertEqual(result["host"], host)
                self.assertEqual(result["cwd"], str(self.bench))
                self.assertEqual(Path(result["tokens"]).resolve(), self.root / "main/seed/tokens.json")
                self.assertEqual(result["summary"], "../results/custom.json")
                self.assertEqual(result["source_ips"], "127.0.0.[1-5]")
                self.assertEqual(result["args"], ["--request-log", "file with spaces"])
        self.assertFalse(any(call[0] in ["curl", "rustup"] for call in self.calls()))

    def test_rustup_bootstrap_and_pinned_toolchain_reuse(self):
        self.env.update(MODE="old", TOKENS="../custom-tokens.json")
        run = self.run_runner(self.bench.parent, "127.0.0.1", "3000")
        self.assertEqual(run.returncode, 7, run.stderr)
        calls = self.calls()
        self.assertTrue(any(call[0] == "curl" for call in calls))
        self.assertIn(["rustup", "toolchain", "install", "1.99.0", "--profile", "minimal"], calls)
        self.assertTrue(any(call[:4] == ["rustup", "run", "1.99.0", "cargo"] for call in calls))
        self.assertEqual(json.loads((self.root / "result").read_text())["tokens"], "../custom-tokens.json")
        (self.root / "calls").unlink()
        run = self.run_runner(self.bench, "127.0.0.1", "3000")
        self.assertEqual(run.returncode, 7, run.stderr)
        self.assertFalse(any(call[0] == "curl" or call[1:3] == ["toolchain", "install"]
                             for call in self.calls()))

    def test_invalid_target_stops_before_building(self):
        for args in [["127.0.0.1"], ["127.0.0.1", "0"], ["127.0.0.1", "65536"],
                     ["127.0.0.1", "not-a-port"], ["127.0.0.1/path", "3000"],
                     ["127.0.0.1@192.0.2.1", "3000"], ["127.0.0.1:80", "3000"],
                     ["127.0.0.999", "3000"], ["010.0.0.1", "3000"], ["::1/path", "3000"]]:
            with self.subTest(args=args):
                run = self.run_runner(self.bench.parent, *args)
                self.assertEqual(run.returncode, 2, run.stderr)
                self.assertIn("Usage:", run.stderr)
        self.assertFalse((self.root / "calls").exists())


if __name__ == "__main__":
    unittest.main()
