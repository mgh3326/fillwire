"""Check the deployment copies and runbook against the built CLI."""

import os
import re
import shlex
import subprocess
import sys
import tempfile
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
binary = sys.argv[1]
runbook = (ROOT / "docs/serve-cutover.md").read_text()
for command, unit in [
    ("serve", "fillwire-serve"),
    ("market-tap", "quote-collector"),
]:
    help_run = subprocess.run(
        [binary, command, "--help"], capture_output=True, text=True, timeout=5
    )
    assert help_run.returncode == 0, help_run
    flags = {
        name: kind
        for name, kind in re.findall(
            r"(?m)^[ \t]+(-[\w-]+)(?: ([\w]+))?[ \t]*$",
            help_run.stdout + help_run.stderr,
        )
    }
    assert flags, help_run

    example = (ROOT / f"deploy/systemd/{unit}.service.example").read_text()
    start = next(line for line in example.splitlines() if line.startswith("ExecStart="))
    deployed = shlex.split(start.split("${IMAGE} ", 1)[1])
    assert deployed[0] == command, deployed

    documented = re.findall(rf"(?m)^fillwire {command} (.+)$", runbook)
    assert len(documented) == 1, documented
    documented = shlex.split(documented[0])
    assert documented[: len(deployed) - 1] == deployed[1:], (documented, deployed)

    args = iter(documented)
    for flag in args:
        assert flag in flags, (command, flag, flags)
        if flags[flag]:
            value = next(args, "")
            assert value and not value.startswith("-"), (flag, value)
    print(f"fillwire {command} --help matches the unit and runbook")

cutover = runbook.split("## 기존 유닛 인계 순서\n", 1)[1].split("\n## ", 1)[0]
rollback = runbook.split("## 롤백\n", 1)[1]
assert "sudo systemctl enable fillwire-serve.service quote-collector.service\n" in cutover
stop_collector = rollback.index("sudo systemctl disable --now quote-collector.service\n")
stop_serve = rollback.index("sudo systemctl disable --now fillwire-serve.service\n")
assert stop_collector < stop_serve
for unit in ("fillwire-rec.service", "fillwire.service", "broker-edge.service", "gatewayd.service"):
    assert f"sudo systemctl disable {unit}\n" in cutover, unit
assert 'cat "$state/boot-id" /proc/sys/kernel/random/boot_id' in cutover
assert 'systemctl is-active "$unit.service"' in cutover
assert "journalctl -b " in cutover

blocks = re.findall(r"```sh\n(.*?)```", runbook, re.S)
for block in blocks:
    subprocess.run(["bash", "-n"], input=block, text=True, check=True)
restore = next(block for block in blocks if 'test -f "$state/complete"' in block)
# Roll back during handoff, when boot flags can still be unchanged, or after
# disabling the units. Runtime state is independent of either boot state.
with tempfile.TemporaryDirectory() as home:
    state = Path(home) / "fillwire-cutover-state"
    state.mkdir()
    (state / "complete").touch()
    (state / "units").write_text(
        "gatewayd.service enabled active\n"
        "broker-edge.service enabled inactive\n"
        "fillwire.service disabled active\n"
        "fillwire-rec.service disabled inactive\n"
    )
    (state / "kis-container").write_text("at-kis-ws true\n")
    mock = r"""
sudo() { "$@"; }
systemctl() {
    local action=$1 unit=$2 file
    case "$action" in
        enable|disable|is-enabled) file="$HOME/$unit.boot" ;;
        start|stop|is-active) file="$HOME/$unit.runtime" ;;
        *) return 2 ;;
    esac
    case "$action" in
        enable) echo enabled > "$file" ;;
        disable) echo disabled > "$file" ;;
        start) echo active > "$file" ;;
        stop) echo inactive > "$file" ;;
        is-enabled) cat "$file"; test "$(cat "$file")" = enabled ;;
        is-active) cat "$file"; test "$(cat "$file")" = active ;;
    esac
}
docker() {
    case "$1" in
        start) test "$2" = at-kis-ws && echo true > "$HOME/kis-running" ;;
        container) test "$5" = at-kis-ws && cat "$HOME/kis-running" ;;
        logs) test "$4" = at-kis-ws ;;
        *) return 2 ;;
    esac
}
"""
    for boot_changed in (False, True):
        for line in (state / "units").read_text().splitlines():
            unit, boot, _ = line.split()
            (Path(home) / f"{unit}.boot").write_text(
                ("disabled" if boot_changed else boot) + "\n"
            )
            (Path(home) / f"{unit}.runtime").write_text("inactive\n")
        (Path(home) / "kis-running").write_text("false\n")
        subprocess.run(
            ["bash"], input=mock + restore, text=True, check=True,
            env={**os.environ, "HOME": home},
        )
print("cutover and rollback restore boot and runtime states independently")
