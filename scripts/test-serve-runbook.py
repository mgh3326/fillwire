"""Check the deployment copies and runbook against the built CLI."""

import re
import shlex
import subprocess
import sys
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
