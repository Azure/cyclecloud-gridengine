import json
import os
import stat
import subprocess
import sys
from pathlib import Path
from typing import Callable, Dict

import pytest

PASSWORD = " synthetic-only\t'\"$` ;\\*?[]\nsecond line "
SCRIPT = Path(__file__).resolve().parents[3] / "generate_autoscale_json.sh"
ADAPTER = r"""
import json
import os
import shutil
import stat
import subprocess
import sys
from pathlib import Path

root = Path.cwd()
settings = json.loads((root / "settings.json").read_text())
command = Path(sys.argv[0]).name
assert "PASSWORD" not in os.environ
if command != "azge":
    assert "SCALELIB_PASSWORD" not in os.environ

def record(name, value):
    (root / (name + ".json")).write_text(json.dumps(value))

if command == "jetpack":
    assert sys.argv[1:] == ["config", "cyclecloud.config.password"]
    if settings["failure"] == "jetpack":
        sys.exit(44)
    sys.stdout.write(settings["password"] + "\n")
elif command == "qconf":
    if sys.argv[1:] == ["-sc"]:
        print("slots x\nslot_type x\nnodearray x\nm_mem_free x\nexclusive x")
elif command == "date":
    print("1700000000")
elif command == "mktemp":
    mask = os.umask(0o077)
    os.umask(mask)
    assert mask == 0o077
    assert sys.argv[1:] == [".autoscale.json.XXXXXX"]
    record("allocation", {"umask": mask, "argv": sys.argv[1:]})
    if settings["failure"] == "mktemp":
        sys.exit(43)
    result = subprocess.run(
        ["/usr/bin/mktemp"] + sys.argv[1:], capture_output=True, text=True
    )
    if result.returncode:
        sys.exit(result.returncode)
    allocated = Path(result.stdout.strip()).lstat()
    assert stat.S_ISREG(allocated.st_mode)
    assert stat.S_IMODE(allocated.st_mode) == 0o600
    record("allocation", {"umask": mask, "path": result.stdout.strip()})
    sys.stdout.write(result.stdout)
elif command == "azge":
    record("invocation", {"argv": sys.argv[1:]})
    class CheckedWriter:
        def write(self, text):
            if not (root / "child.json").exists():
                assert os.environ["SCALELIB_PASSWORD"] == settings["expected"]
                assert not settings["password"] or all(settings["password"] not in arg for arg in sys.argv)
                assert not any(arg.startswith("--password") for arg in sys.argv)
                descriptor = os.fstat(sys.stdout.fileno())
                assert stat.S_ISREG(descriptor.st_mode)
                assert stat.S_IMODE(descriptor.st_mode) == 0o600
                allocation = json.loads((root / "allocation.json").read_text())
                assert os.path.samestat(descriptor, Path(allocation["path"]).stat())
                record("child", {"argv": sys.argv[1:], "mode": 0o600})
            return sys.stdout.write(text)

    writer = CheckedWriter()
    if settings["failure"] == "generation":
        writer.write('{"partial":')
        sys.exit(41)
    from gridengine import cli
    cli.initconfig.__defaults__ = (writer,)
    cli.main()
elif command == "install":
    assert "SCALELIB_PASSWORD" not in os.environ
    assert sys.argv[1:7] == ["-m", "600", "-o", "cyclecloud", "-g", "cyclecloud"]
    assert len(sys.argv) == 9
    record("install", {"argv": sys.argv[1:]})
    if settings["failure"] == "install":
        print("synthetic install failure", file=sys.stderr)
        sys.exit(42)
    source, destination = sys.argv[7:]
    with open(source, "rb") as reader, open(destination, "wb") as writer:
        os.fchmod(writer.fileno(), 0o600)
        shutil.copyfileobj(reader, writer)
else:
    raise AssertionError(command)
"""


@pytest.fixture
def generator(tmp_path: Path) -> Callable[..., Dict]:
    workdir = tmp_path / "trusted workdir"
    workdir.mkdir(mode=0o700)
    bindir = tmp_path / "bin"
    bindir.mkdir()
    installdir = tmp_path / "install dir"
    installdir.mkdir()
    for command in ["jetpack", "qconf", "date", "mktemp", "azge", "install"]:
        adapter = bindir / command
        adapter.write_text("#!{}\n{}".format(sys.executable, ADAPTER))
        adapter.chmod(0o700)

    def run(
        explicit: bool = False,
        failure: str = "",
        source: str = "jetpack",
        password: str = PASSWORD,
    ) -> Dict:
        destination = (
            tmp_path / "explicit output.json"
            if explicit
            else installdir / "autoscale.json"
        )
        destination.write_text("destination sentinel")
        legacy = workdir / ".autoscale.json.1700000000"
        legacy.write_text("legacy sentinel")
        legacy.chmod(0o644)
        referent = workdir / "referent"
        referent.write_text("symlink sentinel")
        decoy = workdir / ".autoscale.json.decoy"
        decoy.symlink_to(referent)
        (workdir / "settings.json").write_text(
            json.dumps(
                {
                    "password": password,
                    "failure": failure,
                    "expected": (
                        password.rstrip("\n") if source == "jetpack" else password
                    ),
                }
            )
        )
        args = [
            "bash",
            "-x",
            str(SCRIPT),
            "--cluster-name",
            "synthetic-cluster",
            "--username",
            "synthetic-user",
            "--url",
            "https://cyclecloud.invalid",
            "--install-dir",
            str(installdir),
        ]
        if explicit:
            args += ["--output", str(destination)]
        environment = {
            "PATH": str(bindir) + os.pathsep + "/usr/bin:/bin",
            "PASSWORD": "inherited-export-must-not-leak",
            "HOME": str(tmp_path),
            "PYTHONPATH": os.pathsep.join(
                str(Path(entry).resolve()) for entry in sys.path
            ),
            "PYTHONDONTWRITEBYTECODE": "1",
        }
        if source == "environment":
            environment["SCALELIB_PASSWORD"] = password
        elif source == "explicit":
            environment["SCALELIB_PASSWORD"] = "must-not-win"
            args += ["--password", password]
        result = subprocess.run(
            args,
            cwd=workdir,
            env=environment,
            capture_output=True,
            text=True,
            umask=0o022,
            timeout=30,
        )
        assert "synthetic-only" not in result.stdout + result.stderr
        assert legacy.read_text() == "legacy sentinel"
        assert stat.S_IMODE(legacy.stat().st_mode) == 0o644
        assert decoy.is_symlink()
        assert decoy.resolve() == referent
        assert referent.read_text() == "symlink sentinel"
        assert set(workdir.glob(".autoscale.json.*")) == {legacy, decoy}
        return {
            "result": result,
            "workdir": workdir,
            "destination": destination,
            "installdir": installdir,
        }

    return run


@pytest.mark.parametrize("source", ["jetpack", "environment", "explicit"])
@pytest.mark.parametrize("password", ["", PASSWORD + "\n\n"])
def test_generator_password_sources(
    generator: Callable[..., Dict], source: str, password: str
) -> None:
    run = generator(source=source, password=password)
    assert run["result"].returncode == 0, run["result"].stderr
    expected = password.rstrip("\n") if source == "jetpack" else password
    assert json.loads(run["destination"].read_text())["password"] == expected
    assert stat.S_IMODE(run["destination"].stat().st_mode) == 0o600


@pytest.mark.parametrize("explicit", [False, True])
def test_generator_keeps_credentials_private_through_install(
    generator: Callable[..., Dict], explicit: bool
) -> None:
    run = generator(explicit=explicit)
    assert run["result"].returncode == 0, run["result"].stderr
    workdir = run["workdir"]
    child = json.loads((workdir / "child.json").read_text())
    assert child["mode"] == 0o600
    assert child["argv"][0] == "initconfig"
    allocation = json.loads((workdir / "allocation.json").read_text())
    assert allocation["umask"] == 0o077
    assert not Path(allocation["path"]).is_absolute()
    assert not (workdir / allocation["path"]).exists()
    install = json.loads((workdir / "install.json").read_text())
    assert install["argv"] == [
        "-m",
        "600",
        "-o",
        "cyclecloud",
        "-g",
        "cyclecloud",
        allocation["path"],
        str(run["destination"]),
    ]
    assert stat.S_IMODE(run["destination"].stat().st_mode) == 0o600
    assert json.loads(run["destination"].read_text()) == {
        "cluster_name": "synthetic-cluster",
        "username": "synthetic-user",
        "password": PASSWORD,
        "url": "https://cyclecloud.invalid",
        "lock_file": str(run["installdir"] / "scalelib.lock"),
        "logging": {"config_file": str(run["installdir"] / "logging.conf")},
        "idle_timeout": 300,
        "boot_timeout": 1800,
        "default_resources": [
            {"select": {}, "name": "slots", "value": "node.vcpu_count"},
            {"select": {}, "name": "slot_type", "value": "node.nodearray"},
            {"select": {}, "name": "nodearray", "value": "node.nodearray"},
            {
                "select": {},
                "name": "m_mem_free",
                "value": "node.resources.memgb",
                "subtract": "1g",
            },
            {"select": {}, "name": "mfree", "value": "node.resources.m_mem_free"},
            {"select": {}, "name": "exclusive", "value": "true"},
        ],
        "gridengine": {
            "relevant_complexes": [
                "slots",
                "slot_type",
                "nodearray",
                "m_mem_free",
                "exclusive",
            ],
            "pes": {"make": {"requires_placement_groups": False}},
            "hostgroups": {
                "@cyclempi": {"constraints": {"node.colocated": True}},
                "@cyclehtc": {"constraints": {"node.colocated": False}},
            },
            "default_hostgroups": [
                {"select": {"node.colocated": True}, "hostgroups": ["@cyclempi"]},
                {"select": {"node.colocated": False}, "hostgroups": ["@cyclehtc"]},
            ],
        },
    }


@pytest.mark.parametrize(
    "failure,exit_code,generated,installed",
    [
        ("generation", 41, True, False),
        ("install", 42, True, True),
        ("mktemp", 43, False, False),
        ("jetpack", 44, False, False),
    ],
)
def test_generator_propagates_failure_and_cleans_only_its_intermediate(
    generator: Callable[..., Dict],
    failure: str,
    exit_code: int,
    generated: bool,
    installed: bool,
) -> None:
    run = generator(failure=failure)
    assert run["result"].returncode == exit_code, run["result"].stderr
    assert (run["workdir"] / "invocation.json").exists() == generated
    assert (run["workdir"] / "child.json").exists() == generated
    assert (run["workdir"] / "install.json").exists() == installed
    if not installed:
        assert run["destination"].read_text() == "destination sentinel"
