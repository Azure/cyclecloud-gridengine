import importlib.util
import io
import shutil
import subprocess
import tarfile
import zipfile
from argparse import Namespace
from pathlib import Path

import pytest


spec = importlib.util.spec_from_file_location("gridengine_package", Path(__file__).resolve().parents[1] / "package.py")
package = importlib.util.module_from_spec(spec)
spec.loader.exec_module(package)


@pytest.fixture
def deb_factory(tmp_path):
    if not shutil.which("dpkg-deb"):
        pytest.skip("dpkg-deb is required for archive integration tests")

    def build(version="8.9.3", copies=1, symlink=False):
        root = tmp_path / "root"
        control = root / "DEBIAN"
        control.mkdir(parents=True)
        (control / "control").write_text("Package: cyclecloud8\nVersion: 8.9.3-3874\nArchitecture: amd64\nMaintainer: Test <test@example.invalid>\nDescription: Synthetic wheel fixture\n")
        buffer = io.BytesIO()
        with zipfile.ZipFile(buffer, "w") as wheel:
            wheel.writestr("cyclecloud_api-8.9.3.dist-info/METADATA", "Metadata-Version: 2.1\nName: cyclecloud-api\nVersion: {}\n".format(version))
        for index in range(copies):
            directory = root / "opt/cycle_server/.installer" / str(index) / "tools"
            directory.mkdir(parents=True)
            target = directory / "cyclecloud_api-8.9.3-py2.py3-none-any.whl"
            if symlink:
                target.symlink_to("/untrusted-target")
            else:
                target.write_bytes(buffer.getvalue())
        deb = tmp_path / "fixture.deb"
        subprocess.check_call(["dpkg-deb", "--build", str(root), str(deb)], stdout=subprocess.DEVNULL)
        return deb, buffer.getvalue()

    return build


def mock_download(monkeypatch, deb):
    def download(command):
        assert command[0] == "curl"
        assert "--fail" in command and "-k" not in command
        assert command[-1] == "https://packages.microsoft.com/repos/cyclecloud/pool/main/c/cyclecloud8/cyclecloud8_8.9.3-3874_amd64.deb"
        shutil.copyfile(deb, command[command.index("--output") + 1])

    monkeypatch.setattr(package, "check_call", download)


def test_extracts_verified_wheel(monkeypatch, tmp_path, deb_factory):
    deb, expected = deb_factory()
    mock_download(monkeypatch, deb)
    destination = tmp_path / "api.whl"
    package.download_cyclecloud_api(str(destination))
    assert destination.read_bytes() == expected
    assert sorted(entry.name for entry in tmp_path.iterdir()) == ["api.whl", "fixture.deb", "root"]


@pytest.mark.parametrize("settings", [{"copies": 0}, {"copies": 2}, {"version": "8.0.1"}, {"symlink": True}])
def test_rejects_invalid_wheel_without_replacing_destination(monkeypatch, tmp_path, deb_factory, settings):
    deb, _ = deb_factory(**settings)
    mock_download(monkeypatch, deb)
    destination = tmp_path / "api.whl"
    destination.write_bytes(b"original")
    with pytest.raises(RuntimeError):
        package.download_cyclecloud_api(str(destination))
    assert destination.read_bytes() == b"original"
    assert sorted(entry.name for entry in tmp_path.iterdir()) == ["api.whl", "fixture.deb", "root"]


def test_download_failure_preserves_destination(monkeypatch, tmp_path):
    monkeypatch.setattr(package.shutil, "which", lambda name: "/usr/bin/dpkg-deb")

    def fail(command):
        raise subprocess.CalledProcessError(22, command)
    monkeypatch.setattr(package, "check_call", fail)
    destination = tmp_path / "api.whl"
    destination.write_bytes(b"original")
    with pytest.raises(subprocess.CalledProcessError):
        package.download_cyclecloud_api(str(destination))
    assert destination.read_bytes() == b"original"
    assert list(tmp_path.iterdir()) == [destination]


def test_local_overrides_do_not_require_deb_tools(monkeypatch, tmp_path):
    monkeypatch.chdir(tmp_path)
    (tmp_path / "libs").mkdir()
    scalelib = tmp_path / "scalelib.tar.gz"
    wheel = tmp_path / "local-api.whl"
    scalelib.write_bytes(b"local scalelib")
    wheel.write_bytes(b"local wheel")
    monkeypatch.setattr(package, "build_sdist", lambda: "gridengine.tar.gz")
    monkeypatch.setattr(package.shutil, "which", lambda name: None)

    def unexpected(command):
        pytest.fail("Unexpected download")
    monkeypatch.setattr(package, "check_call", unexpected)
    result = package.get_cycle_libs(Namespace(scalelib=str(scalelib), cyclecloud_api=str(wheel)))
    assert result == ["gridengine.tar.gz", scalelib.name, wheel.name]
    assert (tmp_path / "libs" / wheel.name).read_bytes() == b"local wheel"


def test_missing_deb_tool_reports_override(monkeypatch, tmp_path):
    monkeypatch.setattr(package.shutil, "which", lambda name: None)
    with pytest.raises(RuntimeError, match="--cyclecloud-api"):
        package.download_cyclecloud_api(str(tmp_path / "api.whl"))


def test_corrupt_deb_preserves_destination(monkeypatch, tmp_path):
    if not shutil.which("dpkg-deb"):
        pytest.skip("dpkg-deb is required for archive integration tests")
    deb = tmp_path / "corrupt.deb"
    deb.write_bytes(b"not a deb")
    mock_download(monkeypatch, deb)
    destination = tmp_path / "api.whl"
    destination.write_bytes(b"original")
    with pytest.raises((RuntimeError, tarfile.ReadError)):
        package.download_cyclecloud_api(str(destination))
    assert destination.read_bytes() == b"original"
    assert sorted(entry.name for entry in tmp_path.iterdir()) == ["api.whl", "corrupt.deb"]


def test_default_api_uses_deb_extraction(monkeypatch, tmp_path, deb_factory):
    deb, expected = deb_factory()
    mock_download(monkeypatch, deb)
    monkeypatch.chdir(tmp_path)
    (tmp_path / "libs").mkdir()
    scalelib = tmp_path / "local-scalelib.tar.gz"
    scalelib.write_bytes(b"local scalelib")
    monkeypatch.setattr(package, "build_sdist", lambda: "gridengine.tar.gz")
    result = package.get_cycle_libs(Namespace(scalelib=str(scalelib), cyclecloud_api=None))
    wheel_name = "cyclecloud_api-8.9.3-py2.py3-none-any.whl"
    assert result == ["gridengine.tar.gz", scalelib.name, wheel_name]
    assert (tmp_path / "libs" / wheel_name).read_bytes() == expected
