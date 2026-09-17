import importlib.util
import io
import json
import subprocess
import zipfile
from argparse import Namespace
from pathlib import Path

import pytest


spec = importlib.util.spec_from_file_location("gridengine_package", Path(__file__).resolve().parents[1] / "package.py")
package = importlib.util.module_from_spec(spec)
spec.loader.exec_module(package)


def mock_release(monkeypatch, version="9.1.2", copies=1, metadata_version=None,
                 metadata_name="cyclecloud-api", corrupt=False, metadata_count=1):
    wheel_name = "cyclecloud_api-{}-py2.py3-none-any.whl".format(version)
    url = "https://github.com/Azure/cyclecloud-scalelib/releases/download/{}/{}".format(
        package.SCALELIB_VERSION, wheel_name
    )
    buffer = io.BytesIO()
    with zipfile.ZipFile(buffer, "w") as wheel:
        for index in range(metadata_count):
            wheel.writestr("api-{}.dist-info/METADATA".format(index),
                          "Metadata-Version: 2.1\nName: {}\nVersion: {}\n".format(
                              metadata_name, metadata_version or version))
    content = b"not a wheel" if corrupt else buffer.getvalue()

    def release(command):
        assert command == [
            "curl", "--fail", "--location", "--silent", "--show-error",
            "https://api.github.com/repos/Azure/cyclecloud-scalelib/releases/tags/{}".format(
                package.SCALELIB_VERSION),
        ]
        return json.dumps({"assets": [
            {"name": "cyclecloud-scalelib-pkg.tar.gz", "browser_download_url": "unused"},
        ] + [{"name": wheel_name, "browser_download_url": url}] * copies}).encode()

    def download(command):
        assert command[0] == "curl"
        assert "--fail" in command and "-k" not in command
        assert command[-1] == url
        Path(command[command.index("--output") + 1]).write_bytes(content)

    monkeypatch.setattr(package, "check_output", release)
    monkeypatch.setattr(package, "check_call", download)
    return wheel_name, content


@pytest.mark.parametrize("scalelib_version,api_version", [("1.0.12", "8.9.3"), ("1.0.13", "9.1.2")])
def test_downloads_wheel_from_selected_release(monkeypatch, tmp_path, scalelib_version, api_version):
    monkeypatch.setattr(package, "SCALELIB_VERSION", scalelib_version)
    wheel_name, expected = mock_release(monkeypatch, version=api_version)
    assert package.download_cyclecloud_api(str(tmp_path)) == wheel_name
    assert (tmp_path / wheel_name).read_bytes() == expected
    assert list(tmp_path.iterdir()) == [tmp_path / wheel_name]


@pytest.mark.parametrize("settings", [
    {"copies": 0}, {"copies": 2}, {"metadata_version": "8.0.1"},
    {"metadata_name": "unrelated"}, {"metadata_count": 0}, {"metadata_count": 2},
    {"corrupt": True},
])
def test_invalid_release_or_wheel_preserves_destination(monkeypatch, tmp_path, settings):
    wheel_name, _ = mock_release(monkeypatch, **settings)
    destination = tmp_path / wheel_name
    destination.write_bytes(b"original")
    with pytest.raises((RuntimeError, zipfile.BadZipFile)):
        package.download_cyclecloud_api(str(tmp_path))
    assert destination.read_bytes() == b"original"
    assert list(tmp_path.iterdir()) == [destination]


@pytest.mark.parametrize("stage", ["check_output", "check_call"])
def test_download_failure_preserves_destination(monkeypatch, tmp_path, stage):
    wheel_name, _ = mock_release(monkeypatch)

    def fail(command):
        raise subprocess.CalledProcessError(22, command)

    monkeypatch.setattr(package, stage, fail)
    destination = tmp_path / wheel_name
    destination.write_bytes(b"original")
    with pytest.raises(subprocess.CalledProcessError):
        package.download_cyclecloud_api(str(tmp_path))
    assert destination.read_bytes() == b"original"
    assert list(tmp_path.iterdir()) == [destination]


def test_local_overrides_do_not_require_downloads(monkeypatch, tmp_path):
    monkeypatch.chdir(tmp_path)
    (tmp_path / "libs").mkdir()
    scalelib = tmp_path / "scalelib.tar.gz"
    wheel = tmp_path / "local-api.whl"
    scalelib.write_bytes(b"local scalelib")
    wheel.write_bytes(b"local wheel")
    monkeypatch.setattr(package, "build_sdist", lambda: "gridengine.tar.gz")

    def unexpected(command):
        pytest.fail("Unexpected download")

    monkeypatch.setattr(package, "check_call", unexpected)
    monkeypatch.setattr(package, "check_output", unexpected)
    result = package.get_cycle_libs(Namespace(scalelib=str(scalelib), cyclecloud_api=str(wheel)))
    assert result == ["gridengine.tar.gz", scalelib.name, wheel.name]
    assert (tmp_path / "libs" / wheel.name).read_bytes() == b"local wheel"


def test_default_api_uses_discovered_filename(monkeypatch, tmp_path):
    wheel_name, expected = mock_release(monkeypatch)
    monkeypatch.chdir(tmp_path)
    (tmp_path / "libs").mkdir()
    scalelib = tmp_path / "local-scalelib.tar.gz"
    scalelib.write_bytes(b"local scalelib")
    monkeypatch.setattr(package, "build_sdist", lambda: "gridengine.tar.gz")
    result = package.get_cycle_libs(Namespace(scalelib=str(scalelib), cyclecloud_api=None))
    assert result == ["gridengine.tar.gz", scalelib.name, wheel_name]
    assert (tmp_path / "libs" / wheel_name).read_bytes() == expected
