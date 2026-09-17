import argparse
import configparser
import glob
import json
import os
import re
import shutil
import sys
import tarfile
import tempfile
import zipfile
from argparse import Namespace
from email.parser import BytesParser
from subprocess import check_call, check_output
from typing import List, Optional

SCALELIB_VERSION = "1.0.12"


def download_cyclecloud_api(destination_dir: str) -> str:
    release_url = "https://api.github.com/repos/Azure/cyclecloud-scalelib/releases/tags/{}".format(
        SCALELIB_VERSION
    )
    release = json.loads(check_output([
        "curl", "--fail", "--location", "--silent", "--show-error", release_url,
    ]))
    assets = [
        asset for asset in release["assets"]
        if re.fullmatch(r"cyclecloud_api-([^-]+)-[^-]+-[^-]+-[^-]+\.whl", asset["name"])
        and "/" not in asset["name"] and "\\" not in asset["name"]
    ]
    if len(assets) != 1:
        raise RuntimeError("Expected one API wheel in scalelib release {}.".format(SCALELIB_VERSION))
    asset = assets[0]
    wheel_name = asset["name"]
    api_version = wheel_name.split("-")[1]
    destination = os.path.join(destination_dir, wheel_name)
    with tempfile.TemporaryDirectory(
        dir=os.path.abspath(destination_dir)
    ) as work_dir:
        wheel_path = os.path.join(work_dir, wheel_name)
        check_call(
            [
                "curl", "--fail", "--location", "--silent", "--show-error",
                "--output", wheel_path, asset["browser_download_url"],
            ]
        )
        with zipfile.ZipFile(wheel_path) as wheel:
            metadata_files = [
                name for name in wheel.namelist()
                if name.endswith(".dist-info/METADATA")
            ]
            if len(metadata_files) != 1:
                raise RuntimeError("Expected one API wheel metadata file.")
            metadata = BytesParser().parsebytes(wheel.read(metadata_files[0]))
            if (
                metadata["Name"] not in ("cyclecloud-api", "cyclecloud_api")
                or metadata["Version"] != api_version
            ):
                raise RuntimeError("API wheel metadata does not match the release asset.")
        os.replace(wheel_path, destination)
    return wheel_name


def build_sdist() -> str:
    cmd = [sys.executable, "setup.py", "sdist"]
    check_call(cmd, cwd=os.path.abspath("gridengine"))
    # see below for more: cyclecloud*gridengine so we cover cyclecloud-gridengine and cyclecloud_gridengine
    sdists = glob.glob("gridengine/dist/cyclecloud*gridengine-*.tar.gz")
    assert len(sdists) == 1, "Found %d sdist packages, expected 1" % len(sdists)
    path = sdists[0]
    # at some point setuptools changed the name of the sdist package to use underscores instead of dashes.
    if "/cyclecloud_gridengine-" in path:
        fixed_path = path.replace("/cyclecloud_gridengine-", "/cyclecloud-gridengine-")
        os.rename(path, fixed_path)
        path = fixed_path
    fname = os.path.basename(path)
    dest = os.path.join("libs", fname)
    if os.path.exists(dest):
        os.remove(dest)
    shutil.move(path, dest)
    return fname


def get_cycle_libs(args: Namespace) -> List[str]:
    ret = [build_sdist()]

    scalelib_file = "cyclecloud-scalelib-{}.tar.gz".format(SCALELIB_VERSION)

    scalelib_url = "https://github.com/Azure/cyclecloud-scalelib/archive/{}.tar.gz".format(
        SCALELIB_VERSION
    )
    to_download = {
        scalelib_file: (args.scalelib, scalelib_url),
    }
    if args.cyclecloud_api:
        to_download[os.path.basename(args.cyclecloud_api)] = (args.cyclecloud_api, None)

    for lib_file in to_download:
        arg_override, url = to_download[lib_file]
        if arg_override:
            if not os.path.exists(arg_override):
                print(arg_override, "does not exist", file=sys.stderr)
                sys.exit(1)
            fname = os.path.basename(arg_override)
            orig = os.path.abspath(arg_override)
            dest = os.path.abspath(os.path.join("libs", fname))
            if orig != dest:
                shutil.copyfile(orig, dest)
            ret.append(fname)
        else:
            dest = os.path.join("libs", lib_file)
            check_call(["curl", "--fail", "--location", "--silent", "--show-error", "--output", dest, url])
            ret.append(lib_file)
            print("Downloaded", lib_file, "to")

    if not args.cyclecloud_api:
        ret.append(download_cyclecloud_api("libs"))
    return ret


def execute() -> None:
    expected_cwd = os.path.abspath(os.path.dirname(__file__))
    os.chdir(expected_cwd)

    if not os.path.exists("libs"):
        os.makedirs("libs")

    argument_parser = argparse.ArgumentParser(
        "Builds CycleCloud GridEngine project with all dependencies.\n"
        + "Scalelib and its cyclecloud-api wheel are downloaded from the selected scalelib version on GitHub."
    )
    argument_parser.add_argument(
        "--scalelib", default=os.environ.get("CYCLECLOUD_SCALELIB"),
        help="Local scalelib archive (defaults to CYCLECLOUD_SCALELIB).",
    )
    argument_parser.add_argument("--cyclecloud-api", default=None)
    args = argument_parser.parse_args()

    cycle_libs = get_cycle_libs(args)

    parser = configparser.ConfigParser()
    ini_path = os.path.abspath("project.ini")

    with open(ini_path) as fr:
        parser.read_file(fr)

    version = parser.get("project", "version")
    if not version:
        raise RuntimeError("Missing [project] -> version in {}".format(ini_path))

    if not os.path.exists("dist"):
        os.makedirs("dist")

    tf = tarfile.TarFile.gzopen(
        "dist/cyclecloud-gridengine-pkg-{}.tar.gz".format(version), "w"
    )

    build_dir = tempfile.mkdtemp("cyclecloud-gridengine")

    def _add(name: str, path: Optional[str] = None, mode: Optional[int] = None) -> None:
        path = path or name
        tarinfo = tarfile.TarInfo("cyclecloud-gridengine/" + name)
        tarinfo.size = os.path.getsize(path)
        tarinfo.mtime = int(os.path.getmtime(path))
        if mode:
            tarinfo.mode = mode

        with open(path, "rb") as fr:
            tf.addfile(tarinfo, fr)

    packages = []
    for dep in cycle_libs:
        dep_path = os.path.abspath(os.path.join("libs", dep))
        _add("packages/" + dep, dep_path)
        packages.append(dep_path)

    check_call(["pip", "download"] + packages, cwd=build_dir)

    print("Using build dir", build_dir)
    for fil in os.listdir(build_dir):
        if fil.startswith("certifi-2019"):
            print("WARNING: Ignoring duplicate certifi {}".format(fil))
            continue
        if "charset_normalizer" in fil or fil.lower().startswith("pyyaml-"):
            print("WARNING: removing {}".format(fil))
            continue
        path = os.path.join(build_dir, fil)
        _add("packages/" + fil, path)

    _add("install.sh", mode=os.stat("install.sh")[0])
    _add("generate_autoscale_json.sh", mode=os.stat("generate_autoscale_json.sh")[0])
    _add("logging.conf", "gridengine/conf/logging.conf")


if __name__ == "__main__":
    execute()
