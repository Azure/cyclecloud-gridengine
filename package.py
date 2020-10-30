import argparse
import configparser
import os
import sys
import tarfile
import tempfile
from subprocess import check_call
from typing import List, Optional
from argparse import Namespace

SCALELIB_VERSION = "0.1.2"
CYCLECLOUD_API_VERSION = "8.0.1"


def get_cycle_libs(args: Namespace) -> List[str]:
    ret = ["cyclecloud-gridengine-2.0.1.tar.gz"]

    scalelib_file = "cyclecloud-scalelib-{}.tar.gz".format(SCALELIB_VERSION)
    cyclecloud_api_file = "cyclecloud_api-{}-py2.py3-none-any.whl".format(
        CYCLECLOUD_API_VERSION
    )

    scalelib_url = "https://github.com/Azure/cyclecloud-scalelib/archive/{}.tar.gz".format(
        SCALELIB_VERSION
    )
    cyclecloud_api_url = "https://github.com/Azure/cyclecloud-gridengine/releases/download/2.0.0/cyclecloud_api-8.0.1-py2.py3-none-any.whl"
    to_download = {
        scalelib_file: (args.scalelib, scalelib_url),
        cyclecloud_api_file: (args.cyclecloud_api, cyclecloud_api_url),
    }

    for lib_file in to_download:
        arg_override, url = to_download[lib_file]
        if arg_override:
            if not os.path.exists(arg_override):
                print(arg_override, "does not exist", file=sys.stderr)
                sys.exit(1)
            ret.append(arg_override)
        else:
            check_call(["curl", "-L", "-k", "-s", "-o", lib_file, url])
            ret.append(lib_file)

    return ret


def execute() -> None:
    expected_cwd = os.path.abspath(os.path.dirname(__file__))
    os.chdir(expected_cwd)

    if not os.path.exists("libs"):
        os.makedirs("libs")

    argument_parser = argparse.ArgumentParser(
        "Builds CycleCloud GridEngine project with all dependencies.\n"
        + "If you don't specify local copies of scalelib or cyclecloud-api they will be downloaded from github."
    )
    argument_parser.add_argument("--scalelib", default=None)
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

    for dep in cycle_libs:
        dep_path = os.path.abspath(os.path.join("libs", dep))
        _add("packages/" + dep, dep_path)
        check_call(["pip", "download", dep_path], cwd=build_dir)

    print("Using build dir", build_dir)
    for fil in os.listdir(build_dir):
        if fil.startswith("certifi-2019"):
            print("WARNING: Ignoring duplicate certifi {}".format(fil))
            continue
        path = os.path.join(build_dir, fil)
        _add("packages/" + fil, path)

    _add("install.sh", mode=os.stat("install.sh")[0])


if __name__ == "__main__":
    execute()
