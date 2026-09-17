import configparser
import os
from pathlib import Path
import shutil
import subprocess
import tarfile

import yaml


BUILD_STEPS = ("Build pkg", "Get SGE binaries")


def build_commands(workflow):
    steps = workflow["jobs"]["build"]["steps"]
    selected = [step for step in steps if step.get("name") in BUILD_STEPS]
    if [step["name"] for step in selected] != list(BUILD_STEPS):
        raise ValueError("Expected exactly one Build pkg followed by Get SGE binaries")
    for step in selected:
        if set(step) - {"name", "id", "run"}:
            raise ValueError("Unsupported build step fields: " + step["name"])
        command = step.get("run")
        if not isinstance(command, str) or not command.strip() or "${{" in command:
            raise ValueError("Expected a plain shell command: " + step["name"])
    return [(step["name"], step["run"]) for step in selected]


def build(source, output):
    with (source / ".github/workflows/release.yml").open() as stream:
        commands = build_commands(yaml.safe_load(stream))
    environment = dict(os.environ, GITHUB_WORKSPACE=str(source))
    for name, command in commands:
        print("Running workflow step: " + name, flush=True)
        subprocess.run(
            ["/bin/bash", "-e", "-o", "pipefail", "-c", command],
            cwd=source, env=environment, check=True,
        )

    project = configparser.ConfigParser()
    project.read(source / "project.ini")
    artifacts = [
        "cyclecloud-gridengine-pkg-{}.tar.gz".format(project["project"]["version"]),
        "sge-2011.11-64.tgz",
        "sge-2011.11-common.tgz",
    ]
    for name in artifacts:
        with tarfile.open(source / "dist" / name, "r:gz") as archive:
            if not archive.getmembers():
                raise ValueError("Empty artifact: " + name)
    for name in artifacts:
        shutil.copyfile(source / "dist" / name, output / name)


if __name__ == "__main__":
    build(Path.cwd(), Path("/output"))