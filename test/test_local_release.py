import copy
import io
from pathlib import Path
import shutil
import subprocess
import tarfile
import tempfile
import unittest
from unittest.mock import patch

import yaml

import package
from util.local_release import BUILD_STEPS, build, build_commands


class LocalReleaseTest(unittest.TestCase):
    def setUp(self):
        self.source = Path(__file__).resolve().parents[1]
        with (self.source / ".github/workflows/release.yml").open() as stream:
            self.workflow = yaml.safe_load(stream)

    def test_only_build_commands_are_selected_unchanged(self):
        commands = build_commands(self.workflow)
        self.assertEqual([name for name, command in commands], list(BUILD_STEPS))
        expected = [
            (step["name"], step["run"])
            for step in self.workflow["jobs"]["build"]["steps"]
            if step["name"] in BUILD_STEPS
        ]
        self.assertEqual(commands, expected)

    def test_publishing_and_unrelated_shell_steps_never_execute(self):
        self.workflow["jobs"]["build"]["steps"].append(
            {"name": "Publish with shell", "run": "exit 99"}
        )
        with patch("util.local_release.yaml.safe_load", return_value=self.workflow):
            with patch("util.local_release.subprocess.run") as run:
                with patch("util.local_release.tarfile.open", side_effect=ValueError):
                    with self.assertRaises(ValueError):
                        build(self.source, self.source)
                self.assertEqual(run.call_count, 2)
                self.assertEqual(
                    [call.args[0][-1] for call in run.call_args_list],
                    [command for name, command in build_commands(self.workflow)],
                )

    def test_changed_steps_fail_closed(self):
        for change in ("missing", "duplicate", "reordered", "uses", "env", "expression", "empty"):
            with self.subTest(change=change):
                workflow = copy.deepcopy(self.workflow)
                steps = workflow["jobs"]["build"]["steps"]
                step = next(step for step in steps if step["name"] == "Build pkg")
                if change == "missing":
                    steps.remove(step)
                elif change == "duplicate":
                    steps.append(copy.deepcopy(step))
                elif change == "reordered":
                    steps.reverse()
                elif change in ("uses", "env"):
                    step[change] = "unsupported"
                else:
                    step["run"] = "${{ secrets.ACTION_PAT }}" if change == "expression" else ""
                with self.assertRaises(ValueError):
                    build_commands(workflow)

    def test_failed_build_stops_before_downloads(self):
        with patch("util.local_release.subprocess.run") as run:
            run.side_effect = subprocess.CalledProcessError(1, "build")
            with self.assertRaises(subprocess.CalledProcessError):
                build(self.source, self.source)
            self.assertEqual(run.call_count, 1)

    def test_scalelib_environment_default_and_cli_precedence(self):
        for arguments, expected in (
            ([], "/input/local scalelib.tar.gz"),
            (["--scalelib", "/explicit/scalelib.tar.gz"], "/explicit/scalelib.tar.gz"),
        ):
            with self.subTest(arguments=arguments):
                with patch.dict("os.environ", {"CYCLECLOUD_SCALELIB": "/input/local scalelib.tar.gz"}):
                    with patch("sys.argv", ["package.py"] + arguments):
                        with patch("package.os.chdir"):
                            with patch("package.get_cycle_libs", side_effect=RuntimeError("stop before build")) as libraries:
                                with self.assertRaisesRegex(RuntimeError, "stop before build"):
                                    package.execute()
                                self.assertEqual(libraries.call_args.args[0].scalelib, expected)

    def test_bundle_excludes_platform_specific_dependencies(self):
        filenames = [
            "charset_normalizer-3.5.1-cp312-cp312-manylinux2014_x86_64.whl",
            "pyyaml-6.0.3-cp312-cp312-manylinux2014_x86_64.whl",
            "PyYAML-6.0.3-cp312-cp312-manylinux2014_x86_64.whl",
            "requests-2.32.5-py3-none-any.whl",
        ]
        with tempfile.TemporaryDirectory() as directory:
            build_dir = Path(directory)
            for filename in filenames:
                (build_dir / filename).write_bytes(b"dependency")
            with patch("sys.argv", ["package.py"]), \
                    patch("package.get_cycle_libs", return_value=[]), \
                    patch("package.check_call"), \
                    patch("package.tempfile.mkdtemp", return_value=directory), \
                    patch("package.os.chdir"), \
                    patch("package.os.makedirs"), \
                    patch("package.tarfile.TarFile.gzopen") as archive:
                package.execute()
            bundled = [call.args[0].name for call in archive.return_value.addfile.call_args_list]
            self.assertIn("cyclecloud-gridengine/packages/" + filenames[-1], bundled)
            for filename in filenames[:-1]:
                self.assertNotIn("cyclecloud-gridengine/packages/" + filename, bundled)

    def test_wrapper_rejects_missing_archive_before_docker(self):
        with tempfile.TemporaryDirectory() as directory:
            result = subprocess.run(
                ["/bin/bash", str(self.source / "docker-package.sh"), "--scalelib",
                 str(Path(directory) / "missing.tar.gz")],
                capture_output=True, text=True,
            )
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("Scalelib archive is not a readable file", result.stderr)

    def test_artifacts_are_validated_before_copying(self):
        for invalid in (False, True):
            with self.subTest(invalid=invalid), tempfile.TemporaryDirectory() as directory:
                source = Path(directory)
                (source / ".github/workflows").mkdir(parents=True)
                shutil.copyfile(
                    self.source / ".github/workflows/release.yml",
                    source / ".github/workflows/release.yml",
                )
                (source / "project.ini").write_text("[project]\nversion = test\n")
                (source / "dist").mkdir()
                output = source / "output"
                output.mkdir()
                names = (
                    "cyclecloud-gridengine-pkg-test.tar.gz",
                    "sge-2011.11-64.tgz",
                    "sge-2011.11-common.tgz",
                )
                for name in names:
                    with tarfile.open(source / "dist" / name, "w:gz") as archive:
                        member = tarfile.TarInfo("payload")
                        member.size = 4
                        archive.addfile(member, io.BytesIO(b"test"))
                if invalid:
                    (source / "dist" / names[-1]).write_text("HTTP error page")
                with patch("util.local_release.subprocess.run"):
                    if invalid:
                        with self.assertRaises(tarfile.ReadError):
                            build(source, output)
                        self.assertEqual(list(output.iterdir()), [])
                    else:
                        build(source, output)
                        self.assertEqual(sorted(path.name for path in output.iterdir()), sorted(names))


if __name__ == "__main__":
    unittest.main()