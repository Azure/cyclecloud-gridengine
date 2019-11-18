# test: ignore
import configparser
import os
import re
from subprocess import check_call, check_output
from typing import List

from setuptools import find_packages, setup
from setuptools.command.test import Command
from setuptools.command.test import test as TestCommand  # noqa: N812

with open("src/version.py") as f:
    _version_line = f.readlines()[0]
    _match = re.match(r'__version__ = "(\d+\.\d+\.\d+)-SNAPSHOT"', _version_line)
    assert _match is not None
    __version__ = _match.group(1)
    proj_ini = configparser.ConfigParser()
    proj_ini.read("../project.ini")
    assert __version__ == proj_ini["project"]["version"], "version.py does not match project.ini version"


class PyTest(TestCommand):
    def finalize_options(self) -> None:
        TestCommand.finalize_options(self)
        import os

        xml_out = os.path.join(".", "build", "test-results", "pytest.xml")
        if not os.path.exists(os.path.dirname(xml_out)):
            os.makedirs(os.path.dirname(xml_out))
        # -s is needed so py.test doesn't mess with stdin/stdout
        self.test_args = ["-s", "test", "--junitxml=%s" % xml_out]
        # needed for older setuptools to actually run this as a test
        self.test_suite = True

    def run_tests(self) -> None:
        # import here, cause outside the eggs aren't loaded
        import sys
        import pytest

        # run the tests, then the format checks.
        errno = pytest.main(self.test_args)
        if errno != 0:
            sys.exit(errno)

        check_call(
            ["black", "--check", "src", "test"],
            cwd=os.path.dirname(os.path.abspath(__file__)),
        )
        check_call(
            ["isort", "-c"],
            cwd=os.path.join(os.path.dirname(os.path.abspath(__file__)), "src"),
        )
        check_call(
            ["isort", "-c"],
            cwd=os.path.join(os.path.dirname(os.path.abspath(__file__)), "test"),
        )

        run_type_checking()

        sys.exit(errno)


class Formatter(Command):
    user_options: List[str] = []

    def initialize_options(self) -> None:
        pass

    def finalize_options(self) -> None:
        pass

    def run(self) -> None:
        check_call(
            ["black", "src", "test"], cwd=os.path.dirname(os.path.abspath(__file__)),
        )
        check_call(
            ["isort", "-y"],
            cwd=os.path.join(os.path.dirname(os.path.abspath(__file__)), "src"),
        )
        check_call(
            ["isort", "-y"],
            cwd=os.path.join(os.path.dirname(os.path.abspath(__file__)), "test"),
        )
        run_type_checking()


def run_type_checking() -> None:
    check_call(
        [
            "mypy",
            "--ignore-missing-imports",
            "--follow-imports=silent",
            "--show-column-numbers",
            "--disallow-untyped-defs",
            os.path.join(os.path.dirname(os.path.abspath(__file__)), "test"),
        ]
    )
    check_call(
        [
            "mypy",
            "--ignore-missing-imports",
            "--follow-imports=silent",
            "--show-column-numbers",
            "--disallow-untyped-defs",
            os.path.join(os.path.dirname(os.path.abspath(__file__)), "src"),
        ]
    )

    check_call(["flake8", "--ignore=F405,E501,W503", "src", "test", "setup.py"])


class TypeChecking(Command):
    user_options: List[str] = []

    def initialize_options(self) -> None:
        pass

    def finalize_options(self) -> None:
        pass

    def run(self) -> None:
        run_type_checking()


setup(
    name="cyclecloud-gridengine",
    version=__version__,
    packages=find_packages(where="src"),
    package_dir={"": "src"},
    package_data={
        "cyclecloud-gridengine": [
            "BUILD_NUMBER",
            "private-requirements.json",
            "../NOTICE",
            "../notices",
        ]
    },
    install_requires=["requests == 2.21.0", "typing_extensions"]
    + ["certifi==2019.3.9", "chardet==3.0.4", "idna==2.8", "urllib3==1.24.1"],  # noqa: W503
    tests_require=["pytest==3.2.3"],
    cmdclass={"test": PyTest, "format": Formatter, "types": TypeChecking},
    url="http://www.cyclecomputing.com",
    maintainer="Cycle Computing",
    maintainer_email="support@cyclecomputing.com",
)