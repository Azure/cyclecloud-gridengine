import os
import shlex
import subprocess
import sys
from abc import ABC, abstractmethod, abstractproperty
from shutil import which
from subprocess import STDOUT, CalledProcessError
from subprocess import check_call as _check_call
from subprocess import check_output as _check_output
from typing import Any, List, Optional

from hpc.autoscale import hpclogging as logging

_QCMD_LOGGER = logging.getLogger("gridengine.driver")
_QCONF_PATH = which("qconf") or ""
_QMOD_PATH = which("qmod") or ""
_QSELECT_PATH = which("qselect") or ""
_QSTAT_PATH = which("qstat") or ""

__VALIDATED = False
if not __VALIDATED:
    for key, value in list(globals().items()):
        if key.startswith("_Q") and key.endswith("_PATH"):
            if not value:
                executable = key.split("_")[0].lower()
                logging.error(
                    "Could not find %s in PATH: %s", executable, os.environ.get("PATH")
                )
__VALIDATED = True


def check_call(cmd: List[str], *args: Any, **kwargs: Any) -> None:
    shlexed = " ".join([shlex.quote(x) for x in cmd])
    logging.trace("Running '%s'", shlexed)
    _QCMD_LOGGER.info(shlexed)
    try:
        _check_call(cmd, *args, **kwargs)
    except Exception as e:
        logging.error("'%s' failed: %s", shlexed, str(e))
        _QCMD_LOGGER.error(">> %s", str(e))
        raise


def call(cmd: List[str]) -> None:
    shlexed = " ".join([shlex.quote(x) for x in cmd])
    logging.trace("Running '%s'", shlexed)
    _QCMD_LOGGER.info(shlexed)
    stderr = ""
    completed_process = None
    try:
        # capture_output was added in 3.7 and we support as far back as 3.6
        if sys.version_info < (3, 7):
            completed_process = subprocess.run(cmd, stderr=subprocess.PIPE)
        else:
            completed_process = subprocess.run(cmd, capture_output=True)

        if completed_process.returncode != 0:
            if completed_process.stderr:
                stderr = completed_process.stderr.decode()
            logging.warning(
                "'%s' failed with exit %d: Stderr '%s'",
                shlexed,
                completed_process.returncode,
                stderr,
            )
    except Exception as e:
        logging.error("'%s' failed: %s.", shlexed, str(e))
        _QCMD_LOGGER.error(">> %s", str(e))
        raise


def check_output(cmd: List[str], *args: Any, **kwargs: Any) -> Any:
    if not cmd or not cmd[0]:
        raise RuntimeError(
            "Could not run the following command {}. Please check your PATH".format(cmd)
        )
    kwargs["stderr"] = kwargs.pop("stderr", STDOUT)
    shlexed = " ".join([shlex.quote(x) for x in cmd])
    logging.trace("Running '%s'", shlexed)
    _QCMD_LOGGER.info(shlexed)
    try:
        return _check_output(cmd, *args, **kwargs).decode()
    except Exception as e:
        logging.error("'%s' failed: %s", shlexed, str(e))
        _QCMD_LOGGER.error(">> %s", str(e))
        raise


class QBin(ABC):
    @abstractmethod
    def qconf(self, args: List[str], check: bool = True) -> str:
        ...

    @abstractmethod
    def qmod(self, args: List[str], check: bool = True) -> str:
        ...

    @abstractmethod
    def qselect(self, args: List[str], check: bool = True) -> str:
        ...

    @abstractmethod
    def qstat(self, args: List[str], check: bool = True) -> str:
        ...

    @abstractproperty
    def is_uge(self) -> bool:
        ...


class QBinImpl(QBin):
    def __init__(self, is_uge: Optional[bool] = None):
        if is_uge is not None:
            self.__is_uge = is_uge
        elif os.getenv("CYCLECLOUD_GRIDENGINE_FLAVOR", ""):
            self.__is_uge = (
                os.getenv("CYCLECLOUD_GRIDENGINE_FLAVOR", "").lower() == "uge"
            )
        else:
            helpmsg = self.qconf(["-help"], check=False)
            self.__is_uge = helpmsg.startswith("UGE")

    def _call(self, bin: str, args: List[str], check: bool = True) -> str:
        try:
            return check_output([bin] + args)
        except CalledProcessError as e:
            if check:
                raise
            return e.stdout

    def qconf(self, args: List[str], check: bool = True) -> str:
        return self._call(_QCONF_PATH, args, check)

    def qmod(self, args: List[str], check: bool = True) -> str:
        return self._call(_QMOD_PATH, args, check)

    def qselect(self, args: List[str], check: bool = True) -> str:
        return self._call(_QSELECT_PATH, args, check)

    def qstat(self, args: List[str], check: bool = True) -> str:
        return self._call(_QSTAT_PATH, args, check)

    @property
    def is_uge(self) -> bool:
        return self.__is_uge
