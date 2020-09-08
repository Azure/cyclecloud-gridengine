import os
import shlex
import subprocess
import sys
from shutil import which
from subprocess import check_call as _check_call
from subprocess import check_output as _check_output
from typing import Any, List

from hpc.autoscale import hpclogging as logging

QCONF_PATH = which("qconf") or ""
QMOD_PATH = which("qmod") or ""
QSELECT_PATH = which("qselect") or ""
QSTAT_PATH = which("qstat") or ""

__VALIDATED = False
if not __VALIDATED:
    for key, value in list(globals().items()):
        if key.startswith("Q") and key.endswith("_PATH"):
            if not value:
                executable = key.split("_")[0].lower()
                logging.error(
                    "Could not find %s in PATH: %s", executable, os.environ.get("PATH")
                )
__VALIDATED = True


DISABLED_RESOURCE_GROUP = "limitcycleclouddisabled"
DISABLED_HOST_GROUP = "@cycleclouddisabled"


def check_call(cmd: List[str], *args: Any, **kwargs: Any) -> None:
    q_logger = logging.getLogger("gridengine.driver")
    shlexed = " ".join([shlex.quote(x) for x in cmd])
    logging.trace("Running '%s'", shlexed)
    q_logger.info(shlexed)
    try:
        _check_call(cmd, *args, **kwargs)
    except Exception as e:
        logging.error("'%s' failed: %s", shlexed, str(e))
        q_logger.error(">> %s", str(e))
        raise


def call(cmd: List[str]) -> None:
    q_logger = logging.getLogger("gridengine.driver")
    shlexed = " ".join([shlex.quote(x) for x in cmd])
    logging.trace("Running '%s'", shlexed)
    q_logger.info(shlexed)
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
        q_logger.error(">> %s", str(e))
        raise


def check_output(cmd: List[str], *args: Any, **kwargs: Any) -> Any:
    if not cmd or not cmd[0]:
        raise RuntimeError(
            "Could not run the following command {}. Please check your PATH".format(cmd)
        )

    q_logger = logging.getLogger("gridengine.driver")
    shlexed = " ".join([shlex.quote(x) for x in cmd])
    logging.trace("Running '%s'", shlexed)
    q_logger.info(shlexed)
    try:
        return _check_output(cmd, *args, **kwargs)
    except Exception as e:
        logging.error("'%s' failed: %s", shlexed, str(e))
        q_logger.error(">> %s", str(e))
        raise
