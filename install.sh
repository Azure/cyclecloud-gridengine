#!/usr/bin/env bash

INSTALL_PYTHON3=0
INSTALL_VIRTUALENV=0
INSTALL_PIP=0
INSTALLDIR=/opt/cycle/gridengine
VENV=$INSTALLDIR/venv
OS=$(awk -F= '/^NAME/{print $2}' /etc/os-release | awk '{print tolower($0)}')

if [ "$OS" == '"ubuntu"' ]; then
    INSTALL_CMD=apt-get
    # ubuntu 18 does not include pip by default
    INSTALL_PIP=1
else
    INSTALL_CMD=yum
fi


while (( "$#" )); do
    case "$1" in
        --install-python3)
            INSTALL_PYTHON3=1
            INSTALL_PIP=1
            INSTALL_VIRTUALENV=1
            shift
            ;;
        --install-venv)
            INSTALL_VIRTUALENV=1
            shift
            ;;
        --venv)
            VENV=$2
            shift 2
            ;;
        --help)
            echo ./install.sh [--install-python3] [--install-venv] [--venv $VENV]
            exit 1
            ;;

        -*|--*=)
            echo "Unknown option $1" >&2
            exit 1
            ;;
        *)
            echo "Unknown option  $1" >&2
            exit 1
            ;;
    esac
done

echo INSTALL_PYTHON3=$INSTALL_PYTHON3
echo INSTALL_VIRTUALENV=$INSTALL_VIRTUALENV
echo VENV=$VENV

# DEPRECATED: For versions < 8.7.0: Remove any jetpack references from the path. 
echo $PATH | sed -E -e 's/\/opt\/cycle\/jetpack\/[^:]*://g'


which python3 > /dev/null;
if [ $? != 0 ]; then
    if [ $INSTALL_PYTHON3 == 1 ]; then
        $INSTALL_CMD install -y python3 || exit 1
    else
        echo Please install python3 >&2;
        exit 1
    fi
fi

python3 -m pip 2>&1 1> /dev/null
if [ $? != 0 ]; then
    if [ $INSTALL_PIP == 1 ]; then
        $INSTALL_CMD install -y python3-pip || exit 1
    else
        echo Please install python3-pip >&2;
        exit 1
    fi
fi


python3 -m virtualenv --version 2>&1 > /dev/null
if [ $? != 0 ]; then
    if [ $INSTALL_VIRTUALENV ]; then
        python3 -m pip install virtualenv || exit 1
    else
        echo Please install virtualenv for python3 >&2
        exit 1
    fi
fi


python3 -m virtualenv $VENV
source $VENV/bin/activate
# not sure why but pip gets confused installing frozendict locally
# if you don't install it first. It has no dependencies so this is safe.
pip install --force-reinstall packages/* 

cat > $VENV/bin/azge <<EOF
#!$VENV/bin/python
import warnings
warnings.filterwarnings("ignore", message="Please use driver.new_singleton_lock")
from gridengine.cli import main
main()
EOF
chmod +x $VENV/bin/azge

azge -h 2>&1 > /dev/null || exit 1

ln -sf $VENV/bin/azge /usr/local/bin/
if [ ! -e /root/bin ]; then
  mkdir /root/bin
fi
ln -sf $VENV/bin/azge /root/bin/

echo 'azge' installed. A symbolic link was made to /usr/local/bin/azge and /root/bin

cp logging.conf $INSTALLDIR/

crontab -l > /tmp/current_crontab
grep -q 'Created by cyclecloud-gridengine install.sh' /tmp/current_crontab
if [ $? != 0 ]; then
    echo \# Created by cyclecloud-gridengine install.sh >> /tmp/current_crontab
    echo '* * * * * . /etc/profile.d/sgesettings.sh && /usr/local/bin/azge autoscale -c /opt/cycle/gridengine/autoscale.json' >> /tmp/current_crontab
    crontab /tmp/current_crontab
fi
rm -f /tmp/current_crontab

crontab -l | grep -q 'Created by cyclecloud-gridengine install.sh' && exit 0
echo "Could not install cron job for autoscale!" >&2
exit 1