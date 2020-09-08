#!/usr/bin/env bash

INSTALL_PYTHON3=0
INSTALL_VIRTUALENV=0
VENV=/opt/cycle/cyclecloud-gridengine/venv


while (( "$#" )); do
    case "$1" in
        --install-python3)
            INSTALL_PYTHON3=1
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

which python3 > /dev/null;
if [ $? != 0 ]; then
    if [ $INSTALL_PYTHON3 == 1 ]; then
        yum install -y python3 || exit 1
    else
        echo Please install python3 >&2;
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
pip install packages/frozendict*
pip install packages/*

cat > $VENV/bin/azge <<EOF
#!$VENV/bin/python

from gridengine.cli import main
main()
EOF
chmod +x $VENV/bin/azge

azge -h 2>&1 > /dev/null || exit 1

ln -sf $VENV/bin/azge /usr/local/bin/

echo 'azge' installed. A symbolic link was made to /usr/local/bin/azge
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