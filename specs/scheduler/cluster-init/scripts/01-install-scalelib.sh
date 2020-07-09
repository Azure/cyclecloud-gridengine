#!/bin/bash
set -e
BOOTSTRAP=$(jetpack config cyclecloud.bootstrap)
API_FILE_NAME=cyclecloud_api-$(jetpack config cyclecloud.cookbooks.version)-py2.py3-none-any.whl
source /etc/profile.d/sgesettings.sh
python3 -m pip install virtualenv
python3 -m virtualenv $BOOTSTRAP/gridenginevenv
source $BOOTSTRAP/gridenginevenv/bin/activate

# valid for 8.x only
# https://cc-8-dev.westus2.cloudapp.azure.com/static/tools/cyclecloud_api-8.0.1-py2.py3-none-any.whl
wget --no-check-certificate $(jetpack config cyclecloud.config.web_server)/static/tools/$API_FILE_NAME -O $BOOTSTRAP/$API_FILE_NAME
#jetpack download cyclecloud_api-8.0.1-py2.py3-none-any.whl --project gridengine #{node[:cyclecloud][:bootstrap]}/
#jetpack download autoscale-0.1.0.tar.gz --project gridengine #{node[:cyclecloud][:bootstrap]}/
jetpack download cyclecloud-gridengine-2.0.0.tar.gz --project gridengine $BOOTSTRAP/

pip install $BOOTSTRAP/$API_FILE_NAME
cp /tmp/cyclecloud-scalelib-0.1.1.tar.gz $BOOTSTRAP/cyclecloud-scalelib-0.1.1.tar.gz
pip install $BOOTSTRAP/cyclecloud-scalelib-0.1.1.tar.gz
pip install $BOOTSTRAP/cyclecloud-gridengine-2.0.0.tar.gz
# takes the bootstrap config and amends relevant info so that the next step can properly create our queues.
python -m gridengine.cli amend_queue_config -c $BOOTSTRAP/gridengine/bootstrap.json > $BOOTSTRAP/gridengine/autoscale.json
python -m gridengine.cli create_queues -c $BOOTSTRAP/gridengine/autoscale.json


cat << EOF > $BOOTSTRAP/gridengine/gridengine_autoscale.sh
#!/bin/env bash
set -e
source $BOOTSTRAP/gridenginevenv/bin/activate
# -c is not required here, leaving so it is obvious what config is used when debugging
python -m gridengine.cli autoscale -c $BOOTSTRAP/gridengine/autoscale.json
EOF
chmod 0755 $BOOTSTRAP/gridengine/gridengine_autoscale.sh
