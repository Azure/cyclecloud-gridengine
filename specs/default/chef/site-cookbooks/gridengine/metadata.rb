name             "gridengine"
maintainer       "Cycle Computing"
maintainer_email "cyclecloud-support@cyclecomputing.com"
license          "Apache 2.0"
description      "Installs/Configures gridengine"
long_description IO.read(File.join(File.dirname(__FILE__), 'README.md'))
version          "2.0.13"

%w{ cuser cganglia cycle_server cshared cyclecloud }.each {|c| depends c }
