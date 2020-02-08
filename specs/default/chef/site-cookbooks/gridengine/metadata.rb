# Copyright (c) Microsoft Corporation. All rights reserved.
# Licensed under the MIT License.
name             "gridengine"
maintainer       "Microsoft"
maintainer_email 'support@cyclecomputing.com'
license          'All Rights Reserved'
description      "Installs/Configures GridEngine"
long_description IO.read(File.join(File.dirname(__FILE__), 'README.md'))
version          "1.0"

%w{ cuser cshared cyclecloud }.each {|c| depends c }
