# Copyright (c) Microsoft Corporation. All rights reserved.
# Licensed under the MIT License.
name "gridengine_master_role"
description "GridEngine Master Role"
run_list("role[scheduler]",
  "recipe[cshared::directories]",
  "recipe[cuser]",
  "recipe[cshared::server]",
  "recipe[gridengine::master]")

default_attributes "cyclecloud" => { "discoverable" => true }
