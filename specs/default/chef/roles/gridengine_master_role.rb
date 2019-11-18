name "sge_master_role"
description "SGE Master Role"
run_list("role[scheduler]",
  "recipe[cshared::directories]",
  "recipe[cuser]",
  "recipe[cshared::server]",
  "recipe[gridengine::master]",
  "recipe[cycle_server::submit_once_clients]",
  "recipe[cycle_server::submit_once_workers]",
  "recipe[cganglia::server]")

default_attributes "cyclecloud" => { "discoverable" => true }
