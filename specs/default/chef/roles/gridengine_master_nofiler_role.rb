name "sge_master_nofiler_role"
description "SGE Master, but not the NFS server"
run_list("role[scheduler]",
  "recipe[cshared::client]",
  "recipe[cuser]",
  "recipe[gridengine::master]",
  "recipe[cycle_server::submit_once_clients]",
  "recipe[cycle_server::submit_once_workers]",
  "recipe[cganglia::server]")

default_attributes "cyclecloud" => { "discoverable" => true }
