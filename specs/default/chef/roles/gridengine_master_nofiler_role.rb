name "sge_master_nofiler_role"
description "SGE Master, but not the NFS server"
run_list("role[scheduler]",
  "recipe[cshared::client]",
  "recipe[cuser]",
  "recipe[gridengine::master]")

default_attributes "cyclecloud" => { "discoverable" => true }
