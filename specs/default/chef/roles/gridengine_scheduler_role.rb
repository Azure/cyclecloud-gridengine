# DEPRECATED:
# Use a single node with sge_master_role instead (or 2 nodes and a runlist
# that separates out the Monitor role and the SGE Master role)
#
# This role is maintaine only for backwards compatibility with existing
# clusters.

name "sge_scheduler_role"
description "SGE Server Role"
run_list("role[scheduler]",
  "recipe[cshared::directories]",
  "recipe[cuser]",
  "recipe[cshared::server]",
  "recipe[gridengine::master]",
  "recipe[cycle_server::submit_once_clients]",
  "recipe[cycle_server::submit_once_workers]",
  "recipe[cganglia::client]")
