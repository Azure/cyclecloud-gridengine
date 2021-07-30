# DEPRECATED:
# Use a single node with sge_scheduler_role instead (or 2 nodes and a runlist
# that separates out the Monitor role and the SGE Scheduler role)
#
# This role is maintaine only for backwards compatibility with existing
# clusters.

name "sge_master_role"
description "SGE Master Role"
run_list("role[scheduler]",
  "recipe[cshared::directories]",
  "recipe[cuser]",
  "recipe[cshared::server]",
  "recipe[gridengine::master]",
  "recipe[cganglia::server]")

default_attributes "cyclecloud" => { "discoverable" => true }
