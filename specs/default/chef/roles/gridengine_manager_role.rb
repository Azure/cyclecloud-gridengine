# DEPRECATED:
# Use a single node with sge_master_role instead (or 2 nodes and a runlist
# that separates out the Monitor role and the SGE Master role)
#
# This role is maintaine only for backwards compatibility with existing
# clusters.

name "sge_manager_role"
description "SGE Manager Role"
run_list("recipe[gridengine::master]",
  "recipe[cshared::client]",
  "recipe[cuser]",
  "recipe[cganglia::server]")
