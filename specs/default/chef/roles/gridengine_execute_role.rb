name "sge_execute_role"
description "SGE Client Role"
run_list("recipe[cyclecloud::_hosts]",
  "recipe[cshared::client]",
  "recipe[cuser]",
  "recipe[gridengine::execute]")
