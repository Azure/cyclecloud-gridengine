# DEPRECATED: use sge_master_role instead
name "sge_master_role"
description "SGE Master Role"
run_list("role[sge_master_role]")

default_attributes "cyclecloud" => { "discoverable" => true }
