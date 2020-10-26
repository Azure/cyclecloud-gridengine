# DEPRECATED: this is a duplicate of sge_master_nofiler_role
name "sge_scheduler_no_filer_role"
description "SGE Server (without filer) Role"
run_list("role[sge_master_nofiler_role]")
