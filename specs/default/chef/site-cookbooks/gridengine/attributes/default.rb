default[:gridengine][:make] = "sge"
default[:gridengine][:version] = "2011.11"
default[:gridengine][:root] = "/sched/sge/sge-2011.11"
default[:gridengine][:cell] = "default"
default[:gridengine][:package_extension] = "tar.gz"
default[:gridengine][:installer] = "cyclecloud-gridengine-pkg-2.0.13.tar.gz"
default[:gridengine][:use_external_download] = false
default[:gridengine][:remote_prefix] = nil

default[:gridengine][:sge_qmaster_port] = "537"
default[:gridengine][:sge_execd_port] = "538"
default[:gridengine][:sge_cluster_name] = "grid1"
default[:gridengine][:gid_range] = "20000-20100"
default[:gridengine][:qmaster_spool_dir] = "#{node['gridengine']['root']}/#{node['gridengine']['cell']}/spool/qmaster" 
default[:gridengine][:execd_spool_dir] = "#{node['gridengine']['root']}/#{node['gridengine']['cell']}/spool"
default[:gridengine][:spooling_method] = "berkeleydb"

default[:gridengine][:shadow_host] = ""
default[:gridengine][:admin_mail] = ""

default[:gridengine][:idle_timeout] = 300

default[:gridengine][:managed_fs] = true
default[:gridengine][:shared][:bin] = true
default[:gridengine][:shared][:spool] = true

default[:gridengine][:slots] = nil
default[:gridengine][:slot_type] = nil


default[:gridengine][:is_grouped] = false

default[:gridengine][:ignore_fqdn] = true

# Grid engine user settings
default[:gridengine][:group][:name] = "sgeadmin"
default[:gridengine][:group][:gid] = 536

default[:gridengine][:user][:name] = "sgeadmin"
default[:gridengine][:user][:description] = "SGE admin user"
default[:gridengine][:user][:home] = "/shared/home/sgeadmin"
default[:gridengine][:user][:shell] = "/bin/bash"
default[:gridengine][:user][:uid] = 536
default[:gridengine][:user][:gid] = node[:gridengine][:group][:gid]

default[:gridengine][:max_group_backlog] = 1
