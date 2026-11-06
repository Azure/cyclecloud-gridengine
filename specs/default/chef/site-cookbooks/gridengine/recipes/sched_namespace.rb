#
# Cookbook Name:: gridengine
# Recipe:: sched_namespace
#

require "shellwords"

sched_mount = (node[:cyclecloud][:mounts] || {})[:nfs_sched] || {}
external_sched_disabled = sched_mount[:disabled].to_s.downcase == "true"
if sched_mount[:mountpoint] == "/sched-main" && !external_sched_disabled
  cluster_name = node[:cyclecloud][:cluster][:name].to_s

  if cluster_name.empty?
    raise "CycleCloud cluster name is missing; cannot determine the external /sched namespace"
  end

  script_path = "/usr/local/bin/gridengine-sched-namespace"
  cookbook_file script_path do
    source "sbin/sched_namespace.sh"
    owner "root"
    group "root"
    mode "0755"
  end

  execute "configure scheduler namespace" do
    command "#{script_path} #{Shellwords.escape(cluster_name)}"
  end
end