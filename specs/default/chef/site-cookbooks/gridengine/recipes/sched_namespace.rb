#
# Cookbook Name:: gridengine
# Recipe:: sched_namespace
#

sched_mount = (node[:cyclecloud][:mounts] || {})[:nfs_sched] || {}
external_sched_disabled = sched_mount[:disabled].to_s.downcase == "true"
if sched_mount[:mountpoint] == "/sched-main" && !external_sched_disabled
  cluster_name = node[:cyclecloud][:cluster][:name].to_s

  if cluster_name.empty?
    raise "CycleCloud cluster name is missing; cannot determine the external /sched namespace"
  end

  safe_cluster_name = cluster_name.gsub(/[^a-zA-Z0-9-]/, "-").downcase
  cluster_sched_root = "/sched-main/#{safe_cluster_name}"

  execute "verify external scheduler filesystem" do
    command "mountpoint -q /sched-main"
  end

  directory cluster_sched_root do
    owner "root"
    group "root"
    mode "0755"
    recursive true
  end

  directory "/sched" do
    owner "root"
    group "root"
    mode "0755"
  end

  mount "/sched" do
    device cluster_sched_root
    fstype "none"
    options "bind"
    action :mount
  end
end