#
# Cookbook Name:: gridengine
# Recipe:: master
#
# The SGE Master is a Q-Master and a Submitter
#

chefstate = node[:cyclecloud][:chefstate]

#directory "#{node[:cyclecloud][:bootstrap]}/gridengine"

slot_type = node[:gridengine][:slot_type] || "master"

myplatform=node[:platform]

package 'Install binutils' do
  package_name 'binutils'
end

package 'Install hwloc' do
  package_name 'hwloc'
end

case myplatform
when 'ubuntu'  
  package 'Install libnuma' do  
    package_name 'libnuma-dev'
  end
when 'centos'
  package 'Install jemalloc' do
  package_name 'jemalloc'
  end
end


group node[:gridengine][:group][:name] do
  gid node[:gridengine][:group][:gid]
  not_if "getent group #{node[:gridengine][:group][:name]}"
end

user node[:gridengine][:user][:name] do
  comment node[:gridengine][:user][:description]
  uid node[:gridengine][:user][:uid]
  gid node[:gridengine][:user][:gid]
  home node[:gridengine][:user][:home]
  shell node[:gridengine][:user][:shell]
  not_if "getent passwd #{node[:gridengine][:user][:name]}"
end

# TODO maybe need to do this?
# directory "/sched/sge" do
#   owner node[:gridengine][:user][:name]
#   group node[:gridengine][:group][:name]
#   mode "0755"
#   action :create
#   recursive true
#   only_if "test -d /sched"
# end

nodename = node[:cyclecloud][:instance][:hostname]

gridengineroot = node[:gridengine][:root]     # /sched/ge/ge-8.2.0-demo


directory gridengineroot do
  owner node[:gridengine][:user][:name]
  group node[:gridengine][:group][:name]
  mode "0755"
  action :create
  recursive true
end

include_recipe "::_install"

directory File.join(gridengineroot, 'conf') do
  owner node[:gridengine][:user][:name]
  group node[:gridengine][:group][:name]
  mode "0755"
  action :create
  recursive true
end

template "#{gridengineroot}/conf/#{nodename}.conf" do
  source "headnode.conf.erb"
  variables(
    :gridengineroot => gridengineroot,
    :nodename => nodename,
    :ignore_fqdn => node[:gridengine][:ignore_fqdn]
  )
end

execute "installqmaster" do
  command "cd #{gridengineroot} && ./inst_sge -m -auto ./conf/#{nodename}.conf"
  creates "#{gridengineroot}/default"
  action :run
end

link "/etc/profile.d/sgesettings.sh" do
  to "#{gridengineroot}/default/common/settings.sh"
end

link "/etc/profile.d/sgesettings.csh" do
  to "#{gridengineroot}/default/common/settings.csh"
end

link "/etc/cluster-setup.sh" do
  to "#{gridengineroot}/default/common/settings.sh"
end

link "/etc/cluster-setup.csh" do
  to "#{gridengineroot}/default/common/settings.csh"
end

execute "set qmaster hostname" do
  command "hostname -f > #{gridengineroot}/default/common/act_qmaster"
end

case node[:platform_family]
when "rhel"
  mail_root = "/bin"
when "debian"
  mail_root = "/usr/bin"
else
  throw "cluster_init: unsupported platform"
end

template "#{gridengineroot}/conf/global" do
  source "global.erb"
  owner "root"
  group "root"
  mode "0755"
  variables(
    :gridengineroot => gridengineroot,
    :mail_root => mail_root
  )
end

template "#{gridengineroot}/conf/sched" do
  source "sched.erb"
  owner "root"
  group "root"
  mode "0755"
end

template "/etc/init.d/sgemaster" do
  source "sgemaster.erb"
  mode 0755
  owner "root"
  group "root"
  variables(
    :gridengineroot => gridengineroot
  )
end

service "sgemaster" do
  action [:enable, :start]
end

# Remove any hosts from previous runs
bash "clear old hosts" do
  code <<-EOH
  for HOST in `ls -1 #{gridengineroot}/default/spool/ | grep -v qmaster`; do
    . /etc/cluster-setup.sh
    qmod -d *@${HOST}
    qconf -dattr hostgroup hostlist ${HOST} @allhosts
    qconf -de ${HOST}
    qconf -ds ${HOST}
    qconf -dh ${HOST}
    rm -rf #{gridengineroot}/default/spool/${HOST};
  done && touch #{chefstate}/gridengine.clear.hosts
  EOH
  creates "#{chefstate}/gridengine.clear.hosts"
  action :run
end

template "/etc/init.d/sgeexecd" do
  source "sgeexecd.erb"
  mode 0755
  owner "root"
  group "root"
  variables(
    :gridengineroot => gridengineroot
  )
end

service 'sgeexecd' do
  action [:enable, :start]
  not_if { pidfile_running? ::File.join(gridengineroot, 'default', 'spool', node[:hostname], 'execd.pid') }
end

execute "setglobal" do
  command ". /etc/cluster-setup.sh && qconf -Mconf #{gridengineroot}/conf/global && touch #{chefstate}/gridengine.global.set"
  creates "#{chefstate}/gridengine.global.set"
  action :run
end

execute "setsched" do
  command ". /etc/cluster-setup.sh && qconf -Msconf #{gridengineroot}/conf/sched && touch #{chefstate}/gridengine.sched.set"
  creates "#{chefstate}/gridengine.sched.set"
  action :run
end

execute "showalljobs" do
  command "echo \"-u *\" > #{gridengineroot}/default/common/sge_qstat"
  creates "#{gridengineroot}/default/common/sge_qstat"
  action :run
end

execute "schedexecinst" do
  command "cd #{gridengineroot} && ./inst_sge -x -noremote -auto #{gridengineroot}/conf/#{nodename}.conf && touch #{chefstate}/gridengine.sgesched.schedexecinst"
  creates "#{chefstate}/gridengine.sgesched.schedexecinst"
  action :run
end

template "#{gridengineroot}/conf/exec" do
  source "exec.erb"
  owner "root"
  group "root"
  mode "0755"
  variables(
    :nodename => nodename,
    :slot_type => slot_type,
    :placement_group => "default"
  )
end

# UGE 8.3 introduces a new attribute `aapre` to complexes
# aapre - "available after preemption"
# Essentially allows a resource to be made avaiable after preemption
# This change requires a new complex file format
if node[:gridengine][:make]=='ge' and (Gem::Version.new(node[:gridengine][:version]) >= Gem::Version.new('8.3.0'))
  complex_file = "conf/complexes.ge-8.3.x"
else
  complex_file = "conf/complexes"
end

cookbook_file "#{gridengineroot}/conf/complexes" do
  source complex_file
  owner "root"
  group "root"
  mode "0755"
  action :create
end

template "#{gridengineroot}/conf/mpislots" do
  source "mpislots.erb"
  owner "root"
  group "root"
  mode "0755"
end


remote_directory "#{node[:cyclecloud][:bootstrap]}/gridengine" do
  source "src"
  owner 'root'
  group 'root'
  mode '0700'
  action :create
  # not_if {::File.exists?("#{node[:cyclecloud][:bootstrap]}/gridengine/gridengine")}
end


cookbook_file "#{node[:cyclecloud][:bootstrap]}/gridengine/setup.py" do
  source 'setup.py'
  owner 'root'
  group 'root'
  mode '0700'
  action :create
  # not_if {::File.exists?("#{node[:cyclecloud][:bootstrap]}/gridengine/setup.py")}
end


package 'python3' do
  action :install
end

bash 'setup virtualenv' do
  code <<-EOH
  set -e
  python3 -m pip install virtualenv
  python3 -m virtualenv #{node[:cyclecloud][:bootstrap]}/gridenginevenv
  source #{node[:cyclecloud][:bootstrap]}/gridenginevenv/bin/activate
  
  jetpack download cyclecloud-api.tar.gz --project gridengine #{node[:cyclecloud][:bootstrap]}/
  jetpack download autoscale-7.9.5.tar.gz --project gridengine #{node[:cyclecloud][:bootstrap]}/
  
  pip install #{node[:cyclecloud][:bootstrap]}/cyclecloud-api.tar.gz
  pip install #{node[:cyclecloud][:bootstrap]}/autoscale-7.9.5.tar.gz
  touch #{node[:cyclecloud][:bootstrap]}/gridenginevenv.installed
  EOH
  action :run
  # if {::File.exist?("#{node[:cyclecloud][:bootstrap]}/gridengine/setup.py")}
  # not_if {::File.exist?("#{node[:cyclecloud][:bootstrap]}/gridenginevenv.installed")}
end


autoscale_config = {
  :cluster_name => node[:cyclecloud][:cluster][:name],
  :username => node[:cyclecloud][:config][:username],
  :password => node[:cyclecloud][:config][:password],
  :url => node[:cyclecloud][:config][:web_server],
  :logging => {:config_file => "#{node[:cyclecloud][:bootstrap]}/gridengine/logging.conf"},
  :default_resources => [ { :select => {}, :name => "slots", :value => "node.vcpu_count"} ]
}

file "#{node[:cyclecloud][:bootstrap]}/gridengine/gridengine_autoscale.sh" do
  content <<-EOF#!/bin/env bash
  set -e
  source #{node[:cyclecloud][:bootstrap]}/gridenginevenv/bin/activate
  PYTHONPATH=$PYTHONPATH:#{node[:cyclecloud][:bootstrap]}/gridengine python -m gridengine.autoscaler -c #{node[:cyclecloud][:bootstrap]}/gridengine/autoscale.json
  EOF
  owner "root"
  group "root"
  mode "0755"
  action :create
end


file "#{node[:cyclecloud][:bootstrap]}/gridengine/autoscale.json" do
  mode "0644"
  owner "root"
  group "root"
  content Chef::JSONCompat.to_json_pretty(autoscale_config)
  not_if {::File.exists?("#{node[:cyclecloud][:bootstrap]}/gridengine/autoscale.json")}
end


cookbook_file "#{node[:cyclecloud][:bootstrap]}/gridengine/logging.conf" do
  source "conf/logging.conf"
  owner 'root'
  group 'root'
  mode '0644'
  action :create
  not_if {::File.exists?("#{node[:cyclecloud][:bootstrap]}/gridengine/logging.conf")}
end



cron "modify jobs" do
  command "#{node[:cyclecloud][:bootstrap]}/cron_wrapper.sh #{node[:cyclecloud][:bootstrap]}/gridengine/src/modify_jobs.py >> #{node[:cyclecloud][:bootstrap]}/gridengine/modify_jobs.out 2>&1"
end

template "#{gridengineroot}/conf/mpi" do
  source "mpi.erb"
  owner "root"
  group "root"
  mode "0755"
  variables(
    :gridengineroot => gridengineroot
  )
end

template "#{gridengineroot}/conf/smpslots" do
  source "smpslots.erb"
  owner "root"
  group "root"
  mode "0755"
end

cookbook_file "#{gridengineroot}/conf/pecfg" do
  source "conf/pecfg"
  owner "root"
  group "root"
  mode "0755"
  action :create
end

template "#{gridengineroot}/conf/gridengine.q" do
  source "gridengine.q.erb"
  owner "root"
  group "root"
  mode "0755"
  variables(
    :gridengineroot => gridengineroot
  )
end

cookbook_file "#{gridengineroot}/SGESuspend.sh" do
  source "sbin/SGESuspend.sh"
  owner "root"
  group "root"
  mode "0755"
  only_if "test -d #{gridengineroot}"
end

cookbook_file "#{gridengineroot}/SGETerm.sh" do
  source "sbin/SGETerm.sh"
  owner "root"
  group "root"
  mode "0755"
  only_if "test -d #{gridengineroot}"
end

execute "set complexes" do
  command ". /etc/cluster-setup.sh && qconf -Mc #{gridengineroot}/conf/complexes && touch #{chefstate}/gridengine.setcomplexes.done"
  creates "#{chefstate}/gridengine.setcomplexes.done"
  action :run
end

%w( mpi mpislots smpslots ).each do |confFile|
  execute "Add the conf file: " + confFile do
    command ". /etc/cluster-setup.sh && qconf -Ap #{File.join(gridengineroot, 'conf', confFile)}"
    not_if ". /etc/cluster-setup.sh && qconf -spl | grep #{confFile}"
  end
end

# Don't set the parallel environments for the all.q once we've already run this.
# To test, look for one of the PEs we add in the list of PEs associated with the queue.
execute "setpecfg" do
  command ". /etc/cluster-setup.sh && qconf -Rattr queue #{gridengineroot}/conf/pecfg all.q"
  not_if ". /etc/cluster-setup.sh && qconf -sq all.q | grep mpislots"
end

# Configure the qmaster to not run jobs unless the jobs themselves are configured to run on the qmaster host.
# It shouldn't be a problem for this to be set every converge.
execute "setexec" do
  command ". /etc/cluster-setup.sh && qconf -Me #{gridengineroot}/conf/exec"
end

#
execute "gridengine.qcfg" do
  command ". /etc/cluster-setup.sh && qconf -Rattr queue #{gridengineroot}/conf/gridengine.q all.q && touch #{chefstate}/gridengine.qcfg"
  only_if "test -f #{gridengineroot}/SGESuspend.sh && test -f #{gridengineroot}/SGETerm.sh && test -f #{gridengineroot}/conf/gridengine.q && test ! -f #{chefstate}/gridengine.qcfg"
end

execute "Add cycle_server user as a manager" do
  command ". /etc/cluster-setup.sh && qconf -am cycle_server"
  not_if ". /etc/cluster-setup.sh && qconf -sm | grep cycle_server"
end

# Pull in the Jetpack LWRP
include_recipe 'jetpack'

# Notify CycleCloud to configure GridEngine monitoring on each converge
applications = ''
if !node[:submitonce][:applications].nil? && !node[:submitonce][:applications].empty?
  if node[:submitonce][:applications].is_a?(Array)
    app_list = node[:submitonce][:applications].join(',')
  else
    app_list = node[:submitonce][:applications]
  end
  applications = "\"applications\": \"#{app_list}\""
end

monitoring_config = "#{node['cyclecloud']['home']}/config/service.d/gridengine.json"
file monitoring_config do
  content <<-EOH
  {
    "system": "gridengine",
    "cluster_name": "#{node[:cyclecloud][:cluster][:name]}",
    "hostname": "#{node[:cyclecloud][:instance][:public_hostname]}",
    "ports": {"ssh": 22},
    "cellname": "default",
    "gridengineroot": "#{node[:gridengine][:root]}",
    "submitonce": {#{applications}}
  }
  EOH
  mode 750
  not_if { ::File.exist?(monitoring_config) }
end

jetpack_send "Registering QMaster for monitoring." do
  file monitoring_config
  routing_key "#{node[:cyclecloud][:service_status][:routing_key]}.gridengine"
end

include_recipe "gridengine::autostart"