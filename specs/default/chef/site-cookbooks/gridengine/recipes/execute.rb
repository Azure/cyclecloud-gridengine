#
# Cookbook Name:: gridengine
# Recipe:: execute
#

include_recipe "gridengine::_sge_root_fs"
include_recipe "gridengine::submitter"

sgeroot = node[:gridengine][:root]

# nodename assignments in the resouce blocks in this recipe are delayed till
# the execute phase by using the lazy evaluation.
# This accomodates run lists that change the hostname of the node.

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
#  when 'centos'
#    package_name 'whatevercentoscallsit'
  end
end

shared_bin = node[:gridengine][:shared][:bin]

if not(shared_bin)
  directory sgeroot do
    owner node[:gridengine][:user][:name]
    group node[:gridengine][:group][:name]
    mode "0755"
    action :create
    recursive true
  end
  
  include_recipe "::_install"
end

cookbook_file "#{node[:cyclecloud][:bootstrap]}/checkforjobs.sh" do
  source "checkforjobs.sh"
  owner "root"
  group "root"
  mode "0755"
  action :create
end

myplatform=node[:platform]
myplatform = "centos" if node[:platform_family] == "rhel" # TODO: fix this hack for redhat

# To keep everything consistent, GridEngine cleanup script uses the same hostname as the
# rest of the recipe. It used to shell out to `hostname`
template "/etc/init.d/sgeclean" do
  source "sgeclean.#{myplatform}.erb"
  owner "root"
  group "root"
  mode "0755"
  variables lazy {
    {
      :sgeroot => sgeroot,
      :nodename => node[:hostname]
    }
  }
end

template "/etc/init.d/sgeexecd" do
  source "sgeexecd.erb"
  mode 0755
  owner "root"
  group "root"
  variables({
    :sgeroot => sgeroot
  })
end

directory "/etc/acpi/events" do
  recursive true
end
cookbook_file "/etc/acpi/events/preempted" do
  source "preempted"
  mode "0644"
end

cookbook_file "/etc/acpi/preempted.sh" do
  source "preempted.sh"
  mode "0755"
end

gridengine_settings = "/etc/cluster-setup.sh"

slot_type = node[:gridengine][:slot_type] || "execute"
placement_group = node[:cyclecloud][:node][:placement_group] || nil
is_node_grouped = node[:gridengine][:is_grouped]

if !is_node_grouped
  # location of VMs is irrelevant. We use the "default" string for compatibility.
  placement_group = "default"
end

# TODO: Find a better way to detect execute node installation
# (needs to handle reuse of hostnames since sgeroot is shared)

# in high node churn envs with reused hostnames, GridEngine isn't re-enabling queues
execute "gridengine_enable_host" do
    command lazy { ". /etc/cluster-setup.sh && qmod -e *@#{node[:hostname]} && touch /etc/gridengineexecd.enabled"}
    creates "/etc/gridengineexecd.enabled"
    action :nothing
end

service 'sgeexecd' do
  action [:enable, :start]
  only_if { ::File.exist?('/etc/sgeexecd.installed') }
  not_if { pidfile_running? ::File.join(sgeroot, 'default', 'spool', node[:hostname], 'execd.pid') }
end

execute "configure_slot_attributes" do
  set_slot_type = lambda { "qconf -mattr exechost complex_values slot_type=#{slot_type} #{node[:hostname]}" }
  
  set_slot_count = lambda { "true" }  # No-Op
  if node[:gridengine][:slots]
    set_slot_count = lambda { "qconf -mattr queue slots #{node[:gridengine][:slots]} all.q@#{node[:hostname]}" }
  end
  
  # Note: hostname changes during converge, which is why we want to run these commands lazily.
  set_placement_group = lambda { "qconf -mattr exechost complex_values placement_group=#{placement_group} #{node[:hostname]}" }

  # grouped/mpi jobs are exclusive
  set_node_exclusivity = lambda { "true" }
  if is_node_grouped
    set_node_exclusivity = lambda { "qconf -mattr exechost complex_values exclusive=true #{node[:hostname]}" }
  end

  command lazy {
    <<-EOS
      . #{gridengine_settings} && \
      #{set_slot_type.call} && \
      #{set_placement_group.call} && \
      #{set_node_exclusivity.call} && \
      #{set_slot_count.call} && \
      touch /etc/gridengineexecd.configured
    EOS

  }
  # bug 2832164 - if configure failed it was never called again and the node would converge but remain disabled.
  only_if { ::File.exist?('/etc/gridengineexecd.installed') }
  creates "/etc/gridengineexecd.configured"

  notifies :start, 'service[sgeexecd]', :immediately
  notifies :run, 'execute[gridengine_enable_host]', :immediately
end

# Store node conf file to local disk to avoid requiring shared filesystem
template "#{Chef::Config['file_cache_path']}/compnode.conf" do
  source "compnode.conf.erb"
  variables lazy {
    {
      :sgeroot => sgeroot,
      :nodename => node[:hostname]
    }
  }
end

execute "install_gridengine_execd" do
  cwd sgeroot
  command "./inst_sge -x -noremote -auto #{Chef::Config['file_cache_path']}/compnode.conf && touch /etc/gridengineexecd.installed"
  creates "/etc/gridengineexecd.installed"
  action :nothing
end

defer_block 'Defer install and start of GridEngine execd until end of converge and Master authorizes node' do
  ruby_block "gridengine exec authorized?" do
    block do
      raise "GridEngine Execute node not authorized yet" unless ::File.exist? "#{sgeroot}/host_tokens/hasauth/#{node[:hostname]}"
    end
    retries 5
    retry_delay 30
    notifies :run, 'execute[install_gridengine_execd]', :immediately
    notifies :run, 'execute[configure_slot_attributes]', :immediately
  end
end

case myplatform
when "ubuntu"
  true # Ubuntu chokes on the chkconfig thing and I think the service enable should take care of it.
when "centos"
  execute "addcallback" do
    command "test -f /etc/init.d/sgeclean && /sbin/chkconfig --add sgeclean"
    creates "/etc/rc.d/rc0.d/K01sgeclean"
  end
end

service "sgeclean" do
  action [:enable, :start]
end

include_recipe "gridengine::autostop"
