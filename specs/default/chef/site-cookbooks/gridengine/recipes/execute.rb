#
# Cookbook Name:: gridengine
# Recipe:: sgeexec
#

include_recipe "gridengine::sgefs"
include_recipe "gridengine::submitter"

gridengineroot = node[:gridengine][:root]
gridenginecell = node[:gridengine][:cell]

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
  directory gridengineroot do
    owner node[:gridengine][:user][:name]
    group node[:gridengine][:group][:name]
    mode "0755"
    action :create
    recursive true
  end
  
  include_recipe "::_install"
end


myplatform=node[:platform]
myplatform = "centos" if node[:platform_family] == "rhel" # TODO: fix this hack for redhat


#template "/etc/init.d/sgeexecd" do
#  source "sgeexecd.erb"
#  mode 0755
#  owner "root"
#  group "root"
#  variables({
#    :gridengineroot => gridengineroot
#  })
#end

template "/etc/systemd/system/sgeexecd.service" do
  source "sgeexecd.service.erb"
  mode 0644
  owner "root"
  group "root"
  variables({
    :gridengineroot => gridengineroot ,
    :gridenginecell => gridenginecell ,
  })
end


directory "/etc/acpi/events" do
  recursive true
end
cookbook_file "/etc/acpi/events/preempted" do
  source "conf/preempted"
  mode "0644"
end

cookbook_file "/etc/acpi/preempted.sh" do
  source "sbin/preempted.sh"
  mode "0755"
end

gridengine_settings = "/etc/cluster-setup.sh"

# Store node conf file to local disk to avoid requiring shared filesystem
template "#{Chef::Config['file_cache_path']}/compnode.conf" do
  source "compnode.conf.erb"
  variables lazy {
    {
      :gridengineroot => gridengineroot,
      :nodename => node[:hostname]
    }
  }
end


ruby_block "gridengine exec authorized?" do
  block do
    raise "gridengine Execute node not authorized yet" \
    unless \
    `qconf -se #{node[:hostname]} > /dev/null && qstat -f -s s | grep #{node[:hostname]} | rev | cut -d" " -f1 | rev | grep -vq d`
    # 1) see if the node has been added at all then 2) see if it is enabled
    # qstat -f is expensive. -s s will limit the jobs to just suspended jobs.
  end
  retries 5
  retry_delay 30
  #notifies :start, 'service[sgeexecd]', :immediately
end

# this starts the sge_execd process as well - also requires host to be authorized 
execute "install_gridengine_execd" do
  cwd gridengineroot
  command "./inst_sge -x -noremote -auto #{Chef::Config['file_cache_path']}/compnode.conf && touch /etc/gridengineexecd.installed"
  creates "/etc/gridengineexecd.installed"
  notifies :enable, 'service[sgeexecd]', :immediately
end

# # special handling of memory, as we don't have a good way to get the true value a priori
# memory = node[:memory][:total].sub("B", "")  # ends with kB, we want just k
# execute "set m_mem_free" do
#   cwd gridengineroot
#   command ". #{gridengine_settings} && qconf -mattr exechost complex_values m_mem_free=#{memory} $(hostname) && touch /etc/gridengineexecd.m_mem_free"
#   creates "/etc/gridengineexecd.m_mem_free"
#   only_if { ::File.exist?('/etc/gridengineexecd.installed') }
#   not_if { ::File.exist?('/etc/gridengineexecd.m_mem_free') }
# end

# Is this pidfile_running check actually working? I see the file, but I don't see the debug logs
service 'sgeexecd' do
  action [:start]
  only_if { ::File.exist?('/etc/gridengineexecd.installed') }
  not_if { pidfile_running? ::File.join(gridengineroot, 'default', 'spool', node[:hostname], 'execd.pid') }
end

