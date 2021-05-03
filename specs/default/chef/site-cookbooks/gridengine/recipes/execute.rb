#
# Cookbook Name:: gridengine
# Recipe:: sgeexec
#

include_recipe "gridengine::sgefs" if node[:gridengine][:managed_fs]
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
    owner node[:gridengine][:user][:uid]
    group node[:gridengine][:user][:gid]
    mode "0755"
    action :create
    recursive true
  end
  
  include_recipe "::_install"
end


myplatform=node[:platform]
myplatform = "centos" if node[:platform_family] == "rhel" # TODO: fix this hack for redhat
nodename = node[:cyclecloud][:instance][:hostname]
nodename_short = nodename.split(".")[0]

# default case systemd
sge_services = ["sgeexecd", "sgemasterd"]
sge_service_names = []
sge_services.each do |sge_service|
  sge_service_template="#{sge_service}.service.erb"
  sge_service_name="#{sge_service}.service"
  sge_service_initfile="/etc/systemd/system/#{sge_service_name}"
  # edge case sysvinit
  case node['platform_family']
  when 'rhel'
    if node['platform_version'].to_i <= 6
      sge_service_template="#{sge_service}.erb"
      sge_service_name=sge_service
      sge_service_initfile="/etc/init.d/#{sge_service}"
    end
  end

  template sge_service_initfile do
    source sge_service_template
    mode 0755
    owner "root"
    group "root"
    variables(
      :gridengineroot => gridengineroot,
      :gridenginecell => gridenginecell
    )
  end
  sge_service_names.push(sge_service_name)
end

sge_execd_service = sge_service_names[0]
sge_qmasterd_service = sge_service_names[1]


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
      :nodename => node[:hostname],
      :gridengineclustername => node[:gridengine][:sge_cluster_name],
      :gridenginecell => gridenginecell,
      :gridengine_gid_range => node[:gridengine][:gid_range],
      :gridengine_admin_mail => node[:gridengine][:admin_mail],
      :gridengine_shadow_host => node[:gridengine][:shadow_host],
      :execd_spool_dir => node[:gridengine][:execd_spool_dir],
      :qmaster_spool_dir => node[:gridengine][:qmaster_spool_dir],
      :gridengine_spooling_method => node[:gridengine][:spooling_method],
      :gridengine_qmaster_port => node[:gridengine][:sge_qmaster_port],
      :gridengine_execd_port => node[:gridengine][:sge_execd_port]
    }
  }
end

# force node.json to be rewritten, then trigger a jetpack log message to update the hostname
execute "trigger dump node.json" do
  command "test 0"
  notifies :run, "ruby_block[dump node.json]", :before
  notifies :run, "execute[update_hostname]", :immediately
end

# make sure the hostname in CycleCloud is updated, so that the autoscaler can authorize us.
execute "update_hostname" do
  command "jetpack log 'updating hostname for '$(hostname) -p low"
  action :nothing
end

# this block prevents "bash[install_gridengine_execd]" from running until the hostname is authorized
ruby_block "gridengine exec authorized?" do
  block do
    cmd =  Mixlib::ShellOut.new(
      "source /etc/profile.d/sgesettings.sh  && qconf -se #{node[:cyclecloud][:instance][:hostname]} > /dev/null"
      ).run_command
   
    raise "gridengine node #{node[:cyclecloud][:instance][:hostname]} not authorized yet" unless cmd.exitstatus == 0
  end
  retries 5
  retry_delay 30
  notifies :run, "bash[install_gridengine_execd]", :delayed
end

# this starts the sge_execd process as well - also requires host to be authorized 
bash "install_gridengine_execd" do
  code <<-EOF
  set -x
  if [ ! -e /etc/profile.d/sgesettings.sh ]; then
    echo Waiting for scheduler;
    exit 1;
  fi
  source /etc/profile.d/sgesettings.sh;
  which qconf > /dev/null || exit 1;
  qconf -se #{node[:cyclecloud][:instance][:hostname]} > /dev/null
  if [ $? != 0 ]; then
    echo #{node[:cyclecloud][:instance][:hostname]} is not authorized to join the cluster yet. RDH2
    exit 2
  fi

  cd $SGE_ROOT || exit 1;
  ./inst_sge -x -noremote -auto #{Chef::Config['file_cache_path']}/compnode.conf
  if [ $? == 0 ]; then
    touch /etc/gridengineexecd.installed
    exit 0
  fi
  
  # install_file=$(ls -t #{gridengineroot}/#{gridenginecell}/common/install_logs/*#{nodename_short}*.log | head -n 1)
  install_file=$(ls -t /tmp/install.* | grep -E 'install\.[0-9]+' | head -n 1)
  if [ ! -e $install_file ]; then
    echo There is no install log file 1>&2
    exit 1
  fi
  echo Here are the contents of $install_file 1>&2
  cat $install_file >&2
  exit 1
  EOF
  creates "/etc/gridengineexecd.installed"
  action :nothing
  notifies :enable, "service[#{sge_execd_service}]", :immediately
  
end


# Is this pidfile_running check actually working? I see the file, but I don't see the debug logs
service sge_execd_service do
  action [:nothing]
end

