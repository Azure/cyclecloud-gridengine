#
# Cookbook Name:: gridengine
# Recipe:: scheduler
#
# The SGE Scheduler is a Scheduler and a Submitter
#

chefstate = node[:cyclecloud][:chefstate]

directory "#{node[:cyclecloud][:bootstrap]}/gridengine"
directory "/opt/cycle/gridengine"

slot_type = node[:gridengine][:slot_type] || "scheduler"

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
  # Install EPEL for jemalloc
  package 'epel-release'

  # jemalloc depends on EPEL
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

execute "installqm" do
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

template "#{gridengineroot}/conf/gridengine.q" do
  source "gridengine.q.erb"
  owner "root"
  group "root"
  mode "0755"
  variables(
    :gridengineroot => gridengineroot
  )
end

remote_directory '#{gridengineroot}/hooks' do
  source 'hooks'
  owner 'root'
  group 'root'
  mode '0755'
  action :create
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


if node[:gridengine][:make]=='ge'
  # UGE can change complexes between releases
  %w( slot_type onsched placement_group exclusive nodearray ).each do |confFile|
    cookbook_file "#{gridengineroot}/conf/complex_#{confFile}" do
      source "conf/complex_#{confFile}"
      owner "root"
      group "root"
      mode "0755"
      only_if "test -d #{gridengineroot}/conf"
    end

    execute "set #{confFile} complex" do
      command ". /etc/cluster-setup.sh && qconf -Ace #{gridengineroot}/conf/complex_#{confFile} && touch #{chefstate}/gridengine.setcomplex.#{confFile}.done"
      creates "#{chefstate}/gridengine.setcomplex.#{confFile}.done"
      action :run
    end
  end
elsif node[:gridengine][:make]=='sge'
  # OGS does't have qconf -Ace options 
  complex_file = "conf/complexes"

  cookbook_file "#{gridengineroot}/conf/complexes" do
    source complex_file
    owner "root"
    group "root"
    mode "0755"
    action :create
  end

  execute "set complexes" do
    command ". /etc/cluster-setup.sh && qconf -Mc #{gridengineroot}/conf/complexes && touch #{chefstate}/gridengine.setcomplexes.done"
    creates "#{chefstate}/gridengine.setcomplexes.done"
    action :run
  end
end

cookbook_file "#{gridengineroot}/conf/pecfg" do
  source "conf/pecfg"
  owner "root"
  group "root"
  mode "0755"
  action :create
end

pe_list = [ "make", "mpi", "mpislots", "smpslots"]

file "#{gridengineroot}/conf/pecfg" do
  content "pe_list #{pe_list.join(' ')}"
  mode "0755"
end

pe_list.each do |confFile|

  template "#{gridengineroot}/conf/#{confFile}" do
    source "#{confFile}.erb"
    owner "root"
    group "root"
    mode "0755"
    variables(
      :gridengineroot => gridengineroot
    )
  end 

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


relevant_complexes = node[:gridengine][:relevant_complexes] || ["slots", "slot_type"]
relevant_complexes_str = relevant_complexes.join(",")

cookbook_file "/opt/cycle/gridengine/logging.conf" do
  source "conf/logging.conf"
  owner 'root'
  group 'root'
  mode '0644'
  action :create
  not_if {::File.exists?("#{node[:cyclecloud][:bootstrap]}/gridengine/logging.conf")}
end

bash 'setup virtualenv' do
  code <<-EOH
  set -e
  source /etc/profile.d/sgesettings.sh
  jetpack download cyclecloud-gridengine-pkg-2.0.0.tar.gz --project gridengine #{node[:cyclecloud][:bootstrap]}/

  cd #{node[:cyclecloud][:bootstrap]}/
  tar xzf cyclecloud-gridengine-pkg-2.0.0.tar.gz
  cd cyclecloud-gridengine/
  INSTALLDIR=/opt/cycle/gridengine
  mkdir -p $INSTALLDIR/venv
  ./install.sh --install-python3 --venv $INSTALLDIR/venv
  azge initconfig --cluster-name #{node[:cyclecloud][:cluster][:name]} \
                  --username     #{node[:cyclecloud][:config][:username]} \
                  --password     #{node[:cyclecloud][:config][:password]} \
                  --url          #{node[:cyclecloud][:config][:web_server]} \
                  --lock-file    $INSTALLDIR/scalelib.lock \
                  --log-config   $INSTALLDIR/logging.conf \
                  --default-resource '{"select": {}, "name": "slots", "value": "node.vcpu_count"}' \
                  --idle-timeout #{node[:gridengine][:idle_timeout]} \
                  --relevant-complexes #{relevant_complexes_str} > $INSTALLDIR/autoscale.json

  # properly create our queues.
  azge create_queues -c $INSTALLDIR/autoscale.json
  touch #{node[:cyclecloud][:bootstrap]}/gridenginevenv.installed
  EOH
  action :run
  not_if {::File.exist?("#{node[:cyclecloud][:bootstrap]}/gridenginevenv.installed")}
end

include_recipe "gridengine::autostart"