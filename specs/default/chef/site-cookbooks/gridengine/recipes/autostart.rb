#
# Cookbook Name:: gridengine
# Recipe:: autostart
#


cron "autostart" do
    command "#{node[:cyclecloud][:bootstrap]}/cron_wrapper.sh #{node[:cyclecloud][:bootstrap]}/gridengine/gridengine_autoscale.sh"
    only_if { node['cyclecloud']['cluster']['autoscale']['start_enabled'] }
end
