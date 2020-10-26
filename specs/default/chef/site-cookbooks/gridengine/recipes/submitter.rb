#
# Cookbook Name:: gridengine
# Recipe:: submitter
#


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

gridengineroot = node[:gridengine][:root]

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
