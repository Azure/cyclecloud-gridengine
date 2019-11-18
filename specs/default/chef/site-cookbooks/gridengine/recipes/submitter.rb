#
# Cookbook Name:: gridengine
# Recipe:: submitter
#


group node[:gridengine][:group][:name] do
  gid node[:gridengine][:group][:gid]
  not_if "grep #{node[:gridengine][:group][:name]} /etc/group --quiet"
end

user node[:gridengine][:user][:name] do
  comment node[:gridengine][:user][:description]
  uid node[:gridengine][:user][:uid]
  gid node[:gridengine][:user][:gid]
  home node[:gridengine][:user][:home]
  shell node[:gridengine][:user][:shell]
  not_if "grep #{node[:gridengine][:user][:name]}  /etc/passwd --quiet"
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
