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
  not_if "grep #{node[:gridengine][:user][:name]} /etc/passwd --quiet"
end

sgeroot = node[:gridengine][:root]

link "/etc/profile.d/sgesettings.sh" do
  to "#{sgeroot}/default/common/settings.sh"
end

link "/etc/profile.d/sgesettings.csh" do
  to "#{sgeroot}/default/common/settings.csh"
end

link "/etc/cluster-setup.sh" do
  to "#{sgeroot}/default/common/settings.sh"
end

link "/etc/cluster-setup.csh" do
  to "#{sgeroot}/default/common/settings.csh"
end

# In environments where the hostname changes because of a upstream recipe in the
# run list, using the lazy evaluation to delay assigning the hostname to the
# execute phase, by which time Ohai would have updated the node's hostname
execute "needauth" do
  command lazy {"touch #{sgeroot}/host_tokens/needauth/#{node[:hostname]}"}
  creates lazy {"#{sgeroot}/host_tokens/hasauth/#{node[:hostname]}"}
  action :run
end
