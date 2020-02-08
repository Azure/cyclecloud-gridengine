#
# Cookbook Name:: gridengine
# Recipe:: _install
#
# For installing GridEngine on master and compute nodes.
#

gridenginemake = node[:gridengine][:make]     # ge, sge
gridenginever = node[:gridengine][:version]   # 8.2.0-demo (ge), 8.2.1 (ge), 6_2u6 (sge), 2011.11 (sge, 8.1.8 (sge)
sgeroot = node[:gridengine][:root] 

# This is a hash for the gridengine install files "gridenginebins"
# The default is the official release from Univa. These are named "bin-lx-amd64" and "common"
# The binaries from the old SGE distribution were named "64" and "common"
gridenginebins = { 'sge' => %w[64 common] }
gridenginebins.default = %w[bin-lx-amd64 common] 

gridengineext = node[:gridengine][:package_extension]

# Cycle's SGE packages end with the tgz extension
if node[:gridengine][:make] == "sge"
  gridengineext = "tgz"
end

gridenginebins[gridenginemake].each do |arch|
  jetpack_download "#{gridenginemake}-#{gridenginever}-#{arch}.#{gridengineext}" do
    project "gridengine"
  end
end

execute "untarcommon" do
  command "tar -xf #{node[:jetpack][:downloads]}/#{gridenginemake}-#{gridenginever}-common.#{gridengineext} -C #{sgeroot}"
  creates "#{sgeroot}/inst_gridengine"
  action :run
end

gridenginebins[gridenginemake][0..-2].each do |myarch|

  execute "untar #{gridenginemake}-#{gridenginever}-#{myarch}.#{gridengineext}" do
    command "tar -xf #{node[:jetpack][:downloads]}/#{gridenginemake}-#{gridenginever}-#{myarch}.#{gridengineext} -C #{sgeroot}"
    case gridenginemake
    when "ge"
      #strip_bin essentially extracts "lx-amd64" from the filename.
      strip_bin = myarch.slice!(4..-1)
      creates "#{sgeroot}/bin/#{strip_bin}"
    when "gridengine"
      strip_bin = myarch.slice!(-3..-1)
      case strip_bin
      when "64"
        creates "#{sgeroot}/bin/linux-x64"
      when "32"
        creates "#{sgeroot}/bin/linux-x86"
      end
    end
    action :run
  end

end
