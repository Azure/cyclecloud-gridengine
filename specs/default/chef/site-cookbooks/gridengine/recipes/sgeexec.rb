#
# Cookbook Name:: gridengine
# Recipe:: sgeexec
#

Chef::Log.warn("This recipe has been decprecated. Please use gridengine::execute instead")
include_recipe "gridengine::execute"
