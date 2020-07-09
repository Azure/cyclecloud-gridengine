#
# Cookbook Name:: gridengine
# Recipe:: master
#
# The SGE Master is a Q-Master and a Submitter
#

Chef::Log.warn("This recipe has been decprecated. Please use gridengine::scheduler instead")
include_recipe "gridengine::scheduler"

