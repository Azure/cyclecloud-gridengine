#
# Cookbook Name:: gridengine
# Recipe:: sgemaster
#
# The SGE Master is a Q-Master and a Submitter 
#

Chef::Log.warn("This recipe has been decprecated. Please use gridengine::master instead")
include_recipe "gridengine::master"

