Description
====

This cookbook installs Grid Engine.  It has two roles, master and execute.  It depends on the cshared cookbook.

# This cookbook supports several versions of Grid Engine, including OGS [Open Grid Scheduler](http://gridscheduler.sourceforge.net/)), SGE ([Son of Grid Engine](https://arc.liv.ac.uk/trac/SGE)) and UGE ([Univa Grid Engine](http://www.univa.com/products/))

Requirements
====


Usage
====

### Suported Versions

There are two chef attributes which control which version of Grid Engine is installed.  These are `gridengine.make` and `gridengine.version`.  `make` is meant to mean the vendor, and `version` is meant to mean the release.

`make` can either be `sge` for the case of OGS or SGE, where `make` should be `ge` for UGE.  

1. `gridengine.make = sge`
  * `version = 2011.11` (OGS 2011.11, default)
  * `version = 8.1.8` (SGE 8.1.8)
2. `gridengine.make = ge`
  * `version = 8.2.0-demo` (GE 8.2.0 demo version, 48 core max)
  * `version = 8.2.1` (GE 8.2.1, where permitted)

These configurations should be set **on all hosts in the cluster**.

### SGE_ROOT

It's conventional to change the `SGE_ROOT` to correspond to the version used.  This may be done automatically in the future.  `SGE_ROOT` is controlled by the chef parameter `gridengine.root`

Typical values may be:

`gridengine.root = /sched/sge/sge-2011.11/` for the default version.

or

`gridengine.root = /sched/ge/ge-8.2.0-demo/` for UGE 8.2.0 demo version.

### Shared or Local

As mentioned, this cookbook depends on the cshared (nfs) cookbook.  The default install shares `SGE_ROOT` as an nfs share to all hosts in the cluster.  By default, all tools are shared by nfs.  This sharing is not a strict requirement of SGE and may, in fact, be eliminated ([sge-nfs](http://gridscheduler.sourceforge.net/howto/nfsreduce.html)).  While sharing the entire `SGE_ROOT` can be convenient for debugging, there are compatibility and performance reasons not share all of `SGE_ROOT`.

For compatibility reasons, we allow for making the *sge binaries* a local resource.  For performance reasons, we allow for making the *sge spool directory* a local resource.  

Sharing all sge resources is done by default, there are a few chef configurations that need to be set allow for some resources to be local.

Firstly, `gridengine.root` and `cshared.client.defaults.sched.mountpoint` must not share a common root directory.  Otherwise, the `cshared.client` will mount `SGE_ROOT` directly.  So to enable local resources in SGE, set these to different values.  

An example would be:

`gridengine.root = /sched/sge/sge-8.1.8` 

combined with

`cshared.client.defaults.sched.mountpoint = /sge-mnt`

Then selective symlinks from the SGE shared directory to the local `SGE_ROOT` can take place.  If `gridengine.root` and `cshared.client.defaults.sched.mountpoint` coincide at the first dir level, all local resource configs will be ignored.

Once these are set, then you can choose:

* `gridengine.shared.bin = false` to use local binaries (shared is default)
* `gridengine.shared.spool = false` to spool jobs locally (shared is default) 

     
Platform
----

Tested on:

Cookbooks
----


Resources and Providers
====

TODO

Attributes
====
