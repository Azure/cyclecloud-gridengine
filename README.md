# CycleCloud GridEngine Project

This project contains the GridEngine [cluster template file](templates/gridengine.txt).
The other GridEngine artifacts, such as install scripts, are contained in the
CycleCloud product.  

## Using Univa Grid Engine in CycleCloud

CycleCloud project for GridEngine uses _sge-2011.11_ by default. You may use your own
[Univa GridEngine](http://www.univa.com/products/) installers according to your Univa license agreement.  
This section documents how to use Univa GridEngine with the CycleCloud GridEngine project.

### Prerequisites

This example will use the 8.6.1-demo version, but all ge versions > 8.4.0 are supported.

1. Users must provide UGE binaries

  * ge-8.6.1-demo-bin-lx-amd64.tar.gz
  * ge-8.6.1-demo-common.tar.gz

2. The cyclecloud cli must be configured. Documentation is available [here](https://docs.microsoft.com/en-us/azure/cyclecloud/install-cyclecloud-cli) 


### Copy the binaries into the cloud locker

Using the CLI, determine the url of the cloud locker for your installation, then
use the `pogo put` command (installed with cyclecloud cli) to upload the installers to the locker.

```bash
$ cyclecloud locker list
azure-storage (az://myprojectwestus2/cyclecloud)
$ pogo put ge-8.6.1-demo-bin-lx-amd64.tar.gz az://myprojectwestus2/cyclecloud/cache/projects/gridengine/blobs/
Copied: az://myprojectwestus2/cyclecloud/cache/projects/gridengine/blobs/ge-8.6.1-demo-bin-lx-amd64.tar.gz 
Processed:   1 | Transferred:   1 | Deleted:   0 | Failures:   0 | Average rate: 5.43 MBps  
$ pogo put ge-8.6.1-demo-common.tar.gz az://myprojectwestus2/cyclecloud/cache/projects/gridengine/blobs/
Copied: az://myprojectwestus2/cyclecloud/cache/projects/gridengine/blobs/ge-8.6.1-demo-common.tar.gz
Processed:   1 | Transferred:   1 | Deleted:   0 | Failures:   0 | Average rate: 6.04 MBps  
```
### Add UGE configs to the cluster template

Make a local copy of the gridengine template and modify it to use the UGE installers
instead of the default.

```bash
wget https://raw.githubusercontent.com/Azure/cyclecloud-gridengine/master/templates/gridengine.txt
```

In the _gridengine.txt_ file, locate the first occurrence of `[[[configuration]]]` and
insert text such that it matches the snippet below.  This file is not sensitive to 
indentation.

> NOTE:
> The details in the configuration, particularly version, should match the installer file name.

```ini
[[[configuration]]]
    gridengine.make = ge
    gridengine.version = 8.6.1-demo
    gridengine.root = /sched/ge/ge-8.6.1-demo
```

These configs will override the default gridengine version and installation location, as the cluster starts.  
It is not safe to move off of the `/sched` as it's a specifically shared nfs location in the cluster.

### Import the cluster template file

Using the cyclecloud cli, import a cluster template from the new cluster template file.

```bash
cyclecloud import_cluster UGE -c 'grid engine' -f gridengine.txt -t
```

Similar to this [tutorial](https://docs.microsoft.com/en-us/azure/cyclecloud/tutorials/modify-cluster-template) in the documentation, new UGE cluster type is now available in the *Create Cluster* menu in the UI.

Configure and create the cluster in the UI, save it, and start it.

### Verify GridEngine version

As an example, the cluster that has been configured and started is called "test-uge".
When the master node reaches the _Started_ state (green), log into the node with 
the `cyclecloud connect` command.

```bash
cyclecloud connect master -c test-uge`
Last login: Tue Jan 29 20:37:14 2019 

 __        __  |    ___       __  |    __         __|
(___ (__| (___ |_, (__/_     (___ |_, (__) (__(_ (__|
        |

Cluster: test-uge
Version: 7.6.2
```

Then check the grid engine version with `qstat`


```bash
qstat -h
UGE 8.6.1
usage: qstat [options]
```

## Configuring Resources

### Important Files

| Description  |  Location |
|---|---|
| Autoscale Config  | /opt/cycle/gridengine/autoscale.json  |
| Autoscale Log  | /opt/cycle/jetpack/logs/autoscale.log  |
| qconf trace log  | /opt/cycle/jetpack/logs/qcmd.log  |

### Autoscale config

This plugin will automatically scale the grid to meet the demands of the workload. 
The _autoscale.json_ config file determines the behavior of the Grid Engine autoscaler.

* Set the cyclecloud connection details
* Set the termination timer for idle nodes
* Multi-dimensional autoscaling is possible, set which attributes to use in the job packing e.g. slots, memory
* Register the queues, parellel environments and hostgroups to be managed

In the section `gridengine.queues` we set the heirarchy for the
Grid Engine queues, PEs and hostgroups. These resources and their
relationships must already exist. Here, the hostgroup `@hpc.q_mpi01` must exist and be linked in the queue conf.

Create the objects:
```bash
qconf -Ahgrp hpc.q_mpi01  # add a hostgroup
qconf -Ape mpi01          # add a pe
qconf -mq hpc.q           # update the queue
```

Configure the linkage in the queue definition:
```ini
pe_list               NONE,[@hpc.q_mpi01=mpi01], \
                      [@hpc.q_mpi02=mpi02]
```

Add the linkage to _autoscale.json_
```json
    "queues": {
      "hpc.q": {
        "constraints": [
          {
            "node.nodearray": "hpc"
          }
        ],
        "hostlist": "NONE",
        "pes": {
          "mpi01": {
            "hostgroups": [
              "@hpc.q_mpi01"
            ]
          },
          "mpi02": {
            "hostgroups": [
              "@hpc.q_mpi02"
            ]
          },
```
Cyclecloud is doing an overlay of hostgroup to nodes within a
SinglePlacementGroup - for HPC VMs in the _hpc_ nodearray this means that they're connected to the same IB fabric. Hosts in the _@hpc.q_mpi01_ hostgroup with be accessed by the _mpi01_ parallel environment in the _hpc.q_. 

There are limits to how many VMs can be added to the same IB fabric.
So to allow for overflow, we add additional PEs and hostgroups. You can then let your jobs spill over by submitting
with a wildcard.

```bash
qsub -q hpc.q -pe mpi\* my-script.sh
```

| Configuration | Type  | Description  |
|---|---|---|
| url  | String  | CC URL  |
| username/password  | String  | CC Connection Details  |
| cluster_name  | String  | CC Cluster Name  |
| default_resources  | Map  | Link a node resource to a Grid Engine host resource for autoscale  |
| gridengine.idle_timeout  | Int  | Wait time before terminating idle nodes (s)  |
| gridengine.relevant_complexes  | List (String)  | Grid engine complexes to consider in autoscaling e.g. slots, mem_free  |
| gridengine.queues  | Map  | Managed queue/pe/hostgroup heirarchy, See above example  |
| gridengine.logging | File | Location of logging config file |

#### Additional autoscaling resource

By default, the cluster with scale based on how many slots are
requested by the jobs. We can add another dimension to autoscaling. 

Let's say we want to autoscale by the job resource request for `mem_free`.

1. Add `mem_free` to the `gridengine.relevant_resources` in _autoscale.json_
2. Link `mem_free` to the node-level memory resource in _autoscale.json_

```json
    "default_resources": [
    {
      "select": {},
      "name": "slots",
      "value": "node.vcpu_count"
    },
    {
      "select": {},
      "name": "mem_free",
      "value": "node.memmb"
    },
  ],

# Contributing

This project welcomes contributions and suggestions.  Most contributions require you to agree to a
Contributor License Agreement (CLA) declaring that you have the right to, and actually do, grant us
the rights to use your contribution. For details, visit https://cla.microsoft.com.

When you submit a pull request, a CLA-bot will automatically determine whether you need to provide
a CLA and decorate the PR appropriately (e.g., label, comment). Simply follow the instructions
provided by the bot. You will only need to do this once across all repos using our CLA.

This project has adopted the [Microsoft Open Source Code of Conduct](https://opensource.microsoft.com/codeofconduct/).
For more information see the [Code of Conduct FAQ](https://opensource.microsoft.com/codeofconduct/faq/) or
contact [opencode@microsoft.com](mailto:opencode@microsoft.com) with any additional questions or comments.
