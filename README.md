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

  * ge-8.6.x-bin-lx-amd64.tar.gz
  * ge-8.6.x-common.tar.gz

2. The cyclecloud cli must be configured. Documentation is available [here](https://docs.microsoft.com/en-us/azure/cyclecloud/install-cyclecloud-cli) 


### Copy the binaries into the cloud locker

A complementary version of UGE (8.6.7-demo) is distributed with Cyclecloud. To use another version upload the
binaries to the storage account that CycleCloud uses.

```bash

$ azcopy cp ge-8.6.12-bin-lx-amd64.tar.gz https://<storage-account-name>.blob.core.windows.net/cyclecloud/gridengine/blobs/
$ azcopy cp ge-8.6.12-common.tar.gz https://<storage-account-name>.blob.core.windows.net/cyclecloud/gridengine/blobs/
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
    gridengine.version = 8.6.12-demo
    gridengine.root = /sched/ge/ge-8.6.12-demo
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

### SGE queues, hostgroups and parallel environments

The cyclecloud-gridengine autoscale utility, `azge`, will add hosts to the cluster according
to the cluster configuration. The autoscaling operations perform the following actions.

1. Read the job resource request and find an appropriate VM to start
1. Start the VM and wait for it to be ready
1. Read the queue and parallel environment from the job
1. Based on the queue/pe assign the host to an appropriate hostgroup
1. Add the host to the cluster as well as to any other queue containing the hostgroup

Consider the following queue definition for a queue named _short.q_
```ini
hostlist              @allhosts @mpihg01 @mpihg02 @lowprio 
...
pe_list               NONE,[@mpihg01=mpi01], \
                      [@mpihg02=mpi02]
```

Submitting a job by `qsub -q short.q -pe mpi02 12 my-script.sh` will start at lease one VM,
and when it's added to the cluster, it will join hostgroup _@mpihg02_ because that's the hostgroup
both available to the queue and to the parallel environment. It will also be added to _@allhosts_, which
is a special hostgroup.

Without specifying a pe, `qsub -q short.q my-script.sh` the resulting VM will be added to _@allhosts_ 
and _@lowpriority_ these are the hostgroups in the queue which aren't assigned pes.

Finally, a job submitted with `qsub -q short.q -pe mpi0* 12 my-script.sh` will result in a VM added to
either _@mpihg01_ or _@mpihg02_ depending on CycleCloud allocation predictions.

Parallel environments implicitly equate to cyclecloud placement group. VMs in a PE are constrained to
be within the same network. If you wish to use a PE that doesn't keep a placement group then use the
_autoscale.json_ to opt out.

Here we opt out of placement groups for the _make_ pe:

```json
"gridengine": {
    "pes": {
      "make": {
        "requires_placement_groups": false
      }
    },
```

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


| Configuration | Type  | Description  |
|---|---|---|
| url  | String  | CC URL  |
| username/password  | String  | CC Connection Details  |
| cluster_name  | String  | CC Cluster Name  |
| default_resources  | Map  | Link a node resource to a Grid Engine host resource for autoscale  |
| gridengine.idle_timeout  | Int  | Wait time before terminating idle nodes (s)  |
| gridengine.boot_timeout  | Int  | Wait time before terminating nodes during long configuration phases (s)  |
| gridengine.relevant_complexes  | List (String)  | Grid engine complexes to consider in autoscaling e.g. slots, mem_free  |
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
```


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
