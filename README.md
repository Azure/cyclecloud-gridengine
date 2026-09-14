# CycleCloud GridEngine Project

This project contains the GridEngine [cluster template file](templates/gridengine.txt).
The other GridEngine artifacts, such as install scripts, are contained in the
CycleCloud product.  


## Configuring Resources
The cyclecloud-gridengine application matches sge resources to azure cloud resources 
to provide rich autoscaling and cluster configuration tools. The application will be deployed
automatically for clusters created via the CycleCloud UI or it can be installed on any 
gridengine admin host on an existing cluster.

### Installing or Upgrading cyclecloud-gridengine

The cyclecloud-gridengine bundle will be available in [github](https://github.com/Azure/cyclecloud-gridengine/releases) 
as a release artifact. Installing and upgrading will be the same process. 
The application requires python3 with virtualenv.

```bash
tar xzf cyclecloud-gridengine-pkg-*.tar.gz
cd cyclecloud-gridengine
./install.sh
./generate_autoscale_json.sh --username USER --password PASS --cluster-name CLUSTER --url https://cyclecloud-address:port
```

### Important Files

The application parses the sge configuration each time it's called - jobs, queues, complexes.
Information is provided in the stderr and stdout of the command as well as to a log file, both
at configurable levels. All gridengine management commands with arguments are logged to file as well.

| Description  |  Location |
|---|---|
| Autoscale Config  | /opt/cycle/gridengine/autoscale.json  |
| Autoscale Log  | /opt/cycle/jetpack/logs/autoscale.log  |
| qconf trace log  | /opt/cycle/jetpack/logs/qcmd.log  |


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
seq_no                10000,[@lowprio=10],[@mpihg01=100],[@mpihg02=200]
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

### CycleCloud Placement Groups
CycleCloud placement groups map one-to-one to Azure VMSS with SinglePlacementGroup - VMs in a placementgroup
share an Infiniband Fabric and share only with VMs within the placement group. 
To intuitively preserve these silos, the placementgroups map 1:1 with gridengine parallel environment 
as well.

Specifying a parallel environment for a job will restrict the job to run in a placement group via smart 
hostgroup assignment logic. You can opt out of this behavior with the aforementioned configuration in 
_autoscale.json_ : `"required_placement_groups" : false`.

### Autoscale config

This plugin will automatically scale the grid to meet the demands of the workload. 
The _autoscale.json_ config file determines the behavior of the Grid Engine autoscaler.

* Set the cyclecloud connection details
* Set the termination timer for idle nodes
* Multi-dimensional autoscaling is possible, set which attributes to use in the job packing e.g. slots, memory
* Register the queues, parallel environments and hostgroups to be managed


| Configuration | Type  | Description  |
|---|---|---|
| url  | String  | CC URL  |
| username/password  | String  | CC Connection Details  |
| cluster_name  | String  | CC Cluster Name  |
| default_resources  | Map  | Link a node resource to a Grid Engine host resource for autoscale  |
| idle_timeout  | Int  | Wait time before terminating idle nodes (s)  |
| boot_timeout  | Int  | Wait time before terminating nodes during long configuration phases (s)  |
| gridengine.relevant_complexes  | List (String)  | Grid engine complexes to consider in autoscaling e.g. slots, mem_free  |
| gridengine.logging | File | Location of logging config file |
| gridengine.pes | Struct | Specify behavior of PEs, e.g. _requires\_placement\_group = false_ |

The autoscaling program will only consider *Relevant Resource*

### Additional autoscaling resource

By default, the cluster with scale based on how many slots are
requested by the jobs. We can add another dimension to autoscaling. 

Let's say we want to autoscale by the job resource request for `m_mem_free`.

1. Add `m_mem_free` to the `gridengine.relevant_resources` in _autoscale.json_
2. Link `m_mem_free` to the node-level memory resource in _autoscale.json_

These attributes can be references with `node.*` as the _value_ in _default/_resources_.

| Node  | Type  | Description  |
|---|---|---|
nodearray | String | Name of the cyclecloud nodearray
placement_group |  String | Name of the cyclecloud placement group within a nodearray
vm_size | String | VM product name, e.g. "Standard_F2s_v2"
vcpu_count | Int | Virtual CPUs available on the node as indicated on individual product pages
pcpu_count | Int | Physical CPUs available on the node
memory | String | Approximate physical memory available in the VM with unit indicator, e.g. "8.0g"

Additional attributes are in the `node.resources.*` namespace, e.g. `node.resources.

| Node  | Type  | Description  |
|---|---|---|
ncpus | String | Number of CPUs available in in the VM 
pcpus |  String | Number of physical CPUs available in the VM
ngpus | Integer | Number of GPUs available in the VM
memb  | String | Approximate physical memory available in the VM with unit indicator, e.g. "8.0b"
memkb | String | Approximate physical memory available in the VM with unit indicator, e.g. "8.0k"
memmb | String | Approximate physical memory available in the VM with unit indicator, e.g. "8.0m"
memgb | String | Approximate physical memory available in the VM with unit indicator, e.g. "8.0g"
memtb | String | Approximate physical memory available in the VM with unit indicator, e.g. "8.0t"
slots | Integer | Same as ncpus
slot_type | String | Addition label for extensions. Not generally used.
m_mem_free | String | Expected free memory on the execution host, e.g. "3.0g"
mfree | String | Same as _m/_mem/_free_

### Resource Mapping

There are also maths available to the default_resources - reduce the slots on a particular 
node array by two and add the docker resource to all nodes:
```json
    "default_resources": [
    {
      "select": {"node.nodearray": "beegfs"},
      "name": "slots",
      "value": "node.vcpu_count",
      "subtract": 2
    },
    {
      "select": {},
      "name": "docker",
      "value": true
    },
```

Mapping the node vCPUs to the slots complex, and memmb to mem_free are commonly used defaults.
The first association is required.

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
      "value": "node.resources.memmb"
    }
 ],
```

Note that if a complex has a shortcut not equal to the entire value, then define both in _default\_resources_
where `physical_cpu` is the complex name:
```json
"default_resources": [
    {
      "select": {},
      "name": "physical_cpu",
      "value": "node.pcpu_count"
    },
    {
      "select": {},
      "name": "pcpu",
      "value": "node.resources.physical_cpu"
    }
]
```

Ordering is also important when you want a particular behavior for a specific attribute, like making sure
only one slot will be allocate per node on a selected nodearray:

```json
    "default_resources": [
    {
      "select": {"node.nodearray": "FPGA"},
      "name": "slots",
      "value": "1",
    },
    {
      "select": {},
      "name": "slots",
      "value": "node.vcpu_count"
    },
]
```

## Hostgroups

The CycleCloud autoscaler, in attempting to satisfy job requirements, will map nodes to
the appropriate hostgroup. Queues, parallel environments and complexes are all considered.
Much of the logic is matching the appropriate cyclecloud bucket (and node quantity) with
the appropriate sge hostgroup. 

For a job submitted as:
`qsub -q "cloud.q" -l "m_mem_free=4g" -pe "mpi*" 48 ./myjob.sh`

Cyclecloud will find get the intersection of hostgroups which:
1. Are included in the _pe\_list_ for _cloud.q_ and match the pe name, e.g. `pe_list [@allhosts=mpislots],[@hpc1=mpi]`.
1. Have adequate resources and subscription quota to provide all job resources.
1. Are not filtered by the hostgroup constraints configuration.

It's possible that multiple hostgroups will meet these requirements, in which case
the logic will need to choose. There are three ways to resolve ambiguities in hostgroup
membership:
1. Configure the queues so that there aren't ambiguities. 
1. Add constraints to _autoscale.json_.
1. Let cyclecloud choose amoungst the matching hostgroups in a name-ordered fashion by adjusting `weight_queue_host_sort < weight_queue_seqno` in the scheduler configuration.
1. Set `seq_no 10000,[@hostgroup1=100],[@hostgroup2=200]` in the queue configuration to indicate a hostgroup preference.


### Hostgroup contstraints

When multiple hostgroups are defined by a queue or xproject then all these hostgroups can potentially have
the hosts added to them. You can limit what kinds of hosts can be added to which queues by setting hostgroup
constraints. Set a constraint based on the node properties. 

```json
"gridengine": {
    "hostgroups": {
      "@mpi": {
        "constraints": {
          "node.vm_size": "Standard_H44rs"
        }
      },
      "@amd-mem": {
        "constraints" : { 
            "node.vm_size": "Standard_D2_v3",
            "node.nodearray": "hpc" 
            }
        },
    }
  }
  ```

> HINT:
> Inspect all the available node properties by `azge buckets`.

## azge
This package comes with a command-line, _azge_. This program should be used
to perform autoscaling and has broken out all the subprocesses under autoscale.
These commands rely on the gridengine environment variables to be set - you must be
able to call `qconf` and `qsub` from the same profile where `azge` is called.

| _azge_ commands  | Description  |
|---|---|
| validate | Checks for known configuration errors in the autoscaler or gridengine 
| jobs | Shows all jobs in the queue 
| buckets | Shows available resource pools for autoscaling 
| nodes | Shows cluster hosts and properties 
| demand | Matches job requirements to cyclecloud buckets and provides autoscale result 
| autoscale | Does full autoscale, starting and removing nodes according to configurations 

When modifying scheduler configurations (_qconf_) or autoscale configurations (_autoscale.json_), 
or even setting up for the first time, _azge_ can be used to check autoscale behavior is matching
expections. As root, you can run the following operations. It's advisable to get familiar with these
to understand the autoscale behavior.

1. Run `azge validate` to verify configurations for known issues.
1. Run `azge buckets` to examine what resources your CycleCloud cluster is offering.
1. Run `azge jobs` to inspect the queued job details.
1. Run `azge demand` perform the job to bucket matching, examine which jobs get matched to which buckets and hostgroups.
1. Run `azge autoscale` to kickoff the node allocation process, or add nodes which are ready to join.

Then, when these commands are behaving as expected, enable ongoing autoscale by adding the `azge autoscale`
command to the root crontab. (Souce the gridengine environment variables)

```cron
* * * * * . $SGE_ROOT/common/settings.sh && /usr/local/bin/azge autoscale -c /opt/cycle/gridengine/autoscale.json
```

## Creating a hybrid cluster

Cyclecloud will support the scenario of bursting to the cloud. The base configuration assumes that the `$SGE_ROOT`
directory is available to the cloud nodes. This assumption can be relaxed by setting `gridengine.shared.spool = false`, 
`gridengine.shared.bin = false` and installing GridEngine locally. 
For a simple case, you should provide a filesystem that can be mounted by the execute nodes which contains the `$SGE_ROOT` directory
and configure that mount in the optional settings. When the dependency of the sched and shared directories are released, you 
can shut down the scheduler node that is part of the cluster by-default and use the configurations 
from the external filesystem.

1. Create a new gridengine cluster
1. Disable return proxy
1. Replace /sched and /shared with external filesystems
1. Save the cluster
1. Remove the scheduler node
1. Configure cyclecloud-gridengine with _autoscale.json_ to use the new cluster

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
### Modifying configs to the cluster template

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
[[[configuration gridengine]]]
    make = ge
    version = 8.6.12-demo
    root = /sched/ge/ge-8.6.12-demo
    cell = "default"
    sge_qmaster_port = "537"
    sge_execd_port = "538"
    sge_cluster_name = "grid1"
    gid_range = "20000-20100"
    qmaster_spool_dir = "/sched/ge/ge-8.6.12-demo/default/spool/qmaster" 
    execd_spool_dir = "/sched/ge/ge-8.6.12-demo/default/spool"
    spooling_method = "berkeleydb"
    shadow_host = ""
    admin_mail = ""
    idle_timeout = 300

    managed_fs = true
    shared.bin = true

    ignore_fqdn = true
    group.name = "sgeadmin"
    group.gid = 536
    user.name = "sgeadmin"
    user.uid = 536
    user.gid = 536
    user.description = "SGE admin user"
    user.home = "/shared/home/sgeadmin"
    user.shell = "/bin/bash"

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
