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
