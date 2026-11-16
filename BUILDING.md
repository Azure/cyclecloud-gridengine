# Developing and Testing CycleCloud GridEngine

Build your changes, upload the project to a CycleCloud storage locker, and create
a test cluster from a custom template.

## Prerequisites

* A configured [Azure CycleCloud CLI](https://learn.microsoft.com/en-us/azure/cyclecloud/how-to/install-cyclecloud-cli).
* Linux (including WSL), Bash, and a running Docker Engine for the Docker build.
  Confirm `docker info` succeeds.
* Internet access to download the container image, packages, and dependencies.

Run the following commands from the repository root.

## Build the Bundle

To build the release artifacts locally with Docker:

```bash
./docker-package.sh
```

The script builds the package and open-source Grid Engine archives and places
them in `blobs/`. It does not publish or upload anything.

To use a local scalelib archive instead of downloading it:

```bash
./docker-package.sh --scalelib ../cyclecloud-scalelib/dist/cyclecloud-scalelib-1.0.12.tar.gz
```

### Package without Docker

Run `python package.py` from the project checkout. The packager downloads scalelib
`1.0.12` from GitHub and extracts API wheel `8.9.3` from the official Microsoft
CycleCloud DEB `8.9.3-3874`. This requires `curl` and `dpkg-deb` (provided by `dpkg`
on Debian/Ubuntu). The DEB is not installed: only the matching regular wheel file
is extracted, and its package name and version are checked before use. Temporary
download and extraction files are removed on success or failure. Allow roughly
500 MB of temporary disk space for the DEB download, in addition to build outputs.

Supply local dependencies to bypass their downloads:

```bash
python package.py --scalelib /path/to/cyclecloud-scalelib-1.0.12.tar.gz \
  --cyclecloud-api /path/to/cyclecloud_api-8.9.3-py2.py3-none-any.whl
```

The `--cyclecloud-api` override does not require `dpkg-deb`. Other Python package
dependencies may still be downloaded during the build. When updating the API
version, also update `CYCLECLOUD_DEB_VERSION` in `package.py` to a corresponding
official package build.

Use the Docker build above to prepare all project blobs for the upload workflow
below.

## Upload to Your Storage Locker

List the available lockers:

```bash
cyclecloud locker list
```

Upload your modified project and the built blobs:

```bash
cyclecloud project upload "LOCKER_NAME"
```

Replace `LOCKER_NAME` with the name of your locker.

## Test Your Changes

### Edit your cluster template

Update a copy of the Grid Engine template to use your uploaded project:

```bash
cp templates/gridengine.txt templates/gridengine-test.txt
sed -i -e 's/\(\[*cluster-init[^]]*\)\]/\1:RELEASE_VERSION]/' -e 's/cyclecloud\/gridengine/gridengine/g' templates/gridengine-test.txt
```

Replace `RELEASE_VERSION` in the command with the cyclecloud-gridengine release
version from `[project]` in `project.ini` (for example, `2.1.2`).

### Import the cluster template file

Using the CycleCloud CLI, import the modified file as a separate test cluster type:

```bash
cyclecloud import_cluster GRIDENGINE_PREVIEW -c 'Grid Engine' -f templates/gridengine-test.txt -t
```

Replace `GRIDENGINE_PREVIEW` with the desired name for your test cluster type.

In the CycleCloud UI, click **+** in the bottom-left corner, select
**GRIDENGINE_PREVIEW**, and configure, save, and start a new cluster to test your
changes.
