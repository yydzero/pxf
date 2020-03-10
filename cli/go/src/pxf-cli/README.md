# `pxf cluster` CLI

## Getting Started

1. Ensure you are set up for PXF development by following the README.md at the root of this repository. This tool requires Go version 1.9 or higher. Follow the directions [here](https://golang.org/doc/) to get the language set up.

1. Go to the pxf-cluster folder and install dependencies
   ```
   cd pxf/server/pxf-cli/go/src/pxf-cli
   go get github.com/golang/dep/cmd/dep
   go get github.com/onsi/ginkgo/ginkgo
   make depend
   ```

1. Run the tests
   ```
   make test
   ```

1. Build the CLI
   ```
   make
   ```
   This will put the binary at `pxf/server/pxf-cli/go/bin/pxf-cli`. You can also install the binary into `${PXF_HOME}/bin/pxf-cli` with:
   ```
   make install
   ```

## Adding New Dependencies

1. Import the dependency in some source file (otherwise `dep` will refuse to install it)

2. Add the dependency to Gopkg.toml

3. Run `make depend`.

## Updating deps

Normally `dep ensure -update -v` is enough (`-v` is verbose flag). Only run this command and commit the ensuing changes if you are ready to add update PXF deps (OSL).

Sometimes, you get an error like:

```
(1)         try github.com/greenplum-db/gp-common-go-libs@fix-nil-cluster.Executor
(1)     ✗   unable to update checked out version: fatal: reference is not a tree: 1f793c56cc19245e971e59b5bce42a4a1cfc28fe
```

when trying to e.g. use a custom version of `gp-common-go-libs`. One way to get around this is to do something like this:

```bash
cd $GOPATH/pkg/dep/sources/https---github.com-greenplum--db-gp--common--go--libs
git fetch --all --prune
```

then re-run the `dep ensure -update` command. Updating deps will break pipelines with similar errors, so re-running the [docker build](https://ud.ci.gpdb.pivotal.io/teams/main/pipelines/gpdb_pxf_docker-images/jobs/docker-gpdb-pxf-dev-centos6/builds/98) can help update the container's local git repositories.

## Debugging the CLI on a live system

Because it's hard to mock out a Greenplum cluster, it's useful to debug on a real live cluster. We can do this using the [`delve`](https://github.com/go-delve/delve) project.

1. Install `dlv` command, see [here](https://github.com/go-delve/delve/blob/master/Documentation/installation/linux/install.md) for more details:

```bash
go get -u github.com/go-delve/delve/cmd/dlv
```

2. Optionally create a file with a list of commands, this could be useful to save you from re-running commands. You can actually run them with the `source` command in the `dlv` REPL:

```
config max-string-len 1000
break vendor/github.com/greenplum-db/gp-common-go-libs/cluster/cluster.go:351
continue
print commandList
```

3. Run the `dlv` command to enter the interactive REPL:

```bash
cd ~/workspace/pxf/cli/go/src/pxf-cli
GPHOME=/usr/local/greenplum-db dlv debug pxf-cli -- cluster restart
```

The [help page for dlv](https://github.com/go-delve/delve/tree/master/Documentation/cli) is useful.
