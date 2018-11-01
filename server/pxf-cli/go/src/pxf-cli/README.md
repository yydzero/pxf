# `pxf` CLI

This directory houses the Go portion of the PXF CLI.

The PXF CLI currently consists of a legacy bash script that wraps a Go program.
Future CLI features will be written in Go, but for now the CLI functionality is
divided between the two halves.

## Getting Started

1. Ensure you are set up for PXF development by following the README.md at the
   root of this repository.

1. Go to the pxf-cli folder and install dependencies
   ```
   cd pxf/cli/go/src/pxf-cli
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

## Adding New Dependencies

1. Import the dependency in some source file (otherwise `dep` will refuse to install it)

2. Add the dependency to Gopkg.toml

3. Run `make depend`.