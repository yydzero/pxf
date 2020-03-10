#!/usr/bin/env bash

set -e

SCRIPT_DIR=$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )

cluster_env_files=$( cd "${SCRIPT_DIR}/../../../cluster_env_files" && pwd )

cp -r "${cluster_env_files}/.ssh" ~

scp "${SCRIPT_DIR}/cli/common.sh" mdw:

for script in "${SCRIPT_DIR}/cli/"test_*.sh; do
	scp "${script}" mdw:
	script_short_name=${script##*/}
	ssh mdw "~gpadmin/${script_short_name}"
done
