#!/bin/bash

set -eoux pipefail

GPHOME=/usr/local/greenplum-db-devel
PXF_ARTIFACTS_DIR=${PWD}/${OUTPUT_ARTIFACT_DIR}

# use a login shell for setting environment
getent hosts
bash --login -c "
	export PXF_HOME=${GPHOME}/pxf BUILD_NUMBER=${TARGET_OS}
	getent hosts
	make -C '${PWD}/pxf_src' test install
"

# Create tarball for PXF
tar -C "${GPHOME}" -czf "${PXF_ARTIFACTS_DIR}/pxf.tar.gz" pxf
