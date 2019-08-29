#!/bin/bash

set -eoux pipefail

GPHOME=/usr/local/greenplum-db-devel
PXF_ARTIFACTS_DIR=${PWD}/${OUTPUT_ARTIFACT_DIR}

su --login -c "
	export PXF_HOME=${GPHOME}/pxf BUILD_NUMBER=${TARGET_OS}
	make -C '${PWD}/pxf_src' test install
" root

# Create tarball for PXF
tar -C "${GPHOME}" -czf "${PXF_ARTIFACTS_DIR}/pxf.tar.gz" pxf
