#!/usr/bin/env bash

dir=$( cd "$(dirname "${BASH_SOURCE[0]}")" && pwd )
[[ -e ${dir}/common.sh ]] || exit 1
source "${dir}/common.sh"

expected_reset_message="Ensure your PXF cluster is stopped before continuing. This is a destructive action. Press y to continue:
Resetting PXF on 4 hosts including master...
ERROR: Failed to reset PXF on 2 out of 4 hosts
sdw1 ==> PXF is running. Please stop PXF before running 'pxf [cluster] reset'

sdw2 ==> PXF is running. Please stop PXF before running 'pxf [cluster] reset'"
compare "${expected_reset_message}" "$(yes | pxf cluster reset 2>&1)" "pxf cluster reset should fail on sdw{1,2}"

expected_stop_message="Stopping PXF on 2 segment hosts...
PXF stopped successfully on 2 out of 2 hosts"
compare "${expected_stop_message}" "$(pxf cluster stop)" "pxf cluster stop should succeed"

expected_init_message="Initializing PXF on 4 hosts including master...
ERROR: PXF failed to initialize on 2 out of 4 hosts
sdw1 ==> Instance already exists. Use 'pxf [cluster] reset' before attempting to re-initialize PXF

sdw2 ==> Instance already exists. Use 'pxf [cluster] reset' before attempting to re-initialize PXF"
compare "${expected_init_message}" "$(pxf cluster init 2>&1)" "pxf cluster init should fail on sdw{1,2}"

expected_reset_sdw_message="Cleaning ${GPHOME}/pxf/conf/pxf-private.classpath...
Ignoring ${PXF_CONF}...
Cleaning ${GPHOME}/pxf/pxf-service...
Cleaning ${GPHOME}/pxf/run...
Reverting changes to ${GPHOME}/pxf/conf/pxf-env-default.sh...
Finished cleaning PXF instance directories"
compare "${expected_reset_sdw_message}" "$(ssh sdw1 "${GPHOME}/pxf/bin/pxf" reset --force)" "pxf reset on sdw1 should succeed"
compare "${expected_reset_sdw_message}" "$(ssh sdw2 "${GPHOME}/pxf/bin/pxf" reset --force)" "pxf reset on sdw2 should succeed"

expected_init_message="Initializing PXF on 4 hosts including master...
ERROR: PXF failed to initialize on 2 out of 4 hosts
mdw ==> Instance already exists. Use 'pxf [cluster] reset' before attempting to re-initialize PXF

smdw ==> Instance already exists. Use 'pxf [cluster] reset' before attempting to re-initialize PXF"
compare "${expected_init_message}" "$(yes | pxf cluster init 2>&1)" "pxf cluster reset should fail on {,s}mdw"

expected_start_message="Starting PXF on 2 segment hosts...
PXF started successfully on 2 out of 2 hosts"
compare "${expected_start_message}" "$(pxf cluster start)" "pxf cluster start should succeed"
