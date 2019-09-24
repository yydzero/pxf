#!/usr/bin/env bash

ssh '{{ PGHOST }}' "
		command -v kinit && kinit -kt ~/pxf/keytabs/pxf.service.keytab gpadmin
		JAVA_HOME={{ JAVA_HOME }} ~/workspace/singlecluster/bin/hdfs $*
	"
