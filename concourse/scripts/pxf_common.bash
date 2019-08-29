#!/bin/bash

GPHOME=/usr/local/greenplum-db-devel
PXF_HOME=${GPHOME}/pxf
MDD_VALUE=/data/gpdata/master/gpseg-1
PXF_COMMON_SRC_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)

# on purpose do not call this PXF_CONF so that it is not set during pxf operations
PXF_CONF_DIR=/home/gpadmin/pxf

JAVA_HOME=$(find /usr/lib/jvm -name 'java-1.8.0-openjdk*' | head -1)

if [[ -d gpdb_src/gpAux/extensions/pxf ]]; then
	PXF_EXTENSIONS_DIR=gpdb_src/gpAux/extensions/pxf
else
	PXF_EXTENSIONS_DIR=gpdb_src/gpcontrib/pxf
fi

function set_env() {
	export TERM=xterm-256color
	export TIMEFORMAT=$'\e[4;33mIt took %R seconds to complete this step\e[0m';
}

function run_regression_test() {
	ln -s "${PWD}/gpdb_src" ~gpadmin/gpdb_src
	cat > ~gpadmin/run_regression_test.sh <<-EOF
		#!/bin/bash
		source /opt/gcc_env.sh || true
		source ${GPHOME}/greenplum_path.sh
		source gpdb_src/gpAux/gpdemo/gpdemo-env.sh
		export PATH=\$PATH:${GPHD_ROOT}/bin

		cd "${PXF_EXTENSIONS_DIR}"
		make installcheck USE_PGXS=1

		[ -s regression.diffs ] && cat regression.diffs && exit 1

		exit 0
	EOF

	chown -R gpadmin:gpadmin "${PXF_EXTENSIONS_DIR}"
	chown gpadmin:gpadmin ~gpadmin/run_regression_test.sh
	chmod a+x ~gpadmin/run_regression_test.sh
	su gpadmin -c ~gpadmin/run_regression_test.sh
}

function build_install_gpdb() {

	# need a login shell to source /opt/gcc_env.sh (~root/.bashrc)
	bash --login -c "
		cd '${PWD}/gpdb_src' || return 1
		CC=\$(command -v gcc) CXX=\$(command -v g++) ./configure \
			--with-{perl,python,libxml,zstd} \
			--disable-{gpfdist,orca} \
			'--prefix=${GPHOME}'
		make -j4 -s
		make -s install
	"
}

function install_gpdb_binary() {
	if [[ -d bin_gpdb ]]; then
		mkdir -p ${GPHOME}
		tar -xzf bin_gpdb/*.tar.gz -C ${GPHOME}
	else
		build_install_gpdb
	fi

	local gphome python_dir python_version=2.7 export_pythonpath='export PYTHONPATH=$PYTHONPATH'
	if [[ ${TARGET_OS} == centos ]]; then
		# We can't use service sshd restart as service is not installed on CentOS 7.
		/usr/sbin/sshd &
		# CentOS 6 uses python 2.6
		if grep 'CentOS release 6' /etc/centos-release; then
			python_version=2.6
		fi
		python_dir=python${python_version}/site-packages
		export_pythonpath+=:/usr/lib/${python_dir}:/usr/lib64/$python_dir
	elif [[ ${TARGET_OS} == ubuntu ]]; then
		# Adjust GPHOME if the binary expects it to be /usr/local/gpdb
		gphome=$(grep ^GPHOME= /usr/local/greenplum-db-devel/greenplum_path.sh | cut -d= -f2)
		if [[ $gphome == /usr/local/gpdb ]]; then
			mv /usr/local/greenplum-db-devel /usr/local/gpdb
			GPHOME=/usr/local/gpdb
			PXF_HOME=${GPHOME}/pxf
		fi
		service ssh start
		python_dir=python${python_version}/dist-packages
		export_pythonpath+=:/usr/local/lib/$python_dir
	fi

	echo "$export_pythonpath" >> "${PXF_COMMON_SRC_DIR}/../../automation/tinc/main/tinc_env.sh"
}

function remote_access_to_gpdb() {
	# Copy cluster keys to root user
	passwd -u root
	cp -Rf cluster_env_files/.ssh/* /root/.ssh
	cp -f cluster_env_files/private_key.pem /root/.ssh/id_rsa
	cp -f cluster_env_files/public_key.pem /root/.ssh/id_rsa.pub
	cp -f cluster_env_files/public_key.openssh /root/.ssh/authorized_keys
	sed 's/edw0/hadoop/' cluster_env_files/etc_hostfile >> /etc/hosts
	# Copy cluster keys to gpadmin user
	rm -rf /home/gpadmin/.ssh/*
	cp cluster_env_files/.ssh/* /home/gpadmin/.ssh
	cp cluster_env_files/.ssh/*.pem /home/gpadmin/.ssh/id_rsa
	cp cluster_env_files/public_key.openssh /home/gpadmin/.ssh/authorized_keys
	{ ssh-keyscan localhost; ssh-keyscan 0.0.0.0; } >> /home/gpadmin/.ssh/known_hosts
	ssh "${SSH_OPTS[@]}" gpadmin@mdw "
		source ${GPHOME}/greenplum_path.sh &&
		export MASTER_DATA_DIRECTORY=${MDD_VALUE} &&
		echo 'host all all 10.0.0.0/16 trust' >> ${MDD_VALUE}/pg_hba.conf &&
		psql -d template1 <<-EOF && gpstop -u
			CREATE EXTENSION pxf;
			CREATE DATABASE gpadmin;
			CREATE ROLE root LOGIN;
		EOF
	"
}


function create_gpdb_cluster() {
	pushd gpdb_src/gpAux/gpdemo || return 1
	su gpadmin -c "source ${GPHOME}/greenplum_path.sh && make create-demo-cluster"
	popd || return 1
}

function add_remote_user_access_for_gpdb() {
	local username=${1}
	# load local cluster configuration
	pushd gpdb_src/gpAux/gpdemo || return 1

	echo "Adding access entry for ${username} to pg_hba.conf"
	su gpadmin -c "source ./gpdemo-env.sh; echo 'local    all     ${username}     trust' >> \${MASTER_DATA_DIRECTORY}/pg_hba.conf"

	echo "Restarting GPDB for change to pg_hba.conf to take effect"
	su gpadmin -c "source ${GPHOME}/greenplum_path.sh; source ./gpdemo-env.sh; gpstop -u"
	popd || return 1
}

function setup_gpadmin_user() {

	# Don't create gpadmin user if already exists
	if ! id -u gpadmin; then
		groupadd -g 1000 gpadmin && useradd -u 1000 -g 1000 -M gpadmin
		echo "gpadmin  ALL=(ALL)	   NOPASSWD: ALL" > /etc/sudoers.d/gpadmin
		groupadd supergroup && usermod -a -G supergroup gpadmin
		mkdir -p ~gpadmin/.ssh
		ssh-keygen -t rsa -N "" -f ~gpadmin/.ssh/id_rsa
		cat /home/gpadmin/.ssh/id_rsa.pub >> ~gpadmin/.ssh/authorized_keys
		chmod 0600 /home/gpadmin/.ssh/authorized_keys
		{ ssh-keyscan localhost; ssh-keyscan 0.0.0.0; } >> ~gpadmin/.ssh/known_hosts
		chown -R gpadmin:gpadmin ${GPHOME} ~gpadmin/.ssh # don't chown cached dirs ~/.m2, etc.
		echo -e "password\npassword" | passwd gpadmin 2> /dev/null
	fi
	cat <<-EOF >> /etc/security/limits.d/gpadmin-limits.conf
		gpadmin soft core unlimited
		gpadmin soft nproc 131072
		gpadmin soft nofile 65536
	EOF
	echo "export JAVA_HOME=${JAVA_HOME}" >> ~gpadmin/.bashrc
	if [[ -d gpdb_src/gpAux/gpdemo ]]; then
		chown -R gpadmin:gpadmin gpdb_src/gpAux/gpdemo
	fi

	if grep -i ubuntu /etc/os-release; then
		echo '[[ -f ~/.bashrc ]] && . ~/.bashrc' >> ~gpadmin/.bash_profile
		chown gpadmin:gpadmin ~gpadmin/.bash_profile
	fi
}

function install_pxf_client() {
	# recompile pxf.so file for dev environments only
	if [[ ${TEST_ENV} == dev ]]; then
		# need a login shell to source /opt/gcc_env.sh (~root/.bashrc)
		bash --login -c "
			source '${GPHOME}/greenplum_path.sh'
			USE_PGXS=1 make -C '${PXF_EXTENSIONS_DIR}' install
		"
	fi
}

function install_pxf_server() {
	if [[ ! -d ${PXF_HOME} ]]; then
		if [[ -d pxf_tarball ]]; then
			tar -xzf pxf_tarball/pxf.tar.gz -C ${GPHOME}
		else
			# requires login shell so that Go's dep is on PATH
			bash --login -c "
				export BUILD_NUMBER=${TARGET_OS} JAVA_TOOL_OPTIONS=-Dfile.encoding=UTF8
				make -C '${PWD}/pxf_src' install
			"
		fi
	fi
	chown -R gpadmin:gpadmin "${PXF_HOME}"
}

function setup_impersonation() {
	local GPHD_ROOT=${1}

	# enable impersonation by gpadmin user
	if [[ ${IMPERSONATION} == true ]]; then
		echo 'Impersonation is enabled, adding support for gpadmin proxy user'
		cat > proxy-config.xml <<-EOF
			<property>
			  <name>hadoop.proxyuser.gpadmin.hosts</name>
			  <value>*</value>
			</property>
			<property>
			  <name>hadoop.proxyuser.gpadmin.groups</name>
			  <value>*</value>
			</property>
			<property>
			  <name>hadoop.security.authorization</name>
			  <value>true</value>
			</property>
			<property>
			  <name>hbase.security.authorization</name>
			  <value>true</value>
			</property>
			<property>
			  <name>hbase.rpc.protection</name>
			  <value>authentication</value>
			</property>
			<property>
			  <name>hbase.coprocessor.master.classes</name>
			  <value>org.apache.hadoop.hbase.security.access.AccessController</value>
			</property>
			<property>
			  <name>hbase.coprocessor.region.classes</name>
			  <value>org.apache.hadoop.hbase.security.access.AccessController,org.apache.hadoop.hbase.security.access.SecureBulkLoadEndpoint</value>
			</property>
			<property>
			  <name>hbase.coprocessor.regionserver.classes</name>
			  <value>org.apache.hadoop.hbase.security.access.AccessController</value>
			</property>
		EOF
		sed -i -e '/<configuration>/r proxy-config.xml' "${GPHD_ROOT}/hadoop/etc/hadoop/core-site.xml" "${GPHD_ROOT}/hbase/conf/hbase-site.xml"
		rm proxy-config.xml
	elif [[ ${IMPERSONATION} == false ]]; then
		echo 'Impersonation is disabled, no proxy user setup performed.'
	else
		echo "ERROR: Invalid or missing CI property value: IMPERSONATION=${IMPERSONATION}"
		exit 1
	fi
	if ! find "${GPHD_ROOT}/hbase/lib" -name 'pxf-hbase-*.jar' | grep pxf-hbase; then
		cp "${PXF_HOME}"/lib/pxf-hbase-*.jar "${GPHD_ROOT}/hbase/lib"
	fi
}

function start_hadoop_services() {
	local GPHD_ROOT=${1}

	# Start all hadoop services
	"${GPHD_ROOT}/bin/init-gphd.sh"
	"${GPHD_ROOT}/bin/start-hdfs.sh"
	"${GPHD_ROOT}/bin/start-zookeeper.sh" &
	"${GPHD_ROOT}/bin/start-yarn.sh" &
	"${GPHD_ROOT}/bin/start-hbase.sh" &
	"${GPHD_ROOT}/bin/start-hive.sh" &
	wait
	export PATH=$PATH:${GPHD_ROOT}/bin

	# list running Hadoop daemons
	jps

	# grant gpadmin user admin privilege for feature tests to be able to run on secured cluster
	if [[ ${IMPERSONATION} == true ]]; then
		echo 'Granting gpadmin user admin privileges for HBase'
		echo "grant 'gpadmin', 'RWXCA'" | hbase shell
	fi
}

function init_and_configure_pxf_server() {
	echo 'Ensure pxf version can be run before pxf init'
	su gpadmin -c "${PXF_HOME}/bin/pxf version | grep -E '^PXF version [0-9]+.[0-9]+.[0-9]+$'" || exit 1

	echo 'Initializing PXF service'
	# requires a login shell to source startup scripts (JAVA_HOME)
	su --login -c "PXF_CONF=${PXF_CONF_DIR} ${PXF_HOME}/bin/pxf init" gpadmin

	# update impersonation value based on CI parameter
	if [[ ! ${IMPERSONATION} == true ]]; then
		echo 'Impersonation is disabled, updating pxf-env.sh property'
		su gpadmin -c "echo 'export PXF_USER_IMPERSONATION=false' >> ${PXF_CONF_DIR}/conf/pxf-env.sh"
	fi

	# update runtime JDK value based on CI parameter
	RUN_JDK_VERSION=${RUN_JDK_VERSION:-1.8}
	if [[ $RUN_JDK_VERSION == 11 ]]; then
		echo 'JDK 11 requested for runtime, setting PXF JAVA_HOME=/usr/lib/jvm/jdk-11 in pxf-env.sh'
		su gpadmin -c "echo 'export JAVA_HOME=/usr/lib/jvm/jdk-11' >> ${PXF_CONF_DIR}/conf/pxf-env.sh"
	fi
}

function configure_pxf_s3_server() {
	mkdir -p "${PXF_CONF_DIR}/servers/s3"
	cp "${PXF_CONF_DIR}/templates/s3-site.xml" "${PXF_CONF_DIR}/servers/s3/s3-site.xml"
	sed -i  -e "s|YOUR_AWS_ACCESS_KEY_ID|${ACCESS_KEY_ID}|" \
		-e "s|YOUR_AWS_SECRET_ACCESS_KEY|${SECRET_ACCESS_KEY}|" \
		"${PXF_CONF_DIR}/servers/s3/s3-site.xml"

	mkdir -p "${PXF_CONF_DIR}/servers/s3-invalid"
	cp "${PXF_CONF_DIR}/templates/s3-site.xml" "${PXF_CONF_DIR}/servers/s3-invalid/s3-site.xml"
}

function configure_pxf_default_server() {
	# copy hadoop config files to PXF_CONF_DIR/servers/default
	if [[ -d /etc/hadoop/conf/ ]]; then
		cp /etc/hadoop/conf/*-site.xml "${PXF_CONF_DIR}/servers/default"
	fi
	if [[ -d /etc/hive/conf/ ]]; then
		cp /etc/hive/conf/*-site.xml "${PXF_CONF_DIR}/servers/default"
	fi
	if [[ -d /etc/hbase/conf/ ]]; then
		cp /etc/hbase/conf/*-site.xml "${PXF_CONF_DIR}/servers/default"
	fi
}

function start_pxf_server() {
	# Check if some other process is listening on 5888
	netstat -tlpna | grep 5888 || true

	echo 'Starting PXF service'
	su --login -c "${PXF_HOME}/bin/pxf start" gpadmin
	# grep with regex to avoid catching grep process itself
	ps -aef | grep '[t]omcat'
}

function setup_minio() {
	echo 'Adding test bucket gpdb-ud-scratch to Minio ...'
	mkdir -p /opt/minio/data/gpdb-ud-scratch

	export MINIO_ACCESS_KEY=admin
	export MINIO_SECRET_KEY=password
	echo "Minio credentials: accessKey=${MINIO_ACCESS_KEY} secretKey=${MINIO_SECRET_KEY}"

	echo 'Starting Minio ...'
	/opt/minio/bin/minio server /opt/minio/data &

	# export minio credentials as access environment variables
	export ACCESS_KEY_ID=${MINIO_ACCESS_KEY}
	export SECRET_ACCESS_KEY=${MINIO_SECRET_KEY}
}
