#!/bin/bash -l

GPHOME="/usr/local/greenplum-db-devel"
SSH_OPTS="-i cluster_env_files/private_key.pem -o StrictHostKeyChecking=no"
GPHD_ROOT="/singlecluster"
NO_OF_FILES=10000

function setup_gpadmin_user() {

    groupadd -g 1000 gpadmin && useradd -u 1000 -g 1000 -M gpadmin
    echo "gpadmin  ALL=(ALL)       NOPASSWD: ALL" > /etc/sudoers.d/gpadmin
    groupadd supergroup && usermod -a -G supergroup gpadmin
    echo -e "password\npassword" | passwd gpadmin 2> /dev/null
    echo -e "gpadmin soft core unlimited" >> /etc/security/limits.d/gpadmin-limits.conf
    echo -e "gpadmin soft nproc 131072" >> /etc/security/limits.d/gpadmin-limits.conf
    echo -e "gpadmin soft nofile 65536" >> /etc/security/limits.d/gpadmin-limits.conf
    echo -e "export JAVA_HOME=/usr/lib/jvm/java-1.7.0-openjdk.x86_64" >> /home/gpadmin/.bashrc
    ln -s ${PWD}/pxf_src /home/gpadmin/pxf_src
}

function setup_sshd() {

    service sshd start
    passwd -u root
    /bin/cp -Rf cluster_env_files/.ssh/* /root/.ssh
    /bin/cp -f cluster_env_files/private_key.pem /root/.ssh/id_rsa
    /bin/cp -f cluster_env_files/public_key.pem /root/.ssh/id_rsa.pub
    /bin/cp -f cluster_env_files/public_key.openssh /root/.ssh/authorized_keys
    echo "##########################################################"
    cat /root/.ssh/id_rsa
    echo "##########################################################"
    sed 's/edw0/hadoop/' cluster_env_files/etc_hostfile >> /etc/hosts
}

function configure_hdfs() {

    sed -i -e 's|hdfs://0.0.0.0:8020|hdfs://hadoop:8020|' ${GPHD_ROOT}/hadoop/etc/hadoop/core-site.xml ${GPHD_ROOT}/hbase/conf/hbase-site.xml ${GPHD_ROOT}/hive/conf/hive-site.xml
	sed -i -e "s/>tez/>mr/g" ${GPHD_ROOT}/hive/conf/hive-site.xml
}

function remote_access_to_gpdb() {

    ssh ${SSH_OPTS} gpadmin@mdw "source /usr/local/greenplum-db-devel/greenplum_path.sh && \
      export MASTER_DATA_DIRECTORY=/data/gpdata/master/gpseg-1 && \
      echo 'host all all 192.168.0.0/16 trust' >> /data/gpdata/master/gpseg-1/pg_hba.conf && \
      psql -d template1 -c 'CREATE EXTENSION pxf;' && \
      psql -d template1 -c 'CREATE DATABASE gpadmin;' && \
      psql -d template1 -c 'CREATE ROLE root LOGIN;' && \
      gpstop -u"
}

function ssh_access_to_hadoop() {

    ssh ${SSH_OPTS} centos@hadoop "sudo mkdir -p /root/.ssh && \
        sudo cp /home/centos/.ssh/authorized_keys /root/.ssh && \
        sudo sed -i 's/PermitRootLogin no/PermitRootLogin yes/' /etc/ssh/sshd_config && \
        sudo service sshd restart"
}

function run_multinode_smoke_test() {

    /singlecluster/bin/hdfs dfs -mkdir -p /tmp
    mkdir -p /tmp/pxf_test

    echo "Number of files = ${NO_OF_FILES}"
    for i in $(seq 1 ${NO_OF_FILES}); do
    cat > /tmp/pxf_test/test_${i}.txt <<-EOF
	1
	2
	3
	EOF
    done

    /singlecluster/bin/hdfs dfs -copyFromLocal /tmp/pxf_test/ /tmp
    /singlecluster/bin/hdfs dfs -chown -R gpadmin:gpadmin /tmp/pxf_test
    echo "Found $(/singlecluster/bin/hdfs dfs -ls /tmp/pxf_test | grep pxf_test | wc -l) items in /tmp/pxf_test"
    expected_output=$((3 * ${NO_OF_FILES}))
    time ssh ${SSH_OPTS} gpadmin@mdw "source ${GPHOME}/greenplum_path.sh
	psql -d template1 -c \"
	CREATE EXTERNAL TABLE pxf_multifile_test (b TEXT) LOCATION ('pxf://tmp/pxf_test?PROFILE=HdfsTextSimple') FORMAT 'CSV';\"
	num_rows=\$(psql -d template1 -t -c \"SELECT COUNT(*) FROM pxf_multifile_test;\" | head -1)
	if [[ \"\${num_rows}\" == \"${expected_output}\" ]]; then
		echo \"Received expected output\"
	else
		echo \"Error. Expected output ${expected_output} does not match actual \${num_rows}\"
	fi
	"
}

function setup_local_gpdb() {

    mkdir -p ${GPHOME}
    tar -xzf gpdb_binary/bin_gpdb.tar.gz -C ${GPHOME}
    tar -xzf pxf_tarball/pxf.tar.gz -C ${GPHOME}
    psi_dir=$(find /usr/lib64 -name psi | sort -r | head -1)
    cp -r ${psi_dir} ${GPHOME}/lib/python
    cp -r cluster_env_files/.ssh /home/gpadmin/.ssh
    cp /home/gpadmin/.ssh/*.pem /home/gpadmin/.ssh/id_rsa
    cp cluster_env_files/public_key.openssh /home/gpadmin/.ssh/authorized_keys
    { ssh-keyscan localhost; ssh-keyscan 0.0.0.0; ssh-keyscan hadoop; } >> /home/gpadmin/.ssh/known_hosts
}

function open_ssh_tunnels() {

    # https://stackoverflow.com/questions/2241063/bash-script-to-setup-a-temporary-ssh-tunnel
    ssh -fNT -M -S /tmp/mdw5432 -L 5432:mdw:5432 gpadmin@mdw
    ssh -fNT -M -S /tmp/hadoop2181 -L 2181:hadoop:2181 root@hadoop
    ssh -S /tmp/mdw5432 -O check gpadmin@mdw
    ssh -S /tmp/hadoop2181 -O check root@hadoop
}

function close_ssh_tunnels() {

    ssh -S /tmp/mdw5432 -O exit gpadmin@mdw
    ssh -S /tmp/hadoop2181 -O exit root@hadoop
}

function run_pxf_automation() {

    /singlecluster/bin/hdfs dfs -chown gpadmin:gpadmin /tmp
    sed -i 's/sutFile=default.xml/sutFile=MultiNodesCluster.xml/g' pxf_src/pxf_automation/jsystem.properties
    chown -R gpadmin:gpadmin /home/gpadmin pxf_src/pxf_automation

    cat > /home/gpadmin/run_pxf_automation_test.sh <<-EOF

	export GPHOME=/usr/local/greenplum-db-devel
	export PXF_HOME=\${GPHOME}/pxf
	export PG_MODE=GPDB
	export PGHOST=localhost
	export PGPORT=5432
	source \${GPHOME}/greenplum_path.sh

	cd pxf_src/pxf_automation
	make GROUP=gpdb >> /tmp/pxf_automation.out

	exit 0
	EOF

	chown gpadmin:gpadmin /home/gpadmin/run_pxf_automation_test.sh
	chmod a+x /home/gpadmin/run_pxf_automation_test.sh

    local i=0
    local uuid=$(uuid)
	while true
	do
	    ssh ${SSH_OPTS} sdw1 "tail /tmp/jstat_pxf_1sec.out"
	    echo "Running PXF Automation - Run #$i"
	    su gpadmin -c "bash /home/gpadmin/run_pxf_automation_test.sh"
	    ssh ${SSH_OPTS} sdw1 "tail /tmp/jstat_pxf_1sec.out &&
	    echo \"jmap -dump:live,format=b,file=bug_repro.$i.hprof \$(pgrep -f tomcat)\"
	    jmap -dump:live,format=b,file=bug_repro.$i.hprof \$(pgrep -f tomcat | head 1) &&
	    gsutil cp bug_repro.$i.hprof gs://ud-tmp/heap_dumps/${uuid}/"
	    echo "Sleeping for 15 minutes"
	    i=$(($i + 1))
	    sleep 900 # Sleep for 15 min
	done
}

function _main() {

    setup_gpadmin_user
    setup_sshd
    remote_access_to_gpdb
    ssh_access_to_hadoop
    setup_local_gpdb

    open_ssh_tunnels
    configure_hdfs
    run_multinode_smoke_test
    run_pxf_automation
    close_ssh_tunnels
}

_main "$@"
