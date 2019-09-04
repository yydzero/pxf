#!/bin/bash

set -exuo pipefail

# defaults
ENV_FILES_DIR=${ENV_FILES_DIR:-dataproc_env_files}
HADOOP_USER=${HADOOP_USER:-gpadmin}
IMAGE_VERSION=${IMAGE_VERSION:-1.3}
INITIALIZATION_SCRIPT=${INITIALIZATION_SCRIPT:-gs://pxf-perf/scripts/initialization-for-kerberos.sh}
KERBEROS=${KERBEROS:-false}
KEYRING=${KEYRING:-dataproc-kerberos}
KEY=${KEY:-dataproc-kerberos-test}
NUM_WORKERS=${NUM_WORKERS:-2}
PETNAME=${PETNAME:-}
PROJECT=${GOOGLE_PROJECT_ID:-}
REGION=${GOOGLE_ZONE%-?} # lop off '-a', '-b', etc. from $GOOGLE_ZONE
REGION=${REGION:-us-central1}
SECRETS_BUCKET=${SECRETS_BUCKET:-data-gpdb-ud-pxf-secrets}
SSH_KEY=${SSH_KEY:-}
EXTRA_USER=${EXTRA_USER:-}
SUBNETWORK=${SUBNETWORK:-dynamic}
TAGS=${TAGS:-bosh-network,outbound-through-nat,tag-concourse-dynamic}
USE_EXTERNAL_IP=${USE_EXTERNAL_IP:-}
ZONE=${GOOGLE_ZONE:-us-central1-a}

if [[ -z $PETNAME ]]; then
    pip install petname
    PETNAME=ccp-$(petname)
fi

gcloud config set project "$PROJECT"
gcloud auth activate-service-account --key-file=<(echo "$GOOGLE_CREDENTIALS")

PROPERTIES="core:hadoop.proxyuser.${HADOOP_USER}.hosts=*,core:hadoop.proxyuser.${HADOOP_USER}.groups=*"
if [[ -n $EXTRA_USER ]]; then
    PROPERTIES+=",core:hadoop.proxyuser.${EXTRA_USER}.hosts=*,core:hadoop.proxyuser.${EXTRA_USER}.groups=*"
fi

# Initialize the dataproc service
GCLOUD_COMMAND=(gcloud beta dataproc clusters
  "--region=$REGION" create "$PETNAME"
  --initialization-actions "$INITIALIZATION_SCRIPT"
  --subnet "projects/${PROJECT}/regions/${REGION}/subnetworks/$SUBNETWORK"
  "--zone=$ZONE"
  "--tags=$TAGS"
  "--num-workers=$NUM_WORKERS"
  --image-version "$IMAGE_VERSION"
  --properties "$PROPERTIES")

if [[ -z $USE_EXTERNAL_IP ]]; then
    GCLOUD_COMMAND+=(--no-address)
fi

if [[ $KERBEROS == true ]]; then
    PLAINTEXT=$(mktemp)
    PLAINTEXT_NAME=$(basename "$PLAINTEXT")

    # Generate a random password
    date +%s | sha256sum | base64 | head -c 64 > "$PLAINTEXT"

    # Encrypt password file with the KMS key
    gcloud kms encrypt \
      --location "$REGION" \
      --keyring "$KEYRING" \
      --key "$KEY" \
      --plaintext-file "$PLAINTEXT" \
      --ciphertext-file "${PLAINTEXT}.enc"

    # Copy the encrypted file to gs
    gsutil cp "${PLAINTEXT}.enc" "gs://${SECRETS_BUCKET}/"

    GCLOUD_COMMAND+=(--kerberos-root-principal-password-uri
      "gs://${SECRETS_BUCKET}/${PLAINTEXT_NAME}.enc"
       --kerberos-kms-key "$KEY"
       --kerberos-kms-key-keyring "$KEYRING"
       --kerberos-kms-key-location "$REGION"
       --kerberos-kms-key-project "$PROJECT")
fi

"${GCLOUD_COMMAND[@]}"

yum install -y -d1 openssh openssh-clients
mkdir -p ~/.ssh
ID_RSA=~/.ssh/google_compute_engine
ssh-keygen -b 4096 -t rsa -f "$ID_RSA" -N "" -C "$HADOOP_USER"
cp "${ID_RSA}"{,.pub} "${ENV_FILES_DIR}"

SSH_KEY_FILE=$(mktemp)
echo "gpadmin:$(< "${ID_RSA}.pub")" > "$SSH_KEY_FILE"
[[ -n $SSH_KEY && -n $EXTRA_USER ]] && echo "$EXTRA_USER:$SSH_KEY" >> "$SSH_KEY_FILE"

eval "HADOOP_HOSTNAMES=( ${PETNAME}-m ${PETNAME}-w-{0..$(( NUM_WORKERS - 1 ))} )"
echo "${HADOOP_HOSTNAMES[0]}" > "${ENV_FILES_DIR}/name"

HOSTS="${ENV_FILES_DIR}/dataproc_hosts"
echo "# entries for dataproc cluster '${PETNAME}'" >"${HOSTS}"

for hostname in "${HADOOP_HOSTNAMES[@]}"; do
    gcloud compute instances add-metadata "${hostname}" \
        --metadata-from-file "ssh-keys=${SSH_KEY_FILE}" \
        --zone "$ZONE"
    echo "$(gcloud compute instances describe "$hostname" --format='get(networkInterfaces[0].accessConfigs[0].natIP)') $hostname" >>"$HOSTS"
done

cat "${HOSTS}" >>/etc/hosts

OPTS=(-o 'UserKnownHostsFile=/dev/null' -o 'StrictHostKeyChecking=no' -i "${ID_RSA}")
SSH=(command ssh -t "${OPTS[@]}")
SCP=(command scp "${OPTS[@]}")

mkdir -p "${ENV_FILES_DIR}/conf"

"${SCP[@]}" "${HADOOP_USER}@${HADOOP_HOSTNAMES[0]}":/etc/{hadoop,hive}/conf/*-site.xml \
    "${ENV_FILES_DIR}/conf"

temp_hdfs_site=$(mktemp)
sed -e '/<\/configuration>/d' "${ENV_FILES_DIR}/conf/hdfs-site.xml" >> "${temp_hdfs_site}"
cat >> "${temp_hdfs_site}" <<-EOF
	  <property>
	    <name>dfs.client.use.datanode.hostname</name>
	    <value>true</value>
	    <description>Whether clients use datanode hostnames when connecting to datanodes.</description>
	  </property>
	</configuration>
EOF
cp "${temp_hdfs_site}" "${ENV_FILES_DIR}/conf/hdfs-site.xml"

"${SSH[@]}" "${HADOOP_USER}@${HADOOP_HOSTNAMES[0]}" \
    "sudo systemctl restart hadoop-hdfs-namenode"

if [[ $KERBEROS == true ]]; then
    "${SSH[@]}" "${HADOOP_USER}@${HADOOP_HOSTNAMES[0]}" "
        set -euo pipefail
        grep default_realm /etc/krb5.conf | awk '{print \$3}' > ~/REALM
        sudo kadmin.local -q 'addprinc -pw pxf gpadmin'
        sudo kadmin.local -q \"xst -k \${HOME}/pxf.service.keytab gpadmin\"
        sudo klist -e -k -t ~/pxf.service.keytab
        sudo chown gpadmin ~/pxf.service.keytab
        sudo addgroup gpadmin hdfs
        sudo addgroup gpadmin hadoop
        sudo addgroup $EXTRA_USER hdfs
        sudo addgroup $EXTRA_USER hadoop
    "

  "${SCP[@]}" "${HADOOP_USER}@${HADOOP_HOSTNAMES[0]}":{REALM,pxf.service.keytab,/etc/krb5.conf} \
      "$ENV_FILES_DIR"
fi
