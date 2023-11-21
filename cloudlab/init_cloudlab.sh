#!/usr/bin/env bash

# Set on every new instantiation
### TO BE FILLED: Please provide all cluster IPs
    # Node w/ first IP (i.e., "manager") must run script before the rest of the nodes
    # (instantiates a memcached to setup RDMA connections)
ORDERED_HOST_NAMES=(
  "apt085.apt.emulab.net"
  "apt090.apt.emulab.net"
  "apt069.apt.emulab.net"
  "apt080.apt.emulab.net"
  "apt070.apt.emulab.net"
)

# Include cloudlab_ssh_config in ssh
# assumes you have created a key w/ ssh-keygen (here named id_rsa_cloudlab)
# and already registered its public key on cloudlab

# set once
CLOUDLAB_USERNAME="sohamb"
SSH_CONFIG="/home/soham/.ssh/config"
# CLOUDLAB_SSHKEY_FILE="${HOME}/.ssh/id_ed25519"

source init-ssh-keys.sh

SSH_PREFIX="n"
CONFIG_NAME="cloudlab_ssh_config"
SCRIPT_TO_COPY_N_RUN="init-preimaged.sh"

# Create file
echo "# cloudlab config" > ${CONFIG_NAME}
echo " " >> ${CONFIG_NAME}
for i in "${!ORDERED_HOST_NAMES[@]}"; do
  echo "Host ${SSH_PREFIX}$((i+1))" >> ${CONFIG_NAME}
  echo "    User ${CLOUDLAB_USERNAME}" >> ${CONFIG_NAME}
  echo "    IdentityFile ${CLOUDLAB_SSHKEY_FILE}" >> ${CONFIG_NAME}
  echo "    HostName ${ORDERED_HOST_NAMES[i]}" >> ${CONFIG_NAME}
  echo " " >> ${CONFIG_NAME}
done

cp ${CONFIG_NAME} ~/.ssh/

# Include in ssh_config if it does not exist
if cat ~/.ssh/config | grep "Include ${CONFIG_NAME}" ; then
   echo "${CONFIG_NAME} is already included in your ${SSH_CONFIG}"
else
   echo "Including ${CONFIG_NAME} in your ${SSH_CONFIG}"

   cp ${SSH_CONFIG} ${SSH_CONFIG}_backup  # take a backup of ssh config
   echo "Include ${CONFIG_NAME}" > ${SSH_CONFIG}
   echo " " >> ${SSH_CONFIG}
   cat ${SSH_CONFIG}_backup >> ${SSH_CONFIG}
fi

##insert to known_hosts
for i in "${!ORDERED_HOST_NAMES[@]}"; do
  ssh-keyscan -H ${ORDERED_HOST_NAMES[i]} >> ~/.ssh/known_hosts
done

SSH_REMOTE_SSHKEY="/users/${CLOUDLAB_USERNAME}/.ssh/id_rsa"
MACHINE_LIST_IDS=$(seq -s " " 1 ${#ORDERED_HOST_NAMES[@]})
# ARCHIVE_TO_COPY="odyssey-5-cluster.tar.gz"

# copy id_rsa_cloudlab to internal nodes (to allow access/scp with each other)
# and init to setup their initial environment
echo "Copying ssh_key and ${SCRIPT_TO_COPY_N_RUN} in cloudlab nodes: ${MACHINE_LIST_IDS}"
parallel scp ${CLOUDLAB_SSHKEY_FILE} ${SSH_PREFIX}{}:${SSH_REMOTE_SSHKEY} ::: ${MACHINE_LIST_IDS}
parallel scp ./${SCRIPT_TO_COPY_N_RUN} ${SSH_PREFIX}{}:~/${SCRIPT_TO_COPY_N_RUN} ::: ${MACHINE_LIST_IDS}
# parallel scp ${ARCHIVE_TO_COPY} ${SSH_PREFIX}{}:~/${ARCHIVE_TO_COPY} ::: ${MACHINE_LIST_IDS}

# run script
echo "Running ${SCRIPT_TO_COPY_N_RUN} in cloudlab nodes: ${MACHINE_LIST_IDS}"
parallel ssh ${SSH_PREFIX}{} './'"${SCRIPT_TO_COPY_N_RUN}"'' ::: ${MACHINE_LIST_IDS}
echo "Init done!"
