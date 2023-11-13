#!/usr/bin/env bash

# WARNING: Before executing this script make sure to have setup
#   ssh-key on github and cloudlab and have share it with executing node

# TODO: Set the two variabled below
GITHUB_USERNAME="akatsarakis"
NO_NODES="5" # WARNING: cannot be higher than number of allocated nodes in cloudlab

# last OFED supporting CX3 nic
OFED="MLNX_OFED_LINUX-5.0-2.1.8.0-ubuntu18.04-x86_64"

if [[ "${NO_NODES}" -gt 9 ]]; then
  echo "Current script supports up to 9 nodes"
  exit 1;
fi

# [Optionally] set terminal bar --> must source ~/.bashrc to apply it
echo " " >> ~/.bashrc
echo "#My Options" >> ~/.bashrc
echo "#Terminal Bar" >> ~/.bashrc
echo "parse_git_branch() {" >> ~/.bashrc
echo "   git branch 2> /dev/null | sed -e '/^[^*]/d' -e 's/* \(.*\)/{\1}/'" >> ~/.bashrc
echo "}" >> ~/.bashrc
echo "export PS1=\"\[\033[36m\]\u\[\033[0;31m\]\$(parse_git_branch)\[\033[m\]@\[\033[32m\]\h:\[\033[33;2m\]\w\[\033[m\]\$\"" >> ~/.bashrc
echo " " >> ~/.bashrc

# Install dependencies
sudo apt update
sudo apt --yes install git vim htop make cmake gcc parallel libnuma-dev libmemcached-dev memcached

# silence parallel citation without the manual "will-cite" after parallel --citation
mkdir ~/.parallel
touch ~/.parallel/will-cite

# Configure (2MB) huge-pages for the KVS
echo 4096 | sudo tee /sys/devices/system/node/node*/hugepages/hugepages-2048kB/nr_hugepages
echo 10000000001 | sudo tee /proc/sys/kernel/shmmax
echo 10000000001 | sudo tee /proc/sys/kernel/shmall

ssh-keyscan -H github.com >> ~/.ssh/known_hosts
git clone https://github.com/ease-lab/Hermes hermes
git clone git@github.com:${GITHUB_USERNAME}/cloudlab.git # assumes ssh-key on github exists on the node
cd cloudlab
./merge-chuncks.sh
cd mellanox

tar -xvzf ${OFED}.tgz
cd ${OFED}
sudo ./mlnxofedinstall --force; sleep 2

# restart driver
sudo /etc/init.d/openibd restart; sleep 2

sudo mst start

sleep 20 # if we try to init nic immediately it typically fails

# Setting the ip to the ib0 might not work on the first try so repeat
MAX_RETRIES=10
for i in `seq 1 ${MAX_RETRIES}`; do
  sudo ifconfig ib0 10.0.3.${HOSTNAME:5:1}/24 up
  if ibdev2netdev | grep "Up" ; then
    break
  fi
  sleep 5
done

if ibdev2netdev | grep "Up" ; then
  echo "IB0 is Up!"
else
  ibdev2netdev
  echo "IB0 is not Up --> setup failed!"
  exit 1
fi

# [Optionally] For dbg ensure everything was configured properly
#ibdev2netdev # --> must show ib0 (up)
#ifconfig --> expected ib0 w/ expected ip
#ibv_devinfo --> PORT_ACTIVE

#############################
# WARNING only on first node!
#############################
if [[ "${HOSTNAME:5:1}" == 1 ]]; then
    sleep 100 # give some time so that all peers has setup their NICs

    # start a subnet manager
    sudo /etc/init.d/opensmd start # there must be at least one subnet-manager in an infiniband subnet cluster

    # Add all cluster nodes to known hosts
    # WARNING: execute this only after all nodes have setup their NICs (i.e., ifconfig up above)
    for i in `seq 1 ${NO_NODES}`; do
      ssh-keyscan -H 10.0.3.${i} >> ~/.ssh/known_hosts
    done

    # run default hermes
    cd ~/hermes/bin/
    ./copy-n-exec-hermesKV.sh no_pass # By default it runs on 5 nodes
fi