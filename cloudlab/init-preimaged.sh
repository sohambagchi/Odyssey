#!/usr/bin/env bash

# WARNING: Before executing this script make sure to have setup
#   ssh-key on github and cloudlab and have share it with executing node

# TODO: Set this variable below
NO_NODES="5" # WARNING: cannot be higher than number of allocated nodes in cloudlab
# ARCHIVE_NAME="odyssey-5-cluster.tar.gz"

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
echo "alias nic-perf='sudo watch -n1 perfquery -x -r' " >> ~/.bashrc
echo " " >> ~/.bashrc
echo "export PATH=\"/users/akats/.local/bin:${PATH}\"" >> ~/.bashrc
echo " " >> ~/.bashrc
source ~/.bashrc

# silence parallel citation without the manual "will-cite" after parallel --citation
mkdir ~/.parallel
touch ~/.parallel/will-cite

# Configure (2MB) huge-pages for the KVS
echo 4096 | sudo tee /sys/devices/system/node/node*/hugepages/hugepages-2048kB/nr_hugepages
echo 10000000001 | sudo tee /proc/sys/kernel/shmmax
echo 10000000001 | sudo tee /proc/sys/kernel/shmall

ssh-keyscan -H github.com >> ~/.ssh/known_hosts
# git clone https://github.com/ease-lab/Hermes hermes
# git clone git@github.com:akatsarakis/hermes-async.git hermes
cd

# Mount /dev/sdb2 as an ext4 directory
sudo mkfs.ext4 /dev/sdb # Format
sudo mkdir -p /mnt/mydisk # Make mount point directory
sudo mount /dev/sdb /mnt/mydisk # Mount it
sudo chown -R aaditya /mnt/mydisk
chmod ug+rwx /mnt/mydisk
mkdir /mnt/mydisk/splinterdb
mkdir /mnt/mydisk/stats
UUID=$(sudo blkid /dev/sdb | grep -oP 'UUID="\K[^"]+') # Get the device UUID
echo "UUID=$UUID /mnt/mydisk ext4 defaults 0 2" | sudo tee -a /etc/fstab # Append it to FSTAB so it persists

# Mount /dev/sdb2 as an ext4 directory
sudo mkfs.ext4 /dev/sdc # Format
sudo mkdir -p /users/aaditya/ext4 # Make mount point directory
sudo mount /dev/sdc /users/aaditya/ext4 # Mount it
sudo chown -R aaditya /users/aaditya/ext4
chmod ug+rwx /users/aaditya/ext4
UUID=$(sudo blkid /dev/sdc | grep -oP 'UUID="\K[^"]+') # Get the device UUID
echo "UUID=$UUID /users/aaditya/ext4 ext4 defaults 0 2" | sudo tee -a /etc/fstab # Append it to FSTAB so it persists

cd /users/aaditya/ext4

git clone https://github.com/sohambagchi/Odyssey odyssey
# cd odyssey ; git submodule update --init ; cd
# tar -xvf ${ARCHIVE_NAME}
cd odyssey ; git checkout merged_code ; sh ./bin/install-latest-cmake.sh ; cmake -B build
# TODO ALSO copy and run install-latest-cmake.sh in n1 and then run the following
#  cd odyssey; cmake -B build

# The below lines build the .so files required for running Odyssey with B+-tree and SplinterDB
splinterdb_folder="/users/aaditya/ext4/odyssey/splinterdb/"
export COMPILER=gcc
sudo apt update -y
sudo apt install -y libaio-dev libconfig-dev libxxhash-dev $COMPILER
export CC=$COMPILER
export LD=$COMPILER
cd $splinterdb_folder
make VERBOSE=1
cd ..

# For B+-tree
bplus_folder="/users/aaditya/ext4/odyssey/bplus-tree/"
cd $bplus_folder
CC=gcc
CSTDFLAG="--std=c99 -pedantic -Wall -Wextra -Wno-unused-parameter"
CPPFLAGS="-c -fpic -Iinclude -Iexternal/snappy"
CPPFLAGS+=" -D_LARGEFILE_SOURCE -D_FILE_OFFSET_BITS=64 -D_XOPEN_SOURCE=500 -D_DARWIN_C_SOURCE"
LDFLAGS="-lpthread"
SRC_DIR=src
INCLUDE_DIR=include
OBJ_DIR=obj

for file in "$SRC_DIR"/*.c; do
    base_name=$(basename "$file" .c)
    $CC $CSTDFLAG $CPPFLAGS $LDFLAGS -I"$INCLUDE_DIR" "$file" -o "$OBJ_DIR/$base_name.o"
done

$CC -shared -o libbplustree.so "$OBJ_DIR"/*.o


sleep 10 # if we try to init nic immediately it typically fails

# Setting the ip to the ib0 might not work on the first try so repeat
MAX_RETRIES=10
for i in `seq 1 ${MAX_RETRIES}`; do
  sudo ifconfig ib0 10.0.4.${HOSTNAME:5:1}/24 up
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

sleep 5




# [Optionally] For dbg ensure everything was configured properly
#ibdev2netdev # --> must show ib0 (up)
#ifconfig --> expected ib0 w/ expected ip
#ibv_devinfo --> PORT_ACTIVE

#############################
# WARNING only on first node!
#############################
if [[ "${HOSTNAME:5:1}" == 1 ]]; then
    sleep 20 # give some time so that all peers has setup their NICs

    git config --global user.name "Soham Bagchi"
    git config --global user.email "sohambagchi@outlook.com"

    # start a subnet manager
    sudo /etc/init.d/opensmd start # there must be at least one subnet-manager in an infiniband subnet cluster

    # Add all cluster nodes to known hosts
    # WARNING: execute this only after all nodes have setup their NICs (i.e., ifconfig up above)
    for i in `seq 1 ${NO_NODES}`; do
      ssh-keyscan -H 10.0.3.${i} >> ~/.ssh/known_hosts
    done
fi

