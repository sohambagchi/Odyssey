#!/bin/bash
export COMPILER=gcc
sudo apt update -y
sudo apt install -y libaio-dev libconfig-dev libxxhash-dev $COMPILER
export CC=$COMPILER
export LD=$COMPILER
make VERBOSE=1
