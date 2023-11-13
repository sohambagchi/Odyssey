#!/usr/bin/env bash

FILE_PATH_TO_SPLIT="./mellanox/MLNX_OFED_LINUX-5.0-2.1.8.0-ubuntu18.04-x86_64.tgz"
split -b 20M ${FILE_PATH_TO_SPLIT} "${FILE_PATH_TO_SPLIT}.part"

#FILE_PATH_TO_SPLIT="./mellanox/MLNX_OFED_LINUX-5.2-2.2.0.0-ubuntu18.04-x86_64.tgz"
#split -b 20M ${FILE_PATH_TO_SPLIT} "${FILE_PATH_TO_SPLIT}.part"
