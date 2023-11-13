#!/usr/bin/env bash

FILE_PATH_TO_MERGE="./mellanox/MLNX_OFED_LINUX-5.0-2.1.8.0-ubuntu18.04-x86_64.tgz"
cat ${FILE_PATH_TO_MERGE}.parta* >${FILE_PATH_TO_MERGE}

#FILE_PATH_TO_SPLIT="./mellanox/MLNX_OFED_LINUX-5.2-2.2.0.0-ubuntu18.04-x86_64.tgz"
#cat ${FILE_PATH_TO_MERGE}.parta* >${FILE_PATH_TO_MERGE}
