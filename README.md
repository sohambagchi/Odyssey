# SplinteRDMA: Taking VMWare's Best To The Future Of Cluster Communication.

Our project explores the implications of replacing MICA with 2 tree based key-value stores. We have used a [B+-tree implementation](https://github.com/embedded2016/bplus-tree), and [SplinterDB](https://github.com/vmware/splinterdb).

This document specifies instructions on how to set the system up on an RDMA cluster and run experiments.

## Contributors
| Name                | Contributions                                  |
| ------------------- | -----------------------------------------      |
| Aaditya Ranga Rajan | SplinterDB Integration                         |
| Ankith Boggaram     | B+-tree integration                            | 
| Soham Bagchi        | Cluster setup, metrics evaluation and analysis |

## Setup Details
Our experiments were run on cloudlab. We used 5 nodes of the r320 class, each with a Mellanox NIC with RDMA capabilities. <Fill in information regarding RDMA image>.

In order to setup the project, please run the init-preimaged.sh script available in the repo. It will download the necessary dependencies such as CMake, and will also create an ext4 partition on your device. Please make sure to edit the paths mentioned in the script.

Once this is done, please change the IPs in bin/cluster.sh. These should be the IPs of your nodes. Also, there is a variable called $OD\_HOME. Please set this to your Odyssey directory.

On your main node, please run the following commands
```
cmake -B build
./bin/copy-run.sh -x hermes
```

This should build the executable and copy it to all the other nodes in your cluster. Now, on your other nodes, execute this
```
./bin/run-exe.sh -x build/hermes
```
Once you have executed this command on all the nodes successfully, Odyssey should be up and running.It will proceed to run YCSB workload A. This runs MICA as the KVS by default. If you would like to run B+-tree or SplinterDB, please make the following change.

In odlib/include/general\_util/od\_top.h, (lines 92 - 94), you will find 3 macros.
```
#define USE_MICA 1 
#define USE_BPLUS 0
#define USE_SPLINTERDB 0
```

If you want to use SplinterDB, set that macro to 1. **Please remember that only one of these can be set to 1 at a time, or else the code will not run.**

## Future Work
There is still room for improvement in the code. The existing Odyssey code is not the best in terms of documentation, and that can certainly be improved. A direction for potential research is integration an adaptive Be-tree and running the experiments again. Feel free to contribute!



