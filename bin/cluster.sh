INFORMATICS="informatics"
CLOUDLAB="cloudlab"
SETUP=$CLOUDLAB


if [ $SETUP == $INFORMATICS ]; then
  echo "informatics"
  OD_HOME="/home/vasilis/Odyssey"
  HOSTS=(r1 r2 r3 )
  allIPs=( 10.204.236.218 
         10.204.236.219
	  10.204.236.220
        192.168.8.4 #houston
        192.168.8.6 #austin
        192.168.8.5 #sanantonio
        192.168.8.3 #indianapolis
        192.168.8.2 #philly
        192.168.5.11
        192.168.5.13
        )
  #localIP=$(ip addr | grep 'infiniband' -A2 | sed -n 2p | awk '{print $2}' | cut -f1  -d'/')
  localIPA=$(ip addr | grep 'ether' -A2 | sed -n 2p | awk '{print $2}' | cut -f1  -d'/')
  localIPB=$(ip addr | grep 'ether' -A2 | sed -n 4p | awk '{print $2}' | cut -f1  -d'/')
  localIPC=$(ip addr | grep 'ether' -A2 | sed -n 6p | awk '{print $2}' | cut -f1  -d'/') 
  NET_DEVICE_NAME="mlx5_0"
  IS_ROCE=1
else
  echo "cloudlab"
  OD_HOME="/users/sohamb/ankith/Odyssey"

  HOSTS=(
    10.0.4.1
    10.0.4.2
    10.0.4.3
    10.0.4.4
    10.0.4.5
  )

  allIPs=(
	10.0.4.1
	10.0.4.2
	10.0.4.3
	10.0.4.4
	10.0.4.5
	)

  localIP=$(ip addr | grep 'state UP' -A2 | grep 'inet 10.0.4'| awk '{print $2}' | cut -f1  -d'/')
  echo $localIP
  NET_DEVICE_NAME="mlx4_0"
  IS_ROCE=0
fi

##########################################
### NO NEED TO CHANGE BELOW THIS POINT ###
##########################################
REMOTE_IPS=${HOSTS[@]/$localIP}
REMOTE_HOSTS=${HOSTS[@]/$localIP}






### TO BE FILLED: Modify to get the local IP of the node running the script (must be one of the cluster nodes)
#cloudlab

#Informatics
#localIP=$(ip addr | grep 'infiniband' -A2 | sed -n 2p | awk '{print $2}' | cut -f1  -d'/')
#localIP=$(ip addr | grep 'ether' -A2 | sed -n 2p | awk '{print $2}' | cut -f1  -d'/')

### Fill the RDMA device name (the "hca_id" of the device when executing ibv_devinfo)
#NET_DEVICE_NAME="mlx4_0"  # cloudlab
#NET_DEVICE_NAME="mlx5_1" # informatics




