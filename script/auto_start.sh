#!/bin/bash

#TODO: open the sdfs
for val in {1..9}
do
    echo start sdfs on VM$val...
    ssh hangy6@fa22-cs425-220$val.cs.illinois.edu "cd mp4/sdfs; python3 server.py &"
done 

echo start sdfs on VM10...
ssh hangy6@fa22-cs425-2210.cs.illinois.edu "cd mp4/sdfs; python3 server.py &"
echo SDFS started!

#TODO: open the coordinator and hot standby

#TODO: open the introducer and  workers
# for val in {1..9}
# do
#     echo VM$val Updating
#     ssh hangy6@fa22-cs425-220$val.cs.illinois.edu "cd mp4/IDunno; pkill python"
#     echo VM$val Updated
# done
# echo VM10 Updating
# ssh tian23@fa22-cs425-2210.cs.illinois.edu "cd mp4; pkill python"
# echo VM10 Updated

# echo "-----------All VMs Have Been Updated!------------"
