#!/bin/bash

#TODO: open the sdfs
echo start sdfs on VM10...
ssh $USER@fa22-cs425-2210.cs.illinois.edu "cd mp4/sdfs; python3 server.py" &
sleep 0.5

for val in {1..9}
do
    echo start sdfs on VM$val...
    ssh $USER@fa22-cs425-220$val.cs.illinois.edu "cd mp4/sdfs; python3 server.py" &
    sleep 0.5
done 

echo SDFS started!


#TODO: open the coordinator and hot standby
for val in {1..2}
do
    echo start coordinator/hot standby on VM$val...
    ssh $USER@fa22-cs425-220$val.cs.illinois.edu "cd mp4/IDunno; python3 coordinator.py" &
    sleep 0.5
done 

#TODO: open the introducer and  workers
echo start worker introducer on VM10...
ssh $USER@fa22-cs425-2210.cs.illinois.edu "cd mp4/IDunno; python3 introducer.py" &
sleep 0.5

for val in {3..9}
do
    echo start worker on VM$val...
    ssh $USER@fa22-cs425-220$val.cs.illinois.edu "cd mp4/IDunno; python3 worker.py" &
    sleep 0.5
done
echo start worker on VM10...
ssh $USER@fa22-cs425-2210.cs.illinois.edu "cd mp4/IDunno; python3 worker.py" &
echo VM10 Updated

# echo "-----------All VMs Have Been Updated!------------"
