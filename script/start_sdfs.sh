#!/bin/bash

#TODO: open the sdfs
echo start sdfs on VM10...
ssh $USER@fa22-cs425-2210.cs.illinois.edu "cd mp4/sdfs; exec -a sdfs python3 server.py" &
sleep 0.5

for val in {1..9}
do
    echo start sdfs on VM$val...
    ssh $USER@fa22-cs425-220$val.cs.illinois.edu "cd mp4/sdfs; exec -a sdfs python3 server.py" &
    sleep 0.5
done 

echo SDFS started!




