#!/bin/bash

for val in {1..9}
do
    echo VM$val Updating
    ssh tian23@fa22-cs425-220$val.cs.illinois.edu "cd mp4; cd sdfs; rm -rf store"
    echo VM$val Updated
done
echo VM10 Updating
ssh tian23@fa22-cs425-220$val.cs.illinois.edu "cd mp4; cd sdfs; rm -rf store"
echo VM10 Updated

echo "-----------All VMs Have Been Updated!------------"