#!/bin/bash

echo VM10 Updating
ssh tian23@fa22-cs425-2210.cs.illinois.edu "cd mp4-refac; git pull"
echo VM10 Updated
for val in {1..9}
do
    echo VM$val Updating
    ssh tian23@fa22-cs425-220$val.cs.illinois.edu "cd mp4-refac; git pull"
    echo VM$val Updated
done


echo "-----------All VMs Have Been Updated!------------"