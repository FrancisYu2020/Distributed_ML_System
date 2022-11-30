#!/bin/bash

for val in {1..9}
do
    echo VM$val Updating
    ssh tian23@fa22-cs425-220$val.cs.illinois.edu "rm -rf mp4-dnn; git clone -b dash-board git@gitlab.engr.illinois.edu:hangy6/mp4-hangy6-tian23.git mp4-dnn"
    echo VM$val Updated
done
echo VM10 Updating
ssh tian23@fa22-cs425-2210.cs.illinois.edu "rm -rf mp4-dnn; git clone -b dash-board git@gitlab.engr.illinois.edu:hangy6/mp4-hangy6-tian23.git mp4-dnn"
echo VM10 Updated

echo "-----------All VMs Have Been Updated!------------"