#!/bin/bash

for val in {1..9}
do
    echo VM$val Updating
    ssh hangy6@fa22-cs425-220$val.cs.illinois.edu "cd mp4; pkill python"
    echo VM$val Updated
done
echo VM10 Updating
ssh hangy6@fa22-cs425-2210.cs.illinois.edu "cd mp4; pkill python"
echo VM10 Updated

echo "-----------All VMs Have Been Updated!------------"