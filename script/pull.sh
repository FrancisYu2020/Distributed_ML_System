#!/bin/bash

git add ..
git commit -m "commit change"
git push origin auto
echo VM10 Updating
ssh hangy6@fa22-cs425-2210.cs.illinois.edu "cd mp4; git checkout auto; git pull origin auto" 
echo VM10 Updated
for val in {1..9}
do
    echo VM$val Updating
    ssh hangy6@fa22-cs425-220$val.cs.illinois.edu "cd mp4; git checkout auto; git pull origin auto"
    echo VM$val Updated
done


echo "-----------All VMs Have Been Updated!------------"