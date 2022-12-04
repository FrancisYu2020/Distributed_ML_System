#!/bin/bash

for val in {1..9}
do
    ssh tian23@fa22-cs425-220$val.cs.illinois.edu "pip3 install torch torchvision torchaudio --extra-index-url https://download.pytorch.org/whl/cpu; pip3 install scipy; pip3 install tqdm"
done
ssh tian23@fa22-cs425-2210.cs.illinois.edu "pip3 install torch torchvision torchaudio --extra-index-url https://download.pytorch.org/whl/cpu; pip3 install scipy; pip3 install tqdm"
