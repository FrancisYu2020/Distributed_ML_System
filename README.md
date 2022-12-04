# MP4-hangy6-tian23 IDunno, a Distributed Learning Cluster

## Description
We build on top of our previous implementation of swim-like ping ack failure detector and HDFS-like Simple Distributed File System (SDFS) to implement the distributed learning cluster, Illinois DNNs (IDunno). This Machine Learning platform provides training and inference features for a neural network or multiple networks simultaneously. You can implement your own network, train and do inference on IDunno. You can also skip the training phase by load pre-trained models and start model serving.

## Installation

You can clone this project to the machines you need to grep log from using following command:

```
ssh: git clone git@gitlab.engr.illinois.edu:hangy6/mp4-hangy6-tian23.git
```
```
https: git clone https://gitlab.engr.illinois.edu/hangy6/mp4-hangy6-tian23.git
``` 

## Setup
To use IDunno, you need to setup required enviroment first. To make this easy, we provide a requirements.txt, please use the following command in project, we assume you already installed some other requirements needed for your model, you can also use install.sh in script/ to install the requirement for pytorch easily. 

```
pip3 install -r requirements.txt
```

## Usage
To start the IDunno, first you need to start the SDFS, please go the machine you want to use as SDFS and execute following commands in sdfs/, you need at least four SDFS machines and you should start the introducer sdfs machine first:

```
python3 server.py
```

And use following command to join the memberlist of failure detector in SDFS, please notice that you need to first join the introducer before other nodes:
```
join
```

To list the current membership list, on any node run:
```
list_mem
```

Please notice that all following command should be executed in IDunno/

Then you need to start the introducer for IDunno, you can run following command:
```
python3 introducer.py
```

Now please open the coordinator and hot-stadby coordinator on specific machine by following commands:
```
python3 coordinator.py
```

You can start any amount of workers by following command in your worker machine, the worker will join the IDunno automatically:
```
python3 worker.py
```

Then you can run following command to start client:
```
python3 client.py
```

We support follwing commands:
```
sub <job name> <local data path> <batch size>
get-stats
job-rates
set-batch <job name> <batch size>
get-results
vm-states
kill <worker/coordinator> <vm id>
```

## Support
If you have any questions, please contact tian23@illinois.edu or hangy6@illinois.edu

## Authors 
Tian Luan & Hang Yu
