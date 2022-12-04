#TODO: open the coordinator and hot standby
echo start hot standby on VM2...
ssh $USER@fa22-cs425-2202.cs.illinois.edu "cd mp4/IDunno; exec -a coordinator python3 coordinator.py" &
sleep 0.5
echo Hot standby loaded!

#TODO: open the workers
for val in {1..9}
do
    if [ $# -eq 0 ]
    then
        if [ $val -eq 3 ]
        then
            echo skip worker 3
            continue
        elif [ $val -eq 4 ]
        then
            echo skip worker 4
            continue
        fi
    else
        if [ $val -eq $1 ] || [ $val -eq $2 ]
        then
            echo skip worker $val
            continue
        fi
    fi
    echo start worker on VM$val...
    ssh $USER@fa22-cs425-220$val.cs.illinois.edu "cd mp4/IDunno; exec -a running_workers python3 worker.py" &
    sleep 0.5
done
echo start worker on VM10...
ssh $USER@fa22-cs425-2210.cs.illinois.edu "cd mp4/IDunno; exec -a running_workers python3 worker.py" &
echo all workers started!