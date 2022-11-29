import time
from glob_var import *
import zerorpc
from sdfs_shell import SDFShell
import random
import collections
from DNN import *
# from torchvision.io import read_image

def import_data(filelist, start, end):
    # 0-index
    img_list = [read_image(filelist[img_idx]) for img_idx in range(start, end)]
    return img_list

if __name__ == "__main__":
    filelist = os.listdir('imageNet/val')
    filelist.sort()
    model_id = SDFShell.put('resnet18')
    another_model_id = SDFShell.put('alexnet')
    round_robin_queue = collections.deque()
    round_robin_queue.append(model_id)
    round_robin_queue.append(another_model_id)

    c = zerorpc.Client(f'tcp://{COORDINATOR_HOST}:{COORDINATOR_PORT}')
    i = 1
    while i:
        #TODO: add interactive shell
        job_name = "resnet18" if round_robin_queue[0] == model_id else "alexnet"
        task_id = f"query {i} for job {job_name}"
        i += 1
        model = round_robin_queue.popleft()
        round_robin_queue.append(model)
        try:
            while True:
                #TODO: write import_data() and finalize input img_idx
                if c.submit_task(task_id, model, import_data(filelist, 0, 10)):
                    print(f'Submit task {task_id} for {model} successful.')
                    break
                else:
                    time.sleep(0.2)
        except:
            c = zerorpc.Client(
                f'tcp://{HOT_STANDBY_COORDINATOR_HOST}:{COORDINATOR_PORT}')
            while True:
                if c.submit_task(task_id, model, import_data(filelist, 0, 10)):
                    print(f'Submit task {task_id} for {model} successful.')
                    break
                else:
                    time.sleep(0.2)
