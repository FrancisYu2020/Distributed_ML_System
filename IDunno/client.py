import time
from glob_var import *
import zerorpc
from sdfs_shell import SDFShell
import random

if __name__ == "__main__":
    i = 0
    model_id = SDFShell.put(TestModel())
    c = zerorpc.Client(f'tcp://{COORDINATOR_HOST}:{COORDINATOR_PORT}')
    for i in range(100):
        task_id = "Work {}".format(i)
        try:
            while True:
                if c.submit_task(task_id, model_id, random.random()):
                    print(f'Submit task {task_id} successful.')
                    break
                time.sleep(0.2)
        except:
            c = zerorpc.Client(
                f'tcp://{HOT_STANDBY_COORDINATOR_HOST}:{COORDINATOR_PORT}')
            while True:
                if c.submit_task(task_id, model_id, random.random()):
                    print(f'Submit task {task_id} successful.')
                    break
                time.sleep(0.2)
