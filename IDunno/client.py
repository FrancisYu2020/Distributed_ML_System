import time
from glob_var import *
import zerorpc
from sdfs_shell import SDFShell
import random
import collections

if __name__ == "__main__":
    i = 0
    model_id = SDFShell.put(TestModel())
    another_model_id = SDFShell.put(AnotherModel())
    round_robin_queue = collections.deque()
    round_robin_queue.append(model_id)
    round_robin_queue.append(another_model_id)

    c = zerorpc.Client(f'tcp://{COORDINATOR_HOST}:{COORDINATOR_PORT}')
    for i in range(100):
        task_id = "Work {}".format(i)
        model = round_robin_queue.popleft()
        round_robin_queue.append(model)
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
