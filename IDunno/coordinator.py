import zerorpc
import socket
from glob_var import *
from collections import deque


class Coordinator:
    def __init__(self):
        self.task_q = deque()
        self.worker_states = {}
        self.max_q_size = 0
        self.hostname = socket.gethostname()

    def debug(func):
        def deco_func(*args, **kwargs):
            print(f'Enter function: {func.__name__}.')
            func(*args, **kwargs)
            print(f'Finish function: {func.__name__}.')
        return deco_func

    def submit_task(self, task_id, model_id, params):
        if len(self.task_q) >= self.max_q_size:
            return False
        self.task_q.append((task_id, model_id, params))
        if self.hostname == COORDINATOR_HOST:
            c = zerorpc.Client(
                f'tcp://{HOT_STANDBY_COORDINATOR_HOST}:{COORDINATOR_PORT}')
            c.submit_task(task_id, model_id, params)
        # print(f'Get task {task_id}')
        return True

    def resubmit_fail_task(self, task_id, model_id, params):
        self.task_q.appendleft((task_id, model_id, params))

        if self.hostname == COORDINATOR_HOST:
            c = zerorpc.Client(
                f'tcp://{HOT_STANDBY_COORDINATOR_HOST}:{COORDINATOR_PORT}')
            c.resubmit_fail_task(task_id, model_id, params)
        return

    def poll_task(self, worker):
        try:
            if worker in self.worker_states and self.worker_states[worker]:
                task_id, model_id, params = self.worker_states[worker]
            else:
                task_id, model_id, params = self.task_q.popleft()
                self.worker_states[worker] = (task_id, model_id, params)
        except IndexError as err:
            # print(err)
            return None

        if self.hostname == COORDINATOR_HOST:
            c = zerorpc.Client(
                f'tcp://{HOT_STANDBY_COORDINATOR_HOST}:{COORDINATOR_PORT}')
            c.poll_task(worker)
        return (task_id, model_id, params)

    def commit_task(self, task_id, worker, res):
        print(f'The Result of taks {task_id} is: {res}')
        self.worker_states[worker] = None

        if self.hostname == COORDINATOR_HOST:
            c = zerorpc.Client(
                f'tcp://{HOT_STANDBY_COORDINATOR_HOST}:{COORDINATOR_PORT}')
            c.commit_task(task_id, worker, res)
        return

    def failure_handle(self, worker):
        if worker in self.worker_states and self.worker_states[worker]:
            task_id, model_id, params = self.worker_states[worker]
            self.resubmit_fail_task(task_id, model_id, params)

            if self.hostname == COORDINATOR_HOST:
                c = zerorpc.Client(
                    f'tcp://{HOT_STANDBY_COORDINATOR_HOST}:{COORDINATOR_PORT}')
                c.failure_handle(worker)

        self.max_q_size -= 1
        del self.worker_states[worker]
        return

    def add_worker(self, worker):
        if worker not in self.worker_states:
            print("Add worker!")
            self.max_q_size += 1
            self.worker_states[worker] = None

        if self.hostname == COORDINATOR_HOST:
            c = zerorpc.Client(
                f'tcp://{HOT_STANDBY_COORDINATOR_HOST}:{COORDINATOR_PORT}')
            c.add_worker(worker)
        return


if __name__ == "__main__":
    s = zerorpc.Server(Coordinator())
    s.bind(f'tcp://0.0.0.0:{COORDINATOR_PORT}')
    s.run()
