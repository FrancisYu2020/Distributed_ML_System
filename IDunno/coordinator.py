import zerorpc
import socket
from glob_var import *
from collections import deque, defaultdict
import pickle
import time


class Coordinator:
    def __init__(self):
        self.task_q = deque()
        self.worker_states = {}
        self.max_q_size = 0
        self.hostname = socket.gethostname()
        self.results = defaultdict(dict)
        self.dash = defaultdict(int)
        self.timers = defaultdict(list)
        self.prev_time = defaultdict(float)
        self.sync_c = zerorpc.Client(
            f'tcp://{HOT_STANDBY_COORDINATOR_HOST}:{COORDINATOR_PORT}')

    def log(func):
        def log_func(*args, **kwargs):
            logging.info(f'Enter func: {func.__name__}.')
            ret = func(*args, **kwargs)
            logging.info(f'Exit func: {func.__name__}.')
            return ret
        return log_func

    def submit_task(self, task_id, model_id, params):
        if len(self.task_q) >= self.max_q_size:
            return False
        logging.info(f'Get {model_id} query.')
        self.task_q.append((task_id, model_id, params))
        if self.hostname == COORDINATOR_HOST:
            self.sync_c.submit_task(task_id, model_id, params)
        return True

    @log
    def resubmit_fail_task(self, task_id, model_id, params):
        self.task_q.appendleft((task_id, model_id, params))
        logging.info(f'Resubmit failed {model_id} query task {task_id}.')
        if self.hostname == COORDINATOR_HOST:
            self.sync_c.resubmit_fail_task(task_id, model_id, params)
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
        logging.info(f'Worker {worker} get {model_id} query task {task_id}.')
        if self.hostname == COORDINATOR_HOST:
            self.sync_c.poll_task(worker)
        return (task_id, model_id, params)

    @log
    def commit_task(self, model_id, task_id, worker, bytes_res):
        res = pickle.loads(bytes_res)
        print(f'The Result of taks {task_id} is: {res}')
        self.worker_states[worker] = None
        if task_id not in self.results[model_id]:
            self.dash[model_id] += 1
            self.results[model_id][task_id] = res
            curr_time = time.time()
            self.timers[model_id].append(curr_time - self.prev_time[model_id])
            self.prev_time[model_id] = curr_time

        logging.info(f'Worker {worker} commit task {task_id}')
        if self.hostname == COORDINATOR_HOST:
            self.sync_c.commit_task(model_id, task_id, worker, bytes_res)
        return

    @log
    def failure_handle(self, worker):
        if worker in self.worker_states and self.worker_states[worker]:
            task_id, model_id, params = self.worker_states[worker]
            logging.warning(
                f'Worker {worker} failed with {model_id} query task {task_id}.')
            self.resubmit_fail_task(task_id, model_id, params)

            if self.hostname == COORDINATOR_HOST:
                self.sync_c.failure_handle(worker)
        self.max_q_size -= 1
        del self.worker_states[worker]
        return

    @log
    def add_worker(self, worker):
        self.max_q_size += 1
        logging.info(f'New worker {worker} added.')
        self.worker_states[worker] = None

        if self.hostname == COORDINATOR_HOST:
            self.sync_c.add_worker(worker)
        return

    def get_dash(self):
        return pickle.dumps(self.dash), pickle.dumps(self.timers)

    def get_res(self):
        return pickle.dumps(self.results)

    def get_worker_states(self):
        ret = {}
        for worker, info in self.worker_states.items():
            if info:
                ret[worker] = info[1]
            else:
                ret[worker] = "Idle"
        return pickle.dumps(ret)


if __name__ == "__main__":
    s = zerorpc.Server(Coordinator())
    s.bind(f'tcp://0.0.0.0:{COORDINATOR_PORT}')
    s.run()
