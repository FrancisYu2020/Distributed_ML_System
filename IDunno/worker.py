import zerorpc
import time
import socket
from glob_var import *
from fd import Server as FDServer
from sdfs_shell import SDFShell
from threading import Thread
import DNN
import pickle


class Worker:
    def __init__(self):
        self.cache = {}
        self.name = socket.gethostname()
        self.fd = FDServer()
        self.host = COORDINATOR_HOST

    def debug(func):
        def deco_func(*args, **kwargs):
            print(f'Enter function: {func.__name__}.')
            func(*args, **kwargs)
            print(f'Enter function: {func.__name__}.')
        return deco_func

    def __wait(self):
        time.sleep(0.25)

    def get_task(self):
        task_params = None
        try:
            c = zerorpc.Client(f'tcp://{self.host}:{COORDINATOR_PORT}')
            while not task_params:
                task_params = c.poll_task(self.name)
                self.__wait()
        except Exception as e:
            self.host = HOT_STANDBY_COORDINATOR_HOST
            c = zerorpc.Client(f'tcp://{self.host}:{COORDINATOR_PORT}')
            while not task_params:
                task_params = c.poll_task(self.name)
                self.__wait()
        task_id, model_id, data = task_params
        if model_id not in self.cache:
            self.cache[model_id] = Model(model_id)
        # params: data or other parameters
        res = self.exec_task(self.cache[model_id], data)

        try:
            c.commit_task(task_id, self.name, res)
        except:
            self.host = HOT_STANDBY_COORDINATOR_HOST
            c = zerorpc.Client(f'tcp://{self.host}:{COORDINATOR_PORT}')
            c.commit_task(task_id, self.name, res)

    def exec_task(self, model, data):
        data = pickle.loads(data)
        model.load_data(data)
        return model.predict(data)

    def run(self):
        while True:
            self.get_task()


def run() -> None:
    w = Worker()
    fd_t = Thread(target=w.fd.run, name="fd", daemon=True)
    fd_t.start()
    print("Worker failure detector started.")
    print("Worker rpc service started.")
    print("A worker started.")
    w.run()
    print("Should not display")

    fd_t.join()
    # w.rpc_server.run()


if __name__ == "__main__":
    run()
