import zerorpc
import time
import socket
from glob_var import *
from fd import Server as FDServer
from sdfs_shell import SDFShell
from threading import Thread
from DNN import *
import pickle


class Worker:
    def __init__(self):
        self.cache = {}
        self.name = socket.gethostname()
        self.fd = FDServer()
        self.host = COORDINATOR_HOST
        self.client = zerorpc.Client(f'tcp://{self.host}:{COORDINATOR_PORT}')

    def log(func):
        def log_func(*args, **kwargs):
            logging.info(f'Enter func: {func.__name__}.')
            ret = func(*args, **kwargs)
            logging.info(f'Exit func: {func.__name__}.')
            return ret
        return log_func

    def __wait(self):
        time.sleep(0)

    @log
    def get_task(self):
        task_params = None
        try:
            while not task_params:
                task_params = self.client.poll_task(self.name)
                self.__wait()
        except Exception as e:
            self.host = HOT_STANDBY_COORDINATOR_HOST
            self.client = zerorpc.Client(
                f'tcp://{self.host}:{COORDINATOR_PORT}')
            while not task_params:
                task_params = self.client.poll_task(self.name)
                self.__wait()
        task_id, model_id, data = task_params
        logging.info(f'Get {model_id} query task {task_id}.')
        if model_id not in self.cache:
            model_name = SDFShell.get(model_id)
            self.cache[model_id] = Model(model_name)
        # params: data or other parameters
        res = self.exec_task(self.cache[model_id], data)
        res = pickle.dumps(res)

        try:
            self.client.commit_task(model_id, task_id, self.name, res)
        except:
            self.host = HOT_STANDBY_COORDINATOR_HOST
            self.client = zerorpc.Client(
                f'tcp://{self.host}:{COORDINATOR_PORT}')
            self.client.commit_task(model_id, task_id, self.name, res)
        logging.info(f'Commit {model_id} query task {task_id}.')

    def import_data(self, filelist):
        # 0-index
        img_list = [read_image(img)
                    for img in filelist]
        return img_list

    @log
    def exec_task(self, model, data):
        filelist = data
        data = self.import_data(filelist)
        model.load_data(data)
        return model.predict()

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
