import zerorpc
import logging
from global_control_store import GlobalControlState as GCS
import uuid
from threading import Thread
from glob_var import *
from DNNs import *
from fd import Server as FDServer
from threading import Thread

logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s.%(msecs)03d %(levelname)s {%(module)s} [%(funcName)s] %(message)s',
                    datefmt='%Y-%m-%d,%H:%M:%S',
                    filename='IDunno_worker_logs.log',
                    filemode='w')


class Worker:
    """Global Scheduler (GS) is used to try to make every component stateless.

    Recv tasks from driver and assign workers to execute tasks.

    Attributes:
        worker_port: The port num of worker.
        rpc_server: The server to provied worker service, implemented by zerorpc.
        fd: The failure detector used to monitor if worker is alive.
        cache: Used to cache the fetched object for efficiency.
    """

    def __init__(self, worker_port: int) -> None:
        """Initial a Worker object.

        Args:
            worker_port (int): The port num of worker.

        Returns:
            None
        """
        self.port = worker_port
        self.rpc_server = zerorpc.Server(self)
        self.rpc_server.bind("tcp://0.0.0.0:{}".format(self.port))
        self.fd = FDServer()
        self.cache = {}

    def recv_task(self, func_id: str, param_ids: list, t_id: str = None) -> str:
        """Receive the task, start a thread to execute the task and return the result object id.

        Args:
            func_id (str): The id of function obj in GCS.
            param_id (str): The id of param obj in GCS.

        Returns:
            str: The result id of result.
        """
        res_id = uuid.uuid1().hex if not t_id else t_id
        logging.info("Start execute task.")
        t = Thread(target=self.exec_task, args=(func_id, param_ids, res_id))
        t.start()
        return res_id

    def exec_task(self, func_id: str, param_ids: list, res_id: str) -> None:
        """Execute task.

        Args:
            func_id (str): The id of function obj in GCS.
            param_id (str): The id of param obj in GCS.
            res_id (str): The id of result obj to be stored in GCS.

        Returns:
            None
        """
        # get function
        func = None
        if func_id not in self.cache:
            logging.info("Func has not been cached, start to fetch func object from GCS.") 
            while not func:
                func = GCS.get(func_id)
            logging.info("Func object fetched.")
            self.cache[func_id] = func
            logging.info("Func has been cached successfully.")
        else:
            logging.info("Find func from cache.") 
            func = self.cache[func_id]

        # get parameters
        logging.info("Start to fetch params objects from GCS.")
        params = []
        for i, param_id in enumerate(param_ids):
            param = None
            if param_id not in self.cache:
                logging.info("Param {} has not been cached, start to fetch it.".format(i))
                while not param:
                    param = GCS.get(param_id)
                logging.info("The {} param object fetched".format(i))
                self.cache[param_id] = param
                logging.info("Param {} has been cached successfully.".format(i))
            else:
                logging.info("Find param {} from cache.".format(i))
                param = self.cache[param_id]
            params.append(param)

        logging.info("All params have been loaded, start to execute function.")
        # execute function
        res = func(params)
        # write result to GCS
        logging.info("Finish executing func. Start to store result to GCS.")
        GCS.put(res, res_id)
        logging.info("Result stored.")


def run() -> None:
    """Run the worker process.

    Args:
        None

    Returns:
        None
    """
    w = Worker(WORKER_PORT)
    # rpc_t = Thread(target = w.rpc_server.run, name="rpc")
    fd_t = Thread(target=w.fd.run, name="fd", daemon=True)

    fd_t.start()
    print("Worker failure detector started.")
    logging.info("Worker failure detector started.")

    print("A worker started.")
    logging.info("A worker started.")

    # rpc_t.start()

    print("Worker rpc service started.")
    logging.info("Worker rpc service started.")
    w.rpc_server.run()
    print("Should not display")

    fd_t.join()
    # w.rpc_server.run()


if __name__ == "__main__":
    run()
