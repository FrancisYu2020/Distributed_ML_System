import zerorpc
import logging
from global_control_store import GlobalControlState as GCS
import uuid
from threading import Thread
from glob_var import *
from DNNs import *


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

    def recv_task(self, func_id: str, param_ids: list) -> str:
        """Receive the task, start a thread to execute the task and return the result object id.

        Args:
            func_id (str): The id of function obj in GCS.
            param_id (str): The id of param obj in GCS.

        Returns:
            str: The result id of result.
        """
        res_id = uuid.uuid1().hex
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
        logging.info("Start to fetch func object from GCS.")
        func = None
        while not func:
            func = GCS.get(func_id)
        logging.info("Func object fetched.")
        # get parameters
        logging.info("Start to fetch params objects from GCS.")
        params = []
        for i, param_id in enumerate(param_ids):
            param = None
            while not param:
                logging.info("Start to fetch {} param object".format(i))
                param = GCS.get(param_id)
                logging.info("{} param object fetched".format(i))
                params.append(param)
        logging.info("All params have been fetched.")
        # execute function
        logging.info("Exectue function.")
        res = func(params)
        # write result to GCS
        logging.info("Finish executing func. Start to store result to GCS.")
        GCS.put(res, res_id)
        logging.info("Result stored.")

    def run(self) -> None:
        """Run the worker process.

        Args:
            None

        Returns:
            None
        """
        logging.info("A worker started.")
        self.rpc_server.run()


if __name__ == "__main__":
    w = Worker(WORKER_PORT)
    w.run()
