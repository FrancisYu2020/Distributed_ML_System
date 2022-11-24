import zerorpc
import logging
import pickle
from global_control_store import GlobalControlState as GCS
import uuid
from threading import Thread
from glob_var import *


class Worker:
    """Global Scheduler (GS) is used to try to make every component stateless.

    Recv tasks from driver and assign workers to execute tasks.

    Attributes:
        worker_port (int): The port num of worker.
        rpc_server (zerorpc.Server): The server to provied worker service, implemented by zerorpc.
    """

    def __init__(self, worker_port) -> None:
        """Initial a Worker object.

        Args:
            worker_port (int): The port num of worker.

        Returns:
            None
        """
        self.port = worker_port
        self.rpc_server = zerorpc.Server(self)
        self.rpc_server.bind("tcp://0.0.0.0:{}".format(self.port))

    def recv_task(self, func_id, param_ids) -> str:
        """Receive the task, start a thread to execute the task and return the result object id.

        Args:
            func_id (str): The id of function obj in GCS.
            param_id (str): The id of param obj in GCS.

        Returns:
            str: The result id of result.
        """
        res_id = uuid.uuid1().hex
        t = Thread(target=self.exec_task, args=(func_id, param_ids, res_id))
        t.start()
        return res_id

    def exec_task(self, func_id, param_ids, res_id) -> None:
        """Execute task.

        Args:
            func_id (str): The id of function obj in GCS.
            param_id (str): The id of param obj in GCS.
            res_id (str): The id of result obj to be stored in GCS.

        Returns:
            None
        """
        # get function
        bytes_func = GCS.get_obj(func_id)
        func = pickle.loads(bytes_func)
        # get parameters
        params = []
        for param_id in param_ids:
            bytes_param = GCS.get_obj(param_id)
            params.append(pickle.loads(bytes_param))
        # execute function
        res = func(params)
        # write result to GCS
        bytes_res = pickle.dumps(res)
        GCS.put_obj(bytes_res, res_id)

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
