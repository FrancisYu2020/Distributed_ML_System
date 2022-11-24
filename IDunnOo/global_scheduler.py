import logging
import zerorpc
from glob_var import *


logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s.%(msecs)03d %(levelname)s {%(module)s} [%(funcName)s] %(message)s',
                    datefmt='%Y-%m-%d,%H:%M:%S',
                    filename='IDunno_logs.log',
                    filemode='w')


class GlobalScheduler:
    """Global Scheduler (GS) is used to try to make every component stateless.

    Recv tasks from driver and assign workers to execute tasks.

    Attributes:
        rpc_server: The server to provied scheduler service, implemented by zerorpc.
    """

    def __init__(self) -> None:
        """Initial a GlobalScheduler object.

        Args:
            None

        Returns:
            None
        """
        self.rpc_server = zerorpc.Server()
        self.rpc_server.bind(
            "tcp://{}:{}".format(GLOBAL_SCHEDULER_HOST, GLOBAL_SCHEDULER_PORT))

    def select_worker(self) -> str:
        """Worker select strategy.

        Select worker based on execute rate.

        Args:
            None

        Returns:
            str: Selected worker host.
        """
        # TODO finish select part

    def sub_task(self, func_id, params_id) -> str:
        """Recv a task from driver.

        Receive a task from driver and use rpc to call worker execute it.

        Args:
            func_id (str): The id of func object.
            params_id (list): A list of params object id.

        Returns:
            str: The result id of task.
        """
        worker = self.select_worker()
        c = zerorpc.Client("tcp://{}:{}".format(worker, WORKER_PORT))
        res_id = c.recv_task(func_id, params_id)
        return res_id

    def run(self) -> None:
        """Start running the Global Scheduler.

        Args:
            None

        Returns:
            None
        """
        self.rpc_server.run()


if __name__ == "__main__":
    gs = GlobalScheduler()
    gs.run()
