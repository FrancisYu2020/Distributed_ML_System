import logging
import zerorpc
from glob_var import *
from global_control_store import GlobalControlState as GCS
from global_control_store import TaskTable



logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s.%(msecs)03d %(levelname)s {%(module)s} [%(funcName)s] %(message)s',
                    datefmt='%Y-%m-%d,%H:%M:%S',
                    filename='IDunno_GS_logs.log',
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
        self.rpc_server = zerorpc.Server(self)
        self.rpc_server.bind(
            "tcp://0.0.0.0:{}".format(GLOBAL_SCHEDULER_PORT))
    
    def heartbeat(self) -> None:
        """Heartbeat func to detect survive, basically do nothing.

        Args:
            None

        Returns:
            None
        """

    def select_worker(self) -> str:
        """Worker select strategy.

        Select worker based on execute rate.

        Args:
            None

        Returns:
            str: Selected worker host.
        """
        # TODO: finish select part
        # fetch task table
        task_t = GCS.get(TASK_TABLE_NAME)
        if not task_t:
            task_t = TaskTable()
        # TODO: complete modify task_t
        # modify task_t...
        GCS.put(task_t)
        worker = "fa22-cs425-2205.cs.illinois.edu"
        return worker

    def sub_task(self, func_id: str, params_id: list) -> str:
        """Recv a task from driver.

        Receive a task from driver and use rpc to call worker execute it.

        Args:
            func_id (str): The id of func object.
            params_id (list): A list of params object id.

        Returns:
            str: The result id of task.
        """
        logging.info("Recv task from driver.")
        worker = self.select_worker()
        c = zerorpc.Client("tcp://{}:{}".format(worker, WORKER_PORT))
        logging.info("Assigned task to {}.".format(worker))
        res_id = c.recv_task(func_id, params_id)
        return res_id

    def run(self) -> None:
        """Start running the Global Scheduler.

        Args:
            None

        Returns:
            None
        """
        logging.info("Global Scheduler started.")
        self.rpc_server.run()


if __name__ == "__main__":
    gs = GlobalScheduler()
    gs.run()
