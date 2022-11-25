import logging
import zerorpc
from glob_var import *
from global_control_store import GlobalControlState as GCS
from global_control_store import WorkerTable


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

    def handle_fail_worker(self, worker: str) -> None:
        """Worker failed, start new thread to check if failed worker has task and reassign the failed task.

        Args:
            worker (str): The failed worker host name.

        Returns:
            None
        """
        logging.warning("Failed worker trigger: {}".format(worker))
        logging.info("Start to get Worker Table.")
        worker_t = GCS.get(WORKER_TABLE_NAME)
        logging.info("Worker Table received.")
        if not worker_t:
            logging.info("No task failed.")
            return
        if worker in worker_t and not GCS.query(worker_t.tab[worker].t_id):
            self.sub_task(worker_t.tab[worker].func_id,
                          worker_t.tab[worker].params_id)
            logging.info("Resub failed task.")
        logging.info("No task failed.")
        return

    def add_worker(self, worker: str) -> None:
        """Triggered by fd.

        Select idle worker.

        Args:
            worker (str): new worker.

        Returns:
            None
        """
        logging.info("Start to get Worker Table.")
        worker_t = GCS.get(WORKER_TABLE_NAME)
        logging.info("Worker Table received.")
        if not worker_t:
            worker_t = WorkerTable()
        worker_t.add_worker(worker)
        logging.info("Start to write new Worker Table.")
        worker_t = GCS.put(worker_t, WORKER_TABLE_NAME)
        logging.info("New Worker Table updated to GCS.")

    def select_worker(self, worker_t: WorkerTable) -> str:
        """Worker select strategy.

        Select idle worker.

        Args:
            None

        Returns:
            str: Selected worker host.
        """
        for w, info in worker_t.tab.items():
            if not info or GCS.query(info.t_id):
                return w
        return None

    def sub_task(self, func_id: str, params_id: list) -> str:
        """Recv a task from driver.

        Receive a task from driver and use rpc to call worker execute it.

        Args:
            func_id (str): The id of func object.
            params_id (list): A list of params object id.

        Returns:
            str: The result id of task.
        """
        logging.info("Start to submit task.")
        # fetch worker table
        logging.info("Start to pull worker table.")
        worker_t = GCS.get(WORKER_TABLE_NAME)
        logging.info("Worker Table pulled.")
        worker = self.select_worker(worker_t)
        if not worker:
            logging.warning("No worker available.")
            return "NONE"
        logging.info("Choose worker {}".format(worker))
        c = zerorpc.Client("tcp://{}:{}".format(worker, WORKER_PORT))
        logging.info("Assigned task to {}.".format(worker))
        res_id = c.recv_task(func_id, params_id)
        worker_t.set_worker_task(worker, res_id, func_id, params_id)
        logging.info("Start to update Worker Table.")
        GCS.put(worker_t, WORKER_TABLE_NAME)
        logging.info("Worker Table updated.")
        logging.info("Task submitted.")
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
