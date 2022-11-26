import logging
import zerorpc
import time
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
        wt_cache: The cache of worker table.
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
        self.wt_cache = None
    
    def __write_wt_wrapper(self, new_wt: WorkerTable) -> None:
        """The wrapper of write to wt.
        
        Args: 
            new_wt (WorkerTable): New worker table.

        Returns: 
            None
        """
        self.wt_cache = new_wt
        GCS.put(self.wt_cache, WORKER_TABLE_NAME)

    def heartbeat(self) -> None:
        """Heartbeat func to detect survive, basically do nothing.

        Args:
            None

        Returns:
            None
        """
        return

    def handle_fail_worker(self, worker: str) -> None:
        """Worker failed, start new thread to check if failed worker has task and reassign the failed task.

        Args:
            worker (str): The failed worker host name.

        Returns:
            None
        """
        logging.warning("Failed worker trigger: {}".format(worker))
        logging.info("Start to get Worker Table.")
        worker_t = GCS.get(WORKER_TABLE_NAME) if not self.wt_cache else self.wt_cache
        logging.info("Worker Table received.")
        if not worker_t:
            logging.info("No task failed.")
            return

        if worker_t.tab[worker] and not GCS.query(worker_t.tab[worker].t_id):
            # delete worker from worker table
            func_id = worker_t.tab[worker].func_id
            params_id = worker_t.tab[worker].params_id
            t_id = worker_t.tab[worker].t_id
            worker_t.del_worker(worker)
            self.__write_wt_wrapper(worker_t)
            logging.info("New Worker Table updated.")
            logging.info("Start to resubmit failed task.")
            self.resub_task(func_id,
                            params_id,
                            t_id)
            logging.info("Resubmitted failed task.")
            return
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
        worker_t = GCS.get(WORKER_TABLE_NAME) if not self.wt_cache else self.wt_cache
        logging.info("Worker Table received.")
        if not worker_t:
            worker_t = WorkerTable()
        worker_t.add_worker(worker)
        logging.info("Start to write new Worker Table.")
        self.__write_wt_wrapper(worker_t)
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

    def resub_task(self, func_id: str, params_id: list, t_id: str = None) -> None:
        """Resub a failed task to another worker.

        Receive a task from driver and use rpc to call worker execute it.

        Args:
            func_id (str): The id of func object.
            params_id (list): A list of params object id.
            t_id (str): Task id, default will generate a unique id.

        Returns:
            None
        """
        while True:
            # try to resubmit task, if resubmit success, quit from loop and update worker table
            # if resubmit fail, which means the chosen worker also dead, then delete it from worker table and try this process again.
            worker = None
            while not worker:
                # select worker, if no worker available, sleep 0.5s and then check again.
                logging.info("Start to resubmit task.")
                # fetch worker table
                logging.info("Start to pull worker table.")
                worker_t = GCS.get(WORKER_TABLE_NAME) if not self.wt_cache else self.wt_cache
                logging.info("Worker Table pulled.")
                worker = self.select_worker(worker_t)
                if not worker:
                    logging.warning("No worker available, wait.")
                    time.sleep(0.5)
            logging.info("Choose worker {}".format(worker))
            try:
                c = zerorpc.Client(
                    "tcp://{}:{}".format(worker, WORKER_PORT), timeout=2)
                logging.info("Assigned task to {}.".format(worker))
                res_id = c.recv_task(func_id, params_id, t_id)
                break
            except Exception as e:
                logging.error(
                    "Submit task failed, error: {}. Worker {} failed, start to delete it.".format(e, worker))
                worker_t.del_worker(worker)
                self.__write_wt_wrapper(worker_t)
                logging.info("New Worker Table updated.")
        worker_t.set_worker_task(worker, res_id, func_id, params_id)
        logging.info("Start to update Worker Table.")
        self.__write_wt_wrapper(worker_t)
        logging.info("Worker Table updated.")
        logging.info("Task submitted.")
        return

    def sub_task(self, func_id: str, params_id: list, t_id: str = None) -> str:
        """Recv a task from driver.

        Receive a task from driver and use rpc to call worker execute it.

        Args:
            func_id (str): The id of func object.
            params_id (list): A list of params object id.
            t_id (str): Task id, default will generate a unique id.

        Returns:
            str: The result id of task. "NONE" means no available worker, "ERROR" means submit task failed.
        """
        logging.info("Start to submit task.")
        # fetch worker table
        logging.info("Start to pull worker table.")
        worker_t = GCS.get(WORKER_TABLE_NAME) if not self.wt_cache else self.wt_cache
        logging.info("Worker Table pulled.")
        worker = self.select_worker(worker_t)
        if not worker:
            logging.warning("No worker available.")
            return "NONE"
        logging.info("Choose worker {}".format(worker))
        try:
            c = zerorpc.Client(
                "tcp://{}:{}".format(worker, WORKER_PORT), timeout=2)
            logging.info("Assigned task to {}.".format(worker))
            res_id = c.recv_task(func_id, params_id, t_id)
        except Exception as e:
            logging.error("Submit task failed, error: {}".format(e))
            return "ERROR"
        worker_t.set_worker_task(worker, res_id, func_id, params_id)
        logging.info("Start to update Worker Table.")
        self.__write_wt_wrapper(worker_t)
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
