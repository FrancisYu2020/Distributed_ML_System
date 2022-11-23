from worker import Worker
import zerorpc
from multiprocessing import Process
import logging
import sys


LOCAL_SCHEDULER_PORT = 6666
DEFAULT_SLAVE_NUM = 2


logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s.%(msecs)03d %(levelname)s {%(module)s} [%(funcName)s] %(message)s',
                    datefmt='%Y-%m-%d,%H:%M:%S',
                    filename='IDunno_logs.log',
                    filemode='w')


class LocalScheduler:
    def __init__(self, worker_num) -> None:
        """Initial a Worker object.

        Args:
            woreker_num (int): The number of worker processes run in local.

        Returns:
            None

        """
        self.worker_ports = [5550 + i for i in range(worker_num)]
        self.slaves = [Worker(self.worker_ports[i]) for i in range(worker_num)]
        self.rpc_server = zerorpc.Server(self)
        self.rpc_server.bind("tcp://0.0.0.0:{}".format(LOCAL_SCHEDULER_PORT))

    def assign_task(self) -> None:
        pass

    def spill_task(self) -> None:
        pass

    def run_worker(self) -> None:
        pass

    def run(self) -> None:
        """Run the local scheduler process.

        Args:
            None

        Returns:
            None

        """
        logging.info("Local scheduler starting...")
        pool = [Process(target=slave.run) for slave in self.slaves]
        pool.append(Process(target=self.rpc_server.run))

        logging.info(
            "{} worker processes starting...".format(len(self.slaves)))
        for p in pool:
            p.start()
        logging.info("Local scheduler rpc service started.")

        for p in pool:
            p.join()
        logging.info("Close local scheduler.")


if __name__ == '__main__':
    ls = LocalScheduler(DEFAULT_SLAVE_NUM) if len(
        sys.argv) < 2 else LocalScheduler(int(sys.argv[1]))
    ls.run()
