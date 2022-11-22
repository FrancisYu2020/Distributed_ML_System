from worker import Worker
import zerorpc
from multiprocessing import Pool
import logging


LOCAL_SCHEDULER_PORT = 6666
MAX_SLAVE_NUM = 4
SLAVE_NUM = 2


logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s.%(msecs)03d %(levelname)s {%(module)s} [%(funcName)s] %(message)s',
                    datefmt='%Y-%m-%d,%H:%M:%S',
                    filename='local_scheduler.log',
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
        self.rpc_client = zerorpc.Client()


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
        pool = Pool(processes = MAX_SLAVE_NUM + 1)
        for slave in self.slaves:
            pool.apply_async(slave.run, ())
        pool.apply_async(self.rpc_server.run, ())

        logging.info("{} worker processes started.".format(len(self.slaves)))
        logging.info("Local scheduler rpc service started.")
        # self.rpc_server.run()
        
        pool.close()
        pool.join()
        logging.info("Close local scheduler.")


if __name__ == '__main__':
    ls = LocalScheduler(SLAVE_NUM)
    ls.run()