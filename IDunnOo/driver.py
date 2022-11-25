import zerorpc
import time
from global_control_store import GlobalControlState as GCS
import threading
from glob_var import *
from DNNs import *


class Driver:
    """Driver (client) is used to submit tasks to and get results, it can be regarded as Client.

    Attributes:
        lock (threading.Lock): The lock used for mutli threading.
    """

    def __init__(self) -> None:
        """The init func of Driver.

        Args:
            None

        Returns:
            None
        """
        self.mutex = threading.Lock()

    def shell(self) -> None:
        """The console shell, accept input and handle it.

        Args:
            None

        Returns:
            None
        """
        start_display = '''###### IDunno ######'''
        hint = ""
        print(start_display)
        print(hint)
        while True:
            cmd = input(">")
            print("Receive command: {}".format(cmd))
            self.execute()
            # TODO: Use the result to do other things.

    def execute(self) -> None:
        res_id = self.sub_task(MockDNN.hello_world, [
                                   "Guts", "Casgte", "Augustin", "Adale", "Buttin", "David", "Ella", "Fuze", "Harry", "Ive", "Jay", "Katlin", "Loki",
                                   "Mamba", "North", "Olive", "Porter", "Quora", "Rez", "Stella", "Telle", "Uber", "Viva", "Wa", "X", "Yelp", "Zelle"])
        if not res_id:
            print("Submit task failed, please resubmit.")
        if res_id == "NONE":
            print("No worker available, please try again.")
            return
        elif res_id == "ERROR":
            print("Submit task failed, please resubmit.")
            return
        t_res = threading.Thread(target=self.get_task_res, args=(res_id,"test_write_file.txt",))
        t_res.start()
        return

    def sub_task(self, func: object, data: list) -> str:
        """Sub a task to GS.

        Args:
            func (object): Task func object.
            data (list): A list of data object.

        Returns:
            str: The id of result object.
        """
        # get func id
        func_id = GCS.put(func)
        # get param_ids
        param_ids = []
        for d in data:
            param_ids.append(GCS.put(d))

        # heartbeat to check if main GS survive
        host = GLOBAL_SCHEDULER_HOST
        try:    # main GS survive
            heartbeat_c = zerorpc.Client(
                "tcp://{}:{}".format(GLOBAL_SCHEDULER_HOST, GLOBAL_SCHEDULER_PORT), timeout=2)
            heartbeat_c.heartbeat()
            heartbeat_c.close()
        except Exception as e:  # use hot standby GS
            host = HOT_STANDBY_GLOBAL_SCHEDULER_HOST

        try:
            c = zerorpc.Client(
                "tcp://{}:{}".format(host, GLOBAL_SCHEDULER_PORT), timeout=30)
            res_id = c.sub_task(func_id, param_ids)
            print("Get res_id: {}".format(res_id))
        except Exception as e:
            print("Submit task failed, please resubmit. Error: {}".format(e))
            return
        return res_id

    def get_task_res(self, res_id: str, target_file: str) -> object:
        """Get a task to GS.

        Args:
            res_id (id): The id of result object.
            target_file (str): The name of target file.

        Returns:
            object: The result object.
        """
        res = None
        while not res:
            res = GCS.get(res_id)
            time.sleep(2)
        with self.mutex:
            with open(target_file, "ab") as f:
                f.write(res)
        print("Finish a task.")
        return


if __name__ == "__main__":
    Driver().shell()
