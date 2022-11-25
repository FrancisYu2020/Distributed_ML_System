import zerorpc
import time
from global_control_store import GlobalControlState as GCS
from glob_var import *
from DNNs import *


class Driver:
    """Driver (client) is used to submit tasks to and get results, it can be regarded as Client.

    Attributes:
        None
    """

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
            res_id = self.sub_task(MockDNN.hello_world, ["Guts", "Casgte", "Augustin"])
            if res_id == "NONE":
                print("No worker available, please try again.")
                continue
            # TODO: Use the result to do other things.
            res = self.get_task_res(res_id)
            print(res)

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
            heartbeat_c = zerorpc.Client("tcp://{}:{}".format(GLOBAL_SCHEDULER_HOST, GLOBAL_SCHEDULER_PORT), timeout=2)
            heartbeat_c.heartbeat()
            heartbeat_c.close()
        except Exception as e:  # use hot standby GS
            host = HOT_STANDBY_GLOBAL_SCHEDULER_HOST

        c = zerorpc.Client(
            "tcp://{}:{}".format(host, GLOBAL_SCHEDULER_PORT))
        res_id = c.sub_task(func_id, param_ids)
        print("Get res_id: {}".format(res_id))
        return res_id

    def get_task_res(self, res_id: str) -> object:
        """Get a task to GS.

        Args:
            func (object): Task func object.
            data (list): A list of data objects.

        Returns:
            object: The result object.
        """
        res = None
        while not res:
            print("Still working, please wait...")
            res = GCS.get(res_id)
            time.sleep(2)
        return res


if __name__ == "__main__":
    Driver().shell()
