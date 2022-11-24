import zerorpc
import time
import pickle
from global_control_store import GlobalControlState as GHC
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
            res_id = self.sub_task(MockDNN.hello_world("Guts"))
            res_obj = self.get_task_res(res_id)
            res = pickle.loads(res_obj)
            print(res)

    def sub_task(func: object, data: list) -> str:
        """Sub a task to GS.

        Args:
            func (object): Task func object.
            data (list): A list of data object.

        Returns:
            str: The id of result object.
        """
        # get func id
        func_id = GHC.put_obj(func)
        # get param_ids
        param_ids = []
        for d in data:
            param_ids.append(GHC.put_obj(d))
        c = zerorpc.Client(
            "tcp://{}:{}".format(GLOBAL_SCHEDULER_HOST, GLOBAL_SCHEDULER_PORT))
        res_id = c.sub_task(func_id, param_ids)
        return res_id

    def get_task_res(res_id: str) -> object:
        """Get a task to GS.

        Args:
            func (object): Task func object.
            data (list): A list of data objects.

        Returns:
            object: The result object.
        """
        res_obj = None
        while not res_obj:
            res_obj = GHC.get_obj(res_id)
            time.sleep(0.5)
        return res_obj


if __name__ == "__main__":
    Driver().shell()
