import zerorpc
from global_control_store import GlobalControlState as GHC
from glob_var import *
import logging


logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s.%(msecs)03d %(levelname)s {%(module)s} [%(funcName)s] %(message)s',
                    datefmt='%Y-%m-%d,%H:%M:%S',
                    filename='IDunno_driver_logs.log',
                    filemode='w')


class Driver:
    """Driver (client) is used to submit tasks to and get results, it can be regarded as Client.

    Attributes:
        None
    """

    def shell() -> None:
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

    def sub_task(func, data) -> str:
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

    def get_task_res(res_id) -> object:
        """Get a task to GS.

        Args:
            func (object): Task func object.
            data (list): A list of data objects.

        Returns:
            object: The result object.
        """
        res_obj = GHC.get_obj(res_id)
        return res_obj


if __name__ == "__main__":
    Driver.shell()
