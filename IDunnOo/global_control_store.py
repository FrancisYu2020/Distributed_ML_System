import socket
import zerorpc
import uuid
import pickle
import logging
from glob_var import *


class TaskInfo:
    def __init__(self, t_id: str, func_id: str, params_id: list) -> None:
        self.t_id = t_id
        self.func_id = func_id
        self.params_id = params_id


class WorkerTable:
    def __init__(self) -> None:
        self.tab = {}   # worker -> TaskInfo

    def add_worker(self, worker) -> None:
        self.tab[worker] = None

    def del_worker(self, worker) -> None:
        del self.tab[worker]

    def set_worker_task(self, worker: str, t_id: str, func_id: str, params_id: str) -> None:
        t_info = TaskInfo(t_id, func_id, params_id)
        self.tab[worker] = t_info


class GlobalControlState:
    """Global Control State (GCS) is used to try to make every component as stateless as possible.

    Contact with Simple Distributed File System (SDFS) to store these information.

    Attributes:
        None
    """

    def put(obj: object, id: str = None) -> str:
        """Put an object to SDFS.

        Args:
            obj (object): The object which need to store.
            id (str): The specified id of object, default will generate a unique object id.

        Returns:
            str: The id of put Object.
        """
        # generate unique id for object if no specified id.
        objId = uuid.uuid1().hex if not id else id

        # get addresses of replicas
        s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        dst_addr = (NAME_NODE_HOST, NAME_NODE_PORT)
        data = "put {}".format(objId)
        s.sendto(data.encode("utf-8"), dst_addr)
        replicas, _ = s.recvfrom(4096)
        replicas = replicas.decode("utf-8").split(" ")

        bytes_obj = pickle.dumps(obj)
        # call rpc to send object
        try:
            c = zerorpc.Client(timeout=None)
            c.connect("tcp://{}:{}".format(replicas[0], DATA_NODE_PORT))
            c.put_file(objId, bytes_obj, replicas[1:])
            c.close()
            logging.info(
                "Put object success, the id of object is: {}.".format(objId))
        # recv result
        # s.settimeout(30)
        # try:
        #     finish, _ = s.recvfrom(4096)
        except Exception as e:
            logging.error("Put object failed.")
        s.close()
        return objId

    def get(objId: str) -> object:
        """"Get an object from SDFS.

        Args:
            objId (str): The id of target object.

        Returns:
            object: Target object.
        """
        # get address
        s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        dst_addr = (NAME_NODE_HOST, NAME_NODE_PORT)
        data = "get {}".format(objId)
        s.sendto(data.encode("utf-8"), dst_addr)
        replicas, _ = s.recvfrom(4096)
        s.close()

        replicas = replicas.decode("utf-8")
        if not replicas:
            logging.warning("Cannot find target object {}.".format(objId))
            return None
        replicas = replicas.split(" ")

        logging.info("Target object in {}.".format(replicas))
        for replica in replicas:
            try:
                c = zerorpc.Client(timeout=None)
                c.connect("tcp://{}:{}".format(replica, DATA_NODE_PORT))
                bytes_obj, _ = c.get_file(objId)
                c.close()
                obj = pickle.loads(bytes_obj)
                logging.info("Get object {} succeeded.".format(objId))
                return obj
            except Exception as e:
                continue
        logging.error("Get object {} failed.".format(objId))
        return None

    def query(objId: str) -> bool:
        """"Query if an object in SDFS.

        Args:
            objId (str): The id of target object.

        Returns:
            bool: True for exists False otherwise.
        """
        s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        dst_addr = (NAME_NODE_HOST, NAME_NODE_PORT)
        data = "query {}".format(objId)
        s.sendto(data.encode("utf-8"), dst_addr)
        exists, _ = s.recvfrom(4096)
        return True if exists.decode("utf-8") == "y" else False
