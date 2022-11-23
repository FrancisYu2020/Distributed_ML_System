import socket
import zerorpc
import uuid
import pickle
import logging


NAME_NODE_HOST = "fa22-cs425-2210.cs.illinois.edu"
NAME_NODE_PORT = 6241
DATA_NODE_PORT = 6242


logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s.%(msecs)03d %(levelname)s {%(module)s} [%(funcName)s] %(message)s',
                    datefmt='%Y-%m-%d,%H:%M:%S',
                    filename='GCS.log',
                    filemode='w')


class ObjectTable:
    """ObjectTable is used to store the info of objects.

    Attributes:
        None
    """

    def __init__(self) -> None:
        pass


class TaskTable:
    """TaskTable is used to store the info of tasks.

    Attributes:
        None
    """

    def __init__(self) -> None:
        pass


class GlobalControlState:
    """GCS is used to try to make every component stateless.

    Contact with Simple Distributed File System (SDFS) to store these information.

    Attributes:
        object_table: Record where is the Object.
        task_table: Record where is the task.
    """

    def __init__(self) -> None:
        """Initial a Global Control State object.

        Args:
            None

        Returns:
            None
        """
        self.obj_table = ObjectTable()
        self.task_table = TaskTable()

    def put_obj(self, obj) -> str:
        """Initial a Worker object.

        Args:
            obj (object): The object which need to store.

        Returns:
            str: The id of put Object.
        """
        # generate unique id for object.
        objId = uuid.uuid1().hex

        # get addresses of replicas
        s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        dst_addr = (NAME_NODE_HOST, NAME_NODE_PORT)
        data = "put {}".format(objId)
        s.sendto(data.encode("utf-8"), dst_addr)
        replicas, _ = s.recvfrom(4096)
        replicas = replicas.decode("utf-8").split(" ")

        bytes_obj = pickle.dumps(obj)
        # call rpc to send object
        c = zerorpc.Client(timeout=None)
        c.connect("tcp://{}:{}".format(replicas[0], DATA_NODE_PORT))
        c.put_file(objId, bytes_obj, replicas[1:])
        c.close()

        # recv result
        s.settimeout(30)
        try:
            finish, _ = s.recvfrom(4096)
        except Exception as e:
            logging.error("Put object timedout.")
        s.close()

        if finish.decode("utf-8") == "finish":
            logging.info(
                "Put object success, generated id is: {}.".format(objId))
        else:
            logging.error("Put object failed.")
        return objId

    def get_obj(self, objId) -> object:
        """Initial a Worker object.

        Args:
            objId (objId): The id of target object.

        Returns:
            object: Target object.
        """
        # get address
        s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        dst_addr = (self.get_namenode_host(), NAME_NODE_PORT)
        data = "get {}".format(objId)
        s.sendto(data.encode("utf-8"), dst_addr)
        replicas, _ = s.recvfrom(4096)
        s.close()

        replicas = replicas.decode("utf-8")
        if not replicas:
            logging.error("Cannot find target object.")
            return None
        replicas = replicas.split(" ")

        for replica in replicas:
            try:
                c = zerorpc.Client(timeout=None)
                c.connect("tcp://" + replica + ":" + DATA_NODE_PORT)
                bytes_obj, v = c.get_file(objId)
                c.close()
                obj = pickle.loads(bytes_obj)
                logging.info("Get object {} succeeded.".format(objId))
                return obj
            except:
                continue
        logging.error("Get object {} failed.".format(objId))
        return None
