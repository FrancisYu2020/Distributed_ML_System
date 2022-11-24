import socket
import zerorpc
import uuid
import pickle
import logging
from glob_var import *


class GlobalControlState:
    """Global Control State (GCS) is used to try to make every component as stateless as possible.

    Contact with Simple Distributed File System (SDFS) to store these information.

    Attributes:
        None
    """

    def put_obj(obj: object, id: str = None) -> str:
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

    def get_obj(objId: str) -> object:
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
            logging.error("Cannot find target object.")
            return None
        replicas = replicas.split(" ")

        for replica in replicas:
            try:
                c = zerorpc.Client(timeout=None)
                c.connect("tcp://" + replica + ":" + DATA_NODE_PORT)
                bytes_obj, _ = c.get_file(objId)
                c.close()
                obj = pickle.loads(bytes_obj)
                logging.info("Get object {} succeeded.".format(objId))
                return obj
            except:
                continue
        logging.error("Get object {} failed.".format(objId))
        return None
