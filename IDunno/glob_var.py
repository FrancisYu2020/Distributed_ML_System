import time
import logging

COORDINATOR_HOST = "fa22-cs425-2201.cs.illinois.edu"
COORDINATOR_PORT = 8888
HOT_STANDBY_COORDINATOR_HOST = "fa22-cs425-2202.cs.illinois.edu"
LOCAL_SCHEDULER_PORT = 6666
NAME_NODE_HOST = "fa22-cs425-2210.cs.illinois.edu"
NAME_NODE_PORT = 6241
DATA_NODE_PORT = 6242
MASTER_PORT = 2333
FOLLOWER_PORT = 2334
PING_PORT = [2345 + i for i in range(11)]
BATCH_SIZE = 16

logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s.%(msecs)03d %(levelname)s {%(module)s} [%(funcName)s] %(message)s',
                    datefmt='%Y-%m-%d,%H:%M:%S',
                    filename='IDunno_GS_logs.log',
                    filemode='w')

class TestModel:
    def predict(self, params):
        time.sleep(3)
        return "Predict Test {} for TestModel".format(params)

class AnotherModel:
    def predict(self, params):
        time.sleep(1.5)
        return "Predict Test {} for AnotherModel".format(params)