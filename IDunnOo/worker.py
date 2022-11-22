import zerorpc
import time
import random

WORKER_PORT = 5555
LOCAL_SCHEDULER_PORT = 6666

class Worker:
    def __init__(self) -> None:
        """Initial a Worker object.

        Args:
            None
        
        Returns:
            None

        """
        self.rpc_server = zerorpc.Server(self)
        self.rpc_server.bind("tcp://0.0.0.0:{}".format(WORKER_PORT))
        self.rpc_client = zerorpc.Client()

    
    def __sleep(self):
        # Sleep random time (0-1s) to avoid conflict.
        sleep_t = random.random()
        time.sleep(sleep_t)


    def train_model(self, model_name, train_data) -> bool:
        """Train a model and save it to SDFS.

        Args:
            model_name (string): The name of the model which need to be trained.
            train_data (list): A list of train data.
        
        Returns:
            bool: The return value. True for success, False otherwise.
        
        """
        while True:
            try:
                # Try to use rpc to get the model.
                self.rpc_client.connect("tcp://127.0.0.1:{}".format(LOCAL_SCHEDULER_PORT))
                model = self.rpc_client.load_model(model_name)
                break
            except:
                self.__sleep()
            
        while True:
            try:
                # Try to use rpc to get the data.
                self.rpc_client.connect("tcp://127.0.0.1:{}".format(LOCAL_SCHEDULER_PORT))
                dataset = self.rpc_client.load_data(train_data)
                break
            except:
                self.__sleep()

        # Utilize model to train dataset.
        res = model.train(dataset)
        while True:
            try:
                # Try to use rpc save the result.
                self.rpc_client.connect("tcp://127.0.0.1:{}".format(LOCAL_SCHEDULER_PORT))
                self.rpc_client.save(res)
                break
            except:
                self.__sleep()
        
        return True

    
    def predict(self, model_name, data) -> bool:
        """Use a model to predict.

        Args:
            model_name (string): The name of the model which is used to predict result.
            data (list): A list of data name.
        
        Returns:
            bool: The return value. True for success, False otherwise.
        
        """
        while True:
            try:
                # Try to use rpc to get the model.
                self.rpc_client.connect("tcp://127.0.0.1:{}".format(LOCAL_SCHEDULER_PORT))
                model = self.rpc_client.load_model(model_name)
                break
            except:
                self.__sleep()
            
        while True:
            try:
                # Try to use rpc to get the data.
                self.rpc_client.connect("tcp://127.0.0.1:{}".format(LOCAL_SCHEDULER_PORT))
                dataset = self.rpc_client.load_data(data)
                break
            except:
                self.__sleep()

        # Utilize model to predict the result of dataset.
        res = model.predict(dataset)
        while True:
            try:
                # Try to use rpc save the result.
                self.rpc_client.connect("tcp://127.0.0.1:{}".format(LOCAL_SCHEDULER_PORT))
                self.rpc_client.save(res)
                break
            except:
                self.__sleep()
        
        return True


    def run(self) -> None:
        """Run the worker process.

        Args:
            None
            
        Returns:
            None

        """
        self.rpc_server.run()
    