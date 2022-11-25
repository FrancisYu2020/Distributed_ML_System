import time

class MockDNN():
    def __init__(self) -> None:
        pass

    def hello_world(names) -> str:
        ret = "Welcome to IDunno, "
        for name in names:
            time.sleep(2)
            ret += name + " "
        ret += "."
        return ret