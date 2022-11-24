class MockDNN():
    def __init__(self) -> None:
        pass

    def hello_world(names) -> str:
        ret = "Welcome to IDunno, "
        for name in names:
            ret += name
        ret += "."
        return ret