

class Driver:
    def __init__(self) -> None:
        pass


    def shell(self) -> None:
        start_display = '''###### IDunno ######'''
        hint = '''    Commands    \n- Train Model\n- Load Model\n- Predict\n- help\nPlease choose option:'''
        print(start_display)
        print(hint)
        while True:
            cmd = input(">")


d = Driver()
d.shell()