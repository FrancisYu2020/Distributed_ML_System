from control_state import ObjectTable, TaskTable, Eventlogs, FunctionTable

class GlobalScheduler:
    def __init__(self) -> None:
        self.object_table = ObjectTable()
        self.task_table = TaskTable()
        self.event_logs = Eventlogs()
        self.function_table = FunctionTable()

    


    

