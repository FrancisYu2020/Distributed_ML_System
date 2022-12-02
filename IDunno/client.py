import time
from glob_var import *
import zerorpc
from sdfs_shell import SDFShell
import collections
from DNN import *
import pickle
import time
import threading

class JobInfo():
    def __init__(self, job_id = None, files = None):
        self.job_id = job_id
        self.files = files

class Client():
    def __init__(self):
        self.rpc_c = zerorpc.Client(
                f'tcp://{COORDINATOR_HOST}:{COORDINATOR_PORT}')
        self.job_q = collections.deque()
        self.jobs = {}
    
    def import_data(self, filelist, start, end):
        # 0-index
        img_list = [read_image(filelist[img_idx]) for img_idx in range(start, end)]
        return pickle.dumps(img_list)

    def dashboard(self):
        try:
            c = zerorpc.Client(f'tcp://{COORDINATOR_HOST}:{COORDINATOR_PORT}')
            dash, results = c.get_dash()
        except:
            c = zerorpc.Client(f'tcp://{HOT_STANDBY_COORDINATOR_HOST}:{COORDINATOR_PORT}')
            dash, results = c.get_dash()
        for job in dash:
            num = dash[job]
            print(f'Job {job} finished {num} queries.')

    def shell(self):
        hint = '''Welcome to IDunno, please choose command:
        1. sub <job name> <local data path>
        2. get-stats
        3. help'''
        while True:
            print(hint)
            cmd = input(">")
            args = cmd.split(" ")
            if args[0] == "sub" and len(args) == 3:
                job_name, data_path = args[1:]

                filelist = os.listdir('imageNet/val')
                filelist.sort()
                filelist = ['imageNet/val/' + f for f in filelist]

                job_id = SDFShell.put(job_name)
                self.jobs[job_name] = JobInfo(job_id, filelist)
                
                self.job_q.append(job_name)
            elif args[0] == "get-stats" and len(args) == 1:
                self.dashboard()
            else:
                print("Invalid command! If you need any help, please input 'help'.")

    def sub_task(self):
        while True:
            if len(self.job_q) == 0:
                time.sleep(0.5)
            else:
                job_name = self.job_q.popleft()
                self.job_q.append(job_name)
                task_id = f'{job_name} {time.time()}'
                filelist = self.jobs[job_name].files
                model_id = self.jobs[job_name].job_id
                try:
                    while True:
                        if self.rpc_c.submit_task(task_id, model_id, self.import_data(filelist, 0, 10)):
                            # print(f'Submit task {task_id} for {job_name} successful.')
                            break
                        else:
                            time.sleep(0.2)
                except Exception as e:
                    print(e)
                    self.rpc_c = zerorpc.Client(
                    f'tcp://{HOT_STANDBY_COORDINATOR_HOST}:{COORDINATOR_PORT}')
                    while True:
                        if self.rpc_c.submit_task(task_id, model_id, self.import_data(filelist, 0, 10)):
                            print(f'Submit task {task_id} for {job_name} successful.')
                            break
                        else:
                            time.sleep(0.2)
    
    def run(self):
        t_shell = threading.Thread(target = self.shell)
        t_shell.start()
        self.sub_task()

        t_shell.join()
        t_sub_task.join()

    # def sub_job(self):
    #     filelist = os.listdir('imageNet/val')
    #     filelist.sort()
    #     filelist = ['imageNet/val/' + f for f in filelist]
    #     model_id = SDFShell.put('resnet18')
    #     another_model_id = SDFShell.put('alexnet')
    #     round_robin_queue = collections.deque()
    #     round_robin_queue.append(model_id)
    #     round_robin_queue.append(another_model_id)

    #     i = 1
    #     while i:
    #         #TODO: add interactive shell
    #         job_name = "resnet18" if round_robin_queue[0] == model_id else "alexnet"
    #         task_id = f"query {i} for job {job_name}"
    #         i += 1
    #         model = round_robin_queue.popleft()
    #         round_robin_queue.append(model)
    #         try:
    #             while True:
    #                 #TODO: write import_data() and finalize input img_idx
    #                 if self.rpc_c.submit_task(task_id, model, self.import_data(filelist, 0, 10)):
    #                     print(f'Submit task {task_id} for {model} successful.')
    #                     break
    #                 else:
    #                     time.sleep(0.2)
    #         except:
    #             self.rpc_c = zerorpc.Client(
    #                 f'tcp://{HOT_STANDBY_COORDINATOR_HOST}:{COORDINATOR_PORT}')
    #             while True:
    #                 if self.rpc_c.submit_task(task_id, model, self.import_data(filelist, 0, 10)):
    #                     print(f'Submit task {task_id} for {model} successful.')
    #                     break
    #                 else:
    #                     time.sleep(0.2)

if __name__ == "__main__":
    c = Client()
    c.run()