import time
from glob_var import *
import zerorpc
from sdfs_shell import SDFShell
import collections
from DNN import *
import pickle
import time
import threading
import numpy as np

class JobInfo():
    def __init__(self, name=None, job_id=None, files=None, batch_size=None):
        self.name = name
        self.job_id = job_id
        self.files = files
        self.batch_size = batch_size


class Client():
    def __init__(self):
        self.rpc_c = zerorpc.Client(
            f'tcp://{COORDINATOR_HOST}:{COORDINATOR_PORT}')
        self.coordinator_host = COORDINATOR_HOST
        self.job_q = collections.deque()
        self.jobs = {}
        self.SHOULD_EXIT = 0


    def import_data(self, filelist, start, end):
        # 0-index
        img_list = [read_image(filelist[img_idx])
                    for img_idx in range(start, end)]
        return pickle.dumps(img_list)

    def dashboard(self):
        try:
            c = zerorpc.Client(
                f'tcp://{self.coordinator_host}:{COORDINATOR_PORT}')
            bytes_dash, bytes_times = c.get_dash()
        except:
            self.coordinator_host = HOT_STANDBY_COORDINATOR_HOST
            c = zerorpc.Client(
                f'tcp://{self.coordinator_host}:{COORDINATOR_PORT}')
            bytes_dash, bytes_times = c.get_dash()
        dash = pickle.loads(bytes_dash)
        times = pickle.loads(bytes_times)
        # print(dash)
        # print(times.keys())
        for job_id in dash:
            num = dash[job_id]
            job_name = self.jobs[job_id].name
            job_times = times[job_id]
            job_times.sort()
            job_times = np.array(job_times)[:-1]
            avg = np.round(len(job_times)/job_times.sum(), 3)
            # print(len(job_times), job_times.sum())
            # print(job_times)
            # job_times = np.array(job_times)[10:]
            query_rates = 1 / job_times
            # avg = np.round(query_rates.mean(), 3)
            std = np.round(query_rates.std() / 200, 3)
            median = np.round(query_rates[len(query_rates)//2], 3)
            percentile1 = np.round(query_rates[int(0.1 * len(query_rates))], 3) # 90 percentile
            percentile2 = np.round(query_rates[int(0.05 * len(query_rates))], 3) # 95 percentile
            percentile3 = np.round(query_rates[int(0.01 * len(query_rates))], 3) # 99 percentile
            record = f'Job {job_name} finished {num} queries, query rate average = {avg}, std = {std}, median = {median}, 90 percentile = {percentile1}, 95 percentile = {percentile2}, 99 percentile = {percentile3}'
            print(record)
            with open('experiment.txt', 'a') as f:
                f.write(record + '\n')

    def job_rates(self):
        print("We are working counting results, please wait for a moment :) ")
        try:
            c = zerorpc.Client(
                f'tcp://{self.coordinator_host}:{COORDINATOR_PORT}')
            bytes_dash, _ = c.get_dash()
        except:
            self.coordinator_host = HOT_STANDBY_COORDINATOR_HOST
            c = zerorpc.Client(
                f'tcp://{self.coordinator_host}:{COORDINATOR_PORT}')
            bytes_dash, _ = c.get_dash()
        pre_dash = pickle.loads(bytes_dash)

        for i in range(1, 11):
            print("\r" + "■" * i * 6 + " ]", end="")
            time.sleep(1)
        print("")

        try:
            c = zerorpc.Client(
                f'tcp://{self.coordinator_host}:{COORDINATOR_PORT}')
            bytes_dash, _ = c.get_dash()
        except:
            self.coordinator_host = HOT_STANDBY_COORDINATOR_HOST
            c = zerorpc.Client(
                f'tcp://{self.coordinator_host}:{COORDINATOR_PORT}')
            bytes_dash, _ = c.get_dash()
        suf_dash = pickle.loads(bytes_dash)
        # print(pre_dash)

        for job_id in pre_dash:
            # print(job_id)
            pre_num = pre_dash[job_id]
            suf_num = suf_dash[job_id]
            job_name = self.jobs[job_id].name
            avg = suf_num - pre_num
            record = f'Job {job_name} speed: {avg} queries / 10s.'
            print(record)
            with open('experiment.txt', 'a') as f:
                f.write(record + '\n')

    def get_results(self):
        try:
            c = zerorpc.Client(
                f'tcp://{self.coordinator_host}:{COORDINATOR_PORT}')
            bytes_res = c.get_res()
        except:
            self.coordinator_host = HOT_STANDBY_COORDINATOR_HOST
            c = zerorpc.Client(
                f'tcp://{self.coordinator_host}:{COORDINATOR_PORT}')
            bytes_res = c.get_res()
        res = pickle.loads(bytes_res)
        for model_id, tasks in res.items():
            name = self.jobs[model_id].name
            sorted_res = sorted(tasks.items(), key=lambda x: x[0])
            write_content = [r[1] for r in sorted_res]
            with open(f'{name}_query_results', 'w') as f:
                f.write("\n".join(write_content))

    def get_vm_states(self):
        try:
            c = zerorpc.Client(
                f'tcp://{self.coordinator_host}:{COORDINATOR_PORT}')
            bytes_vms = c.get_worker_states()
        except:
            self.coordinator_host = HOT_STANDBY_COORDINATOR_HOST
            c = zerorpc.Client(
                f'tcp://{self.coordinator_host}:{COORDINATOR_PORT}')
            bytes_vms = c.get_worker_states()
        vms = pickle.loads(bytes_vms)
        vm_states = collections.defaultdict(list)
        for vm, job_id in vms.items():
            if job_id != "Idle":
                vm_states[self.jobs[job_id].name].append(vm)
            else:
                vm_states["Idle"].append(vm)
        # TODO: complete print vm states part
        for job, vms in vm_states.items():
            print("{}: {}".format(job, vms))
    
    def kill_worker(self, idx):
        pass
        print(f"Worker on VM{idx} has been killed successfully!")
    
    def kill_coordinator(self, idx):
        if idx == 2:
            print("Cannot kill hot standby!")
        elif idx == 1:
            pass
        else:
            print(f"VM{idx} is not a coordinator!")

    def experiment(self):
        ts = 0
        while 1:
            if self.SHOULD_EXIT:
                exit(0)
            if len(self.jobs) < 1:
                continue
            else:
                time.sleep(10)
                ts += 10
                print(f"# At timestamp = {ts}s:")
                self.dashboard()

    def shell(self):
        hint = '''Welcome to IDunno, please choose command:
        1. sub <job name> <local data path> <batch size>
        2. get-stats
        3. job-rates
        4. set-batch <job name> <batch size>
        5. get-results
        6. vm-states
        7. kill <worker/coordinator> <VM ID>
        8. help
        9. exit'''
        print(hint)
        while True:
            cmd = input("> ")
            args = cmd.split(" ")
            if args[0] == "sub" and len(args) == 4:
                job_name, data_path, batch_size = args[1:]

                filelist = os.listdir('imageNet/val')
                filelist.sort()
                filelist = ['imageNet/val/' + f for f in filelist]

                job_id = SDFShell.put(job_name)
                self.jobs[job_id] = JobInfo(
                    job_name, job_id, filelist, int(batch_size))

                self.job_q.append(job_id)
                print(f'Submit job {job_name} successful.')
            elif args[0] == "get-stats" and len(args) == 1:
                self.dashboard()
            elif args[0] == "job-rates" and len(args) == 1:
                self.job_rates()
            elif args[0] == "set-batch" and len(args) == 3:
                job_name, batch_size = args[1:]
                for id, info in self.jobs.items():
                    if info.name == job_name:
                        self.jobs[id].batch_size = int(batch_size)
                        print(
                            f'Set batch size of {job_name} job to {batch_size} successfully.')
            elif args[0] == "get-results" and len(args) == 1:
                self.get_results()
                print(f'All results have been stored in local.')
            elif args[0] == "vm-states" and len(args) == 1:
                self.get_vm_states()
            elif args[0] == "help" and len(args) == 1:
                print(hint)
            elif len(args) == 3 and args[0] == "kill":
                if args[1] == "worker":
                    self.kill_worker(int(args[2]))
                elif args[1] == "coordinator":
                    self.kill_coordinator(int(args[2]))
            elif args[0] == "exit":
                # exit(0)
                # break
                self.SHOULD_EXIT = 1
                exit(0)
            else:
                print("Invalid command! If you need any help, please input 'help'.")

    def sub_task(self):
        while True:
            if self.SHOULD_EXIT:
                exit(0)
            if len(self.job_q) == 0:
                time.sleep(0.5)
            else:
                job_id = self.job_q.popleft()
                self.job_q.append(job_id)
                task_id = f'{job_id} {time.time()}'
                filelist = self.jobs[job_id].files
                total = len(filelist)
                start_pos = random.randint(0, total // 2)
                try:
                    while True:
                        if self.rpc_c.submit_task(task_id, job_id, filelist[start_pos: start_pos + self.jobs[job_id].batch_size]):
                            # print(f'Submit task {task_id} for {job_name} successful.')
                            break
                        else:
                            time.sleep(0.2)
                except Exception as e:
                    # print(e)
                    self.coordinator_host = HOT_STANDBY_COORDINATOR_HOST
                    self.rpc_c = zerorpc.Client(
                        f'tcp://{HOT_STANDBY_COORDINATOR_HOST}:{COORDINATOR_PORT}')
                    while True:
                        if self.rpc_c.submit_task(task_id, job_id, filelist[start_pos: start_pos + self.jobs[job_id].batch_size]):
                            break
                        else:
                            time.sleep(0.2)

    def run(self):
        t_shell = threading.Thread(target=self.shell)
        # t_experiment = threading.Thread(target=self.experiment)
        t_shell.start()
        # t_experiment.start()
        self.sub_task()

        t_shell.join()
        # t_experiment.join()
        t_sub_task.join()


if __name__ == "__main__":
    c = Client()
    c.run()
