# reimplementation of MP2, in this implementation, we assume master will always join first and will never leave the ring

import socket
import json
import threading
import zerorpc
from glob_var import *

ML_lock = threading.Lock()


class Server:
    def __init__(self, master_host="fa22-cs425-2210.cs.illinois.edu"):
        self.hostname = socket.gethostname()
        self.hostID = int(self.hostname[13:15])
        print("I am VM ", self.hostID)
        self.master_host = master_host
        # self.is_master = True if self.hostname == master_host else False
        self.is_master = True
        if self.is_master:
            print("I am the master node -^.^- ")
        else:
            print("I am a pariah node :(")
        self.ML = []
        # self.neighbor_timestamps = {} # dict of key = hostname, value = [isInRing, timestamp], only used in master node to record a node status
    
    def update(self):
        for host in self.ML:
            # broadcast the updated ML to every node marked in the ring
            s1 = socket.socket()
            s1.connect((host, FOLLOWER_PORT))
            s1.send(json.dumps(self.ML).encode())
            s1.close()
            print("successfully send ML: ", self.ML)

    def fail(self, host):
        # node leave, do nothing but send a leave request ["leave", host] to master
        with ML_lock:
            if host not in self.ML:
                return
            self.ML.remove(host)
            
            # heartbeat to check if main GS survive
            host = GLOBAL_SCHEDULER_HOST
            try:    # main GS survive
                with zerorpc.Client("tcp://{}:{}".format(GLOBAL_SCHEDULER_HOST, GLOBAL_SCHEDULER_PORT), timeout=2) as heartbeat_c:
                    heartbeat_c.heartbeat()
            except:  # use hot standby GS
                host = HOT_STANDBY_GLOBAL_SCHEDULER_HOST

            c = zerorpc.Client(
                "tcp://{}:{}".format(host, GLOBAL_SCHEDULER_PORT))
            c.fail_worker(host)

            self.update()

    def listen_join_and_leave(self):
        # TODO: run a thread to know who are joining and leaving
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        s.bind((self.hostname, MASTER_PORT))
        s.listen(5)
        while 1:
            conn, addr = s.accept()
            # leave or join message heard from common nodes ["join"/"leave", node hostname]
            news = json.loads(conn.recv(4096).decode())

            if news[0] == "join":
                with ML_lock:
                    if news[1] in self.ML:
                        print("Node already exists in the ring!")
                        continue
                    # only a node join, mark its existance and timestamp and add to self.ML
                    print(news[1])
                    new_t = threading.Thread(
                        target=self.receive_ack, args=[news[1]])
                    new_t.start()
                    # with TS_lock:
                    # self.neighbor_timestamps[news[1]][0] = 1
                    # self.neighbor_timestamps[news[1]][1] = self.timer.time()
                    self.ML.append(news[1])
                    print("master send join messages: ", self.ML)
                    self.update()
            else:
                raise NotImplementedError(
                    "TODO: fix bug in listen_join_and_leave, the received news has unrecognizable message header")
            # TODO: ask everyone to update their membership list

    def receive_ack(self, monitor_host):
        if not self.is_master:
            return
        monitorID = int(monitor_host[13:15])
        s = socket.socket()
        s.connect((monitor_host, PING_PORT[monitorID]))
        while True:
            try:
                s.send("hi".encode("utf-8"))
            except Exception as e:
                # print("Error: " + str(e))
                print("Host " + str(monitorID) + " Fail")
                try:
                    s1 = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                    s1.connect((monitor_host, FOLLOWER_PORT))
                    s1.send("you are dead".encode())
                    s1.close()
                except:
                    pass
                self.fail(monitor_host)
                s.close()
                return

    def run(self):
        tm = threading.Thread(
            target=self.listen_join_and_leave, name="listen_join_and_leave")
        tm.start()
        tm.join()

s = Server()
s.run()