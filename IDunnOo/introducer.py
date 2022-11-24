# reimplementation of MP2, in this implementation, we assume master will always join first and will never leave the ring

import socket
import json
import time
import threading
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

    def get_neighbors(self):
        # If a node is not in the ring, self.ML = [] hence directly return []
        # if a node is in the ring, find the index of it and return its 3 or less successors
        if not len(self.ML):
            return []
        i = 0
        while i < len(self.ML) and (self.ML[i] != self.hostname):
            i += 1
        if i == len(self.ML) and (i != 0):
            raise NotImplementedError(
                "TODO: fix bug that current server is in the ring but not in the membership list!")
        if len(self.ML) <= 4:
            ret = self.ML[:i] + self.ML[i+1:]
        else:
            ret = (self.ML * 2)[i+1:i+4]
        return ret

    def join(self):
        # a common node join, do nothing but send a join request ["join", current node hostname] to master
        t = threading.Thread(target=self.ping, name="heartbeat")
        t.start()
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        s.connect((self.master_host, MASTER_PORT))
        s.send(json.dumps(["join", self.hostname]).encode())
        s.close()

    def leave(self, host=None):
        if host == None:
            # self.leave() leave the node itself from the ring
            host = self.hostname
        # TODO: send a leave message to the master
        if self.is_master:
            # we do not allow master to leave
            print("Cannot force master node to leave!")
            return
        # else send leave message to the master
        if host == self.hostname:
            # self leave, remember to clear self.ML
            self.ML = []
        # node leave, do nothing but send a leave request ["leave", host] to master
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        s.connect((self.master_host, MASTER_PORT))
        s.send(json.dumps(["leave", host]).encode())
        s.close()

    def listen_join_and_leave(self):
        # TODO: run a thread to know who are joining and leaving
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        s.bind((self.hostname, MASTER_PORT))
        s.listen(5)
        while 1:
            conn, addr = s.accept()
            # leave or join message heard from common nodes ["join"/"leave", node hostname]
            news = json.loads(conn.recv(4096).decode())

            if news[0] == "leave":
                idx = -1
                for i in range(len(self.ML)):
                    if self.ML[i] == news[1]:
                        idx = i
                        break
                if idx == -1:
                    print(f"node {news[1]} already left")
                    # This should indicate that multiple nodes detected the leave/fail and reported to the master, we do nothing
                    # raise NotImplementedError("TODO: fix bug in listen_join_and_leave that inactive host is not in the membership list")
                    continue
                # directly remove the node from self.ML
                self.ML = self.ML[:idx] + self.ML[idx+1:]
                # with TS_lock:
                #     self.neighbor_timestamps[news[1]][0] = 0
            elif news[0] == "join":
                if news[1] in self.ML:
                    print("Node already exists in the ring!")
                    continue
                # only a node join, mark its existance and timestamp and add to self.ML
                # print(news[1])
                new_t = threading.Thread(
                    target=self.receive_ack, args=[news[1]])
                new_t.start()
                # with TS_lock:
                # self.neighbor_timestamps[news[1]][0] = 1
                # self.neighbor_timestamps[news[1]][1] = self.timer.time()
                self.ML.append(news[1])
                # print("master send join messages: ", self.ML)
            else:
                raise NotImplementedError(
                    "TODO: fix bug in listen_join_and_leave, the received news has unrecognizable message header")
            # TODO: ask everyone to update their membership list
            for host in self.ML:
                if self.is_master:
                    continue
                # broadcast the updated ML to every node marked in the ring
                s1 = socket.socket()
                s1.connect((host, FOLLOWER_PORT))
                s1.send(json.dumps(self.ML).encode())
                s1.close()
                # print("successfully send ML: ", self.ML)

    def ping(self):
        if self.is_master:
            return
        # send ping to check neighbors alive every 300 ms

        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        s.bind((self.hostname, PING_PORT[self.hostID]))
        s.listen(5)
        conn, _ = s.accept()
        while True:
            conn.recv(100)

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
                    s1.connect((monitor_host, MASTER_PORT))
                    s1.send("you are dead".encode())
                    s1.close()
                except:
                    pass
                self.leave(monitor_host)
                s.close()
                return

    def run(self):
        tm = threading.Thread(
            target=self.listen_join_and_leave, name="listen_join_and_leave")
        tm.start()
        tm.join()

s = Server()
s.run()