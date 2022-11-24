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
        self.is_master = False
        if self.is_master:
            print("I am the master node -^.^- ")
        else:
            print("I am a pariah node :(")
        self.ML = []
        self.timer = time
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

    def listen_to_master(self):
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        s.bind((self.hostname, MASTER_PORT))
        s.listen(5)
        while 1:
            conn, addr = s.accept()
            info = conn.recv(4096).decode()
            if info == "you are dead":
                self.ML = []
            else:
                # simply update whatever heard from the master
                self.ML = json.loads(info)
    
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

    def shell(self):
        while 1:
            print("=======================================================")
            print("1. list_mem: list current membership list in the ring")
            print("2. list_self: list current node")
            print("3. join: join current node to the ring")
            print("4. leave: leave current node from the ring")
            print("5. help: show this usage prompt")
            print("6. exit: shutdown this node")
            print("=======================================================")
            print("Please input command:")
            command = input()
            if command == "list_mem":
                with ML_lock:
                    print(self.ML)
            elif command == "list_self":
                print(self.hostname)
            elif command == "join":
                self.join()
            elif command == "leave":
                self.leave()
            elif command == "help":
                print("=======================================================")
                print("1. list_mem: list current membership list in the ring")
                print("2. list_self: list current node")
                print("3. join: join current node to the ring")
                print("4. leave: leave current node from the ring")
                print("5. help: show this usage prompt")
                print("6. exit: shutdown this node")
                print("=======================================================")
            else:
                print("invalid command, use help to check available commands!")

    def run(self):
        tn = threading.Thread(target=self.listen_to_master,
                              name="listen_to_master")
        t1 = threading.Thread(target=self.shell, name="shell")

        tn.start()
        tn.join()
        t1.join()

s = Server()
s.run()