#!/usr/bin/env python

import sys
import socket
import time
import logging
import traceback
from struct import *

class MQException(Exception):
    def __init__(self, value):
        self.value = value

    def __str__(self):
        return repr(self.value)

# raise me if recv timeout with len(res) == 0
class MQRecvNullException(Exception):
    pass

class MQClient:
    __reqh_size = 28 # sizeof(mqreader_req_header_t)
    __info_magic_end = 'MaGiC_END'

    def __init__(self, conf):
        self.conf = conf
        self.reqh = {
                'magic':0xE69626EF,
                'version':1,
                'cmd':1,
                'server_id':conf['server_id'],
                'server_name':conf['server_name'],
                'body_size':20 # sizeof(master_info_t)
                }
        self.mi = {}
        self.mi_file = self.conf['data_path'] + '/master.info'
        self.mi_fd = file(self.mi_file, 'r+')

    def __del__(self):
        self.mi_fd.close()

    def connect(self):
        self.socket = socket.socket()
        self.socket.settimeout(10.0)
        self.socket.connect((self.conf['host'], self.conf['port']))

    def close(self):
        self.socket.close()

    def get_minfo(self):
        self.mi_fd.seek(0) # to the begining
        line = self.mi_fd.readline()
        l = line.split()
        if len(l) != 3 :
            raise MQException, "mi_file format error: " + line
        elif l[2] != self.__info_magic_end :
            raise MQException, "mi_file ending magic not correct: " + l[2]

        self.mi['log_name'], self.mi['log_pos'] = l[0], int(l[1])

    def save_minfo(self):
        line = "%s\t%d\t%s"%(self.mi['log_name'], self.mi['log_pos'], self.__info_magic_end)
        logging.debug(line);
        self.mi_fd.seek(0)
        self.mi_fd.write(line + '\n')
        self.mi_fd.flush()

    def send(self):
        self.get_minfo()

        # req_header 28b + mi 20b
        req = pack("IBBH16sI16sI", 
                self.reqh["magic"],
                self.reqh["version"],
                self.reqh["cmd"],
                self.reqh["server_id"],
                self.reqh["server_name"],
                self.reqh["body_size"],
                self.mi["log_name"],
                self.mi["log_pos"])
        assert(len(req) == self.__reqh_size + self.reqh["body_size"])
        logging.info("REQ %s %s"%(self.reqh, self.mi))
        self.socket.send(req) #, socket.MSG_NOSIGNAL)

    def recv(self):
        '''return True when OK, False on error'''
        # event_header 48b + minfo 20b + event_body
        res = self.socket.recv(68)
        lenres = len(res)
        if lenres != 68:
            if lenres == 0:
                raise MQRecvNullException()
            else:
                raise Exception("recv 68 bytes returned [%s]"%res)
        resht = unpack("IIIIHBB24sI16sI", res)
        self.resh = {
                'magic':resht[0],
                'now':resht[1],
                'xid':resht[2],
                'size':resht[3],
                'server_id':resht[4],
                'type':resht[5],
                'flags':resht[6],
                'key':resht[7],
                'reserved':resht[8],
                }
        self.mi = {
                'log_name':resht[9],
                'log_pos':resht[10],
                }

        logging.debug("%s %s"%(self.resh, self.mi))
        self.res_body = self.socket.recv(self.resh['size'])
        if len(self.res_body) != self.resh['size']:
            return Exception("recv %u bytes returned [%s]"%(self.resh['size'], self.res_body))

        self.save_minfo()


    def onRecv(self):
        '''should be implemented in subclass'''
        pass

    def main(self):
        while True:
            try:
                self.connect()
                self.send()
                i = 0
                while True:
                    self.recv()
                    self.onRecv()
                    # process 
                    i += 1
                    if i == 1000:
                        time.sleep(1)
                        i = 0

            except MQRecvNullException:
                logging.debug("recv null")
                time.sleep(1)
            except Exception:
                logging.exception("main loop")
                traceback.print_exc(file=sys.stdout)
                time.sleep(1)

            self.close()


if __name__ == '__main__':
    count = 10000
    if len(sys.argv) > 1:
        count = int(sys.argv[1])

    logging.basicConfig(**{"filename":"logs/rclient.log", "level":10, 'format' : '%(asctime)s %(levelname)s %(message)s', 'filemode' : 'a'})

    print >> sys.stderr, "repeating %d times..."%count
    t = time.time()
    s = {
            "host":"127.0.0.1",
            "port":9910,
            'server_id':2,
            'server_name':'mq-rclient',
            'data_path':'./data',
            }
    c = MQClient(s)
    c.main()
    dt = time.time() - t
    print >> sys.stderr, "msg=%d time=%f msg/sec=%f\n"%(count, dt, count/dt)



