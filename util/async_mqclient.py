#!/usr/bin/env python

import sys
import socket
import time
import logging
import traceback
from struct import *
from tornado import ioloop, iostream

class MQException(Exception):
    def __init__(self, value):
        self.value = value

    def __str__(self):
        return repr(self.value)

# raise me if recv timeout with len(res) == 0
class MQRecvNullException(Exception):
    pass

class MQClient(object):
    __reqh_size = 28 # sizeof(mqreader_req_header_t)
    __info_magic_end = 'MaGiC_END'

    def __init__(self, conf, io_loop=None):
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
        self.io_loop = io_loop or ioloop.IOLoop.instance()
        self.reconnect_times = 0
        self.state = 'init'

    def connect(self):
        self.state = 'connecting'
        self.io_loop.add_callback(self._connect)

    def _connect(self):
        self.state = 'connected'
        self.reconnect_times = 0
        self.socket = socket.socket()
        self.stream = iostream.IOStream(self.socket)
        self.stream.set_close_callback(self.on_close);
        self.stream.connect((self.conf['host'], self.conf['port']), self.send)

    def close(self):
        self.stream.close()
        self.state = 'closed'

    def send(self):
        self.state = 'sending'
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
        logging.debug("REQ %s %s"%(self.reqh, self.mi))

        try:
            self.stream.write(req)
            self.stream.read_bytes(68, self.read_header)
        except:
            logging.error('error on reading payload')
            self.on_error()

    def read_header(self,res):
        self.state = 'reading_head'
        resht = unpack("IIIIHBB24sI16sI", res)
        self.resh = {
            'magic':resht[0],
            'now':resht[1],
            'xid':resht[2],
            'size':resht[3],
            'server_id':resht[4],
            'type':resht[5],
            'flags':resht[6],
            'key':resht[7].rstrip(chr(0)),
            'reserved':resht[8],
        }
        self.mi = {
            'log_name':resht[9],
            'log_pos':resht[10],
        }

        logging.debug("%s %s"%(self.resh, self.mi))
        try:
            self.stream.read_bytes(self.resh['size'],self.read_payload)
        except:
            logging.error('error on reading payload')
            self.on_error()

    def read_payload(self,payload):
        self.state = 'reading_payload'
        self.res_body = payload
        self.save_minfo()
        try:
            self.onRecv()
            self.stream.read_bytes(68, self.read_header)
        except:
            logging.exception("error on reading header")
            traceback.print_exc(file=sys.stdout)
            self.on_error()

    def on_close(self):
        if self.state == 'pending_reconnect':
            return
        self.close()
        self.reconnect_times += 1
        if self.reconnect_times >= 1000:
            self.reconnect_times = 0
            #self.io_loop.stop()
        logging.debug("MQ Socket disconnected, reconnecting...")
        self.state = 'pending_reconnect'
        self.io_loop.add_timeout(time.time() + 1, self.connect)
        
    def on_error(self):
        if self.state == 'pending_reconnect':
            return
        self.close()
        self.reconnect_times += 1
        if self.reconnect_times >= 1000:
            self.reconnect_times = 0
            #self.io_loop.stop()
        logging.debug("MQ Socket disconnected, reconnecting...")
        self.state = 'pending_reconnect'
        self.io_loop.add_timeout(time.time() + 1, self.connect)

    def onRecv(self):
        '''should be implemented in subclass'''
        print "key="+self.resh['key']
        print self.res_body

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

def run_loop(handler_cls, conf):
    t = handler_cls(conf)
    t.connect()
    io_loop = ioloop.IOLoop.instance()
    io_loop.start()

if __name__ == '__main__':
    s = {
        "host":"192.168.100.17",
        "port":9410,
        "server_id":3,
        "server_name":"mq-rclient",
        "data_path":"./data",
    }
    c = MQClient(s)
    io_loop = ioloop.IOLoop.instance()
    io_loop.start()

