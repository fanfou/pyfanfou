import socket
import logging
from tornado import ioloop, iostream
from pyfanfou.util.async_mqclient import MQClient, run_loop
import conf
from urllib import urlencode, quote_plus
import json

def cstr(c, encoding='utf-8'):
    if isinstance(c, unicode):
        return c.encode(encoding)
    else:
        return str(c)
    
def flat_dicts(d):
    if isinstance(d, dict):
        for k, v in d.iteritems():
            k = cstr(k)
            for pk, f in flat_dicts(v):
                if not pk:
                    yield k, f
                else:
                    yield '%s.%s' % (k, pk), f
    elif isinstance(d, (list, tuple)):
        for t in d:
            yield None, cstr(t)
    else:
        yield None, cstr(d)

def urlize(d):
    try:
        d = json.loads(d)
    except:
        return quote_plus(cstr(d))
    attrs = list(flat_dicts(d))
    attrs.sort()
    return urlencode(attrs)

trace_server = None
class TraceHandler(MQClient):
    def onRecv(self):
        key = self.resh['key']
        idx = key.find('\x00')
        if idx >= 0:
            key = key[:idx]
        if trace_server:
            trace_server.handle_queue_msg(key, urlize(self.res_body))

class QueueListener(object):
    def __init__(self, stream):
        self.conn_id = str(id(self))
        self.stream = stream
        self.stream.set_close_callback(self._handle_disconnect)
        
    def _handle_disconnect(self):
        del self.server.listeners[self.conn_id]
        self.server = None

class Server:
    def __init__(self, conf):
        self.daemon_host = conf['daemon_host']
        self.daemon_port = conf['daemon_port']
        self.listeners = {}
    
    def handle_accept(self, fd, events):
        conn, addr = self._sock.accept()
        p = QueueListener(iostream.IOStream(conn))
        self.listeners[p.conn_id] = p
        p.server = self
                                
    def start(self):
        self._sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM, 0)
        self._sock.setblocking(0)
        self._sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self._sock.bind((self.daemon_host, self.daemon_port))
        self._sock.listen(128)
        ioloop.IOLoop.instance().add_handler(self._sock.fileno(),
                                             self.handle_accept,
                                             ioloop.IOLoop.READ)
    def handle_queue_msg(self, key, body):
        for connid, listener in self.listeners.iteritems():
            listener.stream.write('%s\t%s\r\n' % (key, body))
        
if __name__ == '__main__':
    logging_conf = conf.loggingConf
    logging.basicConfig(**logging_conf)
    mconf = {}
    mconf.update(conf.mqConf)
    trace_server = Server(conf.traceServerConf)
    trace_server.start()
    run_loop(TraceHandler, mconf)
