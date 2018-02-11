#!/usr/bin/env python
import sys
import ConfigParser
import traceback
import time
import logging
import httplib
import urlparse
import urllib
import json
import conf

import pusher
from pyfanfou.util.mqclient import MQClient

def force_string(data, encoding='utf-8'):
    if isinstance(data, dict):
        newdata = {}
        for k, v in data.iteritems():
            newdata[force_string(k, encoding)] = force_string(v, encoding)
        return newdata
    elif isinstance(data, list):
        return [force_string(v, encoding) for v in data]
    elif isinstance(data, tuple):
        return tuple(force_string(v, encoding) for v in data)
    elif isinstance(data, unicode):
        return data.encode(encoding)
    else:
        return data

class UpdateHandler(MQClient):
    def onRecv(self):
        key = self.resh['key']
        idx = key.find('\x00')
        if idx >= 0:
            key = key[:idx]
        try:
            args = json.loads(self.res_body)
            self.handle_pusher(key, args)
        except:
            import traceback
            traceback.print_exc()
            return
        
    def handle_pusher(self, key, args):
        if key == 'message.create':
            if args['status'] == 0:
                pusher.pusher_status_create(args['id'], args['sendfrom'])
        elif key == 'message.delete':
            pusher.pusher_status_delete(args['id'], args['sendfrom'])
        elif key == 'friends.create':
            fromid = args['from']['id']
            toid = args['to']['id']
            pusher.pusher_friend_link(fromid, toid)
        elif key in ('friends.destroy', 'friends.block'):
            fromid = args['from']['id']
            toid = args['to']['id']
            pusher.pusher_friend_unlink(fromid, toid)
        elif key == 'statuspush.add':
            pusher.pusher_load(args['userid'])
        elif key == 'user.login':
            userid = args['id']
            pusher.pusher_user_login(userid)
        else:
            #print key, args
            pass

def run_service():
    logging_conf = conf.loggingConf
    logging.basicConfig(**logging_conf)

    mqConf = conf.mqConf
    t = UpdateHandler(mqConf)
    print 'server starting ...'
    t.main()
    print 'service stop'
    
if __name__ == '__main__':
    run_service()
