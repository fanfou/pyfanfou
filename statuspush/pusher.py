import sys
import time
import redis
import conf
from pyfanfou.util.db import SKDB

_rserver = None
def redisServer():
    global _rserver
    if _rserver is None:
        redisConf = conf.redisConf
        _rserver = redis.Redis(redisConf['host'])
    return _rserver

def pusher_flushall(*args):
    rserver = redisServer()
    rserver.flushall()

def pusher_trim(userid):
    userid = int(userid)
    rserver = redisServer()
    rserver.zremrangebyrank('hme.%s' % userid, 1000, -1)

def pusher_init(*args):
    rserver = redisServer()
    conn = SKDB(conf.msgDBConf)
    maxid = conn.getOne('select max(id) from msg')
    lastid = rserver.get('last.status.id')
    if not lastid:
        lastid = maxid - 100000
    rserver.set('last.status.id', maxid)
    print 'minid', lastid, 'maxid', maxid
    r, num = conn.query('select id, sendfrom from msg where status=0 and id > %s and id <= %s order by id' % (lastid, maxid))
    for _ in xrange(num):
        t = r.fetch_row()
        msgid, sendfrom = t[0]
        pusher_status_create(msgid, sendfrom)
        #key = 'stus.%s' % sendfrom
        #rserver.zadd(key, msgid, -msgid)

def force_pusher_init():
    rserver = redisServer()
    lastid = rserver.get('last.status.id')
    if not lastid:
        print 'initializing pusher'
        pusher_init()

def loadStatusList(userid):
    conn = SKDB(conf.ffMsgDBConf)
    rserver = redisServer()
    key = 'stus.%s' % userid
    s = set()
    if rserver.zcard(key) <= 0:
        for msgid in conn.getOneCell('select msgid from profile where k=%s and level=0 order by msgid desc' % userid):
            rserver.zadd(key, msgid, -msgid)
            s.add(msgid)
    return s

def pusher_status_create(msgid, sendfrom):
    rserver = redisServer()
    msgid = int(msgid)
    lastid = rserver.get('last.status.id')
    if not lastid or int(lastid) < msgid:
        rserver.set('last.status.id', msgid)
    rserver.zadd('stus.%s' % sendfrom, msgid, -msgid)

    if rserver.zcard('hme.%s' % sendfrom) > 0:
        rserver.zadd('hme.%s' % sendfrom, msgid, -msgid)
        rserver.publish('hme.%s' % sendfrom, msgid)

    for followerid in rserver.zrange('flw.%s' % sendfrom, 0, -1):
        rserver.zadd('hme.%s' % followerid, msgid, -msgid)
        rserver.publish('hme.%s' % followerid, msgid)
        if rserver.zcard('hme.%s' % followerid) > 3000:
            rserver.zremrangebyrank('hme.%s' % followerid, 1000, -1)

def pusher_status_delete(msgid, sendfrom):
    rserver = redisServer()
    msgid = int(msgid)
    sendfrom = int(sendfrom)
    rserver.zrem('stus.%s' % sendfrom, msgid)
    rserver.zrem('hme.%s' % sendfrom, msgid)
    rserver.publish('hme.%s' % sendfrom, -msgid)
    for followerid in rserver.zrange('flw.%s' % sendfrom, 0, -1):
        rserver.zrem('hme.%s' % followerid, msgid)
        rserver.publish('hme.%s' % followerid, -msgid)

def pusher_user_login(userid, lasttime=0):
    rserver = redisServer()
    userid = int(userid)
    if lasttime <= 0:
        lasttime = int(time.time())
    lasttime = int(lasttime)
    for followerid in rserver.zrange('flw.%s' % userid, 0, -1):
        rserver.zadd('frd.%s' % followerid, userid, -lasttime)

def pusher_friend_link(userid, followid):
    rserver = redisServer()
    userid = int(userid)
    now = int(time.time())
    followid = int(followid)
    if rserver.zcard('hme.%s' % userid) > 0:
        rserver.zadd('flw.%s' % followid, userid, -userid)
        rserver.zadd('frd.%s' % userid, followid, -now)
        for msgid in rserver.zrange('stus.%s' % followid, 0, -1):
            print 'added', msgid
            rserver.zadd('hme.%s' % userid, msgid, -int(msgid))

def pusher_friend_unlink(userid, followid):
    rserver = redisServer()
    userid = int(userid)
    followid = int(followid)
    if rserver.zcard('hme.%s' % userid) > 0:
        rserver.zrem('flw.%s' % followid, userid)
        rserver.zrem('frd.%s' % userid, followid)
        for msgid in rserver.zrange('stus.%s' % followid, 0, -1):
            print 'deleted', msgid
            rserver.zrem('hme.%s' % userid, msgid)

def pusher_get(userid, offset=0, count=20):
    rserver = redisServer()
    for msgid in rserver.zrange('hme.%s' % userid, offset, offset + count):
        print msgid

def pusher_load(userid):
    #force_pusher_init()
    userid = int(userid)
    conn = SKDB(conf.dbConf)
    rserver = redisServer()
    if rserver.zcard('hme.%s' % userid) > 0:
        print "uid:", userid, rserver.zcard('hme.%s' % userid)
        print "user home is already loaded!"
        return
    print 'load home timeline of user', userid
    home_msg_set = set(rserver.zrange('stus.%s' % userid, 0, -1))
    # get all friends of userid
    r, num = conn.query('select followid, lasttime from userlink join userinfo on followid = userinfo.id where userid=%s and level=4 order by lasttime desc' % userid)
    for _ in xrange(num):
        followid, lasttime = r.fetch_row()[0]
        rserver.zadd('flw.%s' % followid, userid, -userid)
        rserver.zadd('frd.%s' % userid, followid, -lasttime)
        msg_set = set(rserver.zrange('stus.%s' % followid, 0, -1))
        home_msg_set = home_msg_set.union(msg_set)

    home_msgs = sorted(home_msg_set)
    home_msgs.reverse()
    if len(home_msgs) < 1000:
        print 'home timeline of', userid, 'is not enough'
    for msgid in home_msgs[:1000]:
        rserver.zadd('hme.%s' % userid, msgid, -int(msgid))

def help():
    print 'Usage: %s [command] [args]' % sys.argv[0]
    print 'Commands:'
    print '', 'flushall\t\t\tdelete all keys'
    print '', 'init\t\t\t\tinitialize redis database, importing recent statuses'
    print '', 'load\t\t\t\tload user follow/timeline from database'
    print '', 'get <userid> <offset> <count>\tget user home timeline'
    print '', 'trim <userid>\ttrim user home timeline to 1000'
    print '', 'status_create <statusid> <userid>\ta status is created'
    print '', 'status_delete <statusid> <userid>\ta status is deleted'
    print '', 'friend_link <userid> <followid>\ta friendship is created'
    print '', 'friend_unlink <userid> <followid>\ta friendship is deleted'
    print '', 'user_login <userid>\t\ta user logged in'

if __name__ == '__main__':
    if len(sys.argv) < 2:
        help()
        sys.exit(1)
    commands = sys.argv[1]
    fn = globals().get('pusher_%s' % commands)
    if fn and callable(fn):
        fn(*sys.argv[2:])
    else:
        help()
        sys.exit()
