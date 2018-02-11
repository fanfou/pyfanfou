#!/usr/bin/env python
#coding=utf-8
import sys
reload(sys)
sys.setdefaultencoding('utf-8')

import MySQLdb

'''
    对 MySQLdb 的封装
'''
class SKDB:

    db = {}
    conf = {}

    '''
        - conf dictionary key: host/user/passwd/db/port/charset/int autocommit
        - see http://mysql-python.sourceforge.net/MySQLdb.html
    '''
    def __init__(self, conf):
        self.db = MySQLdb.connect(host = conf['host'], port = conf['port'], user = conf['user'], passwd = conf['password'], db = conf['database'])
        if not 'charset' in conf:
            self.db.set_character_set('utf8')
        else:
            self.db.set_character_set(conf['charset'])
        if not 'autocommit' in conf:
            self.db.autocommit(0)
        else:
            self.db.autocommit(conf['autocommit'])

    '''
        查询sql，返回mysql_result对象
        - sql string
        - return (mysql_result, int num_rows)
    '''
    def query(self, sql):
        self.db.query(sql)
        r = self.db.store_result()
        rnum = r.num_rows()
        return r, rnum

    '''
        执行写操作sql，返回affected_rows
        - sql string
        - return int affected_rows
    '''
    def execute(self, sql):
        self.db.query(sql)
        return self.db.affected_rows()

    '''
        转义字符串
    '''
    def escape_string(self, string):
        return self.db.escape_string(string)

    '''
        提交
    '''
    def commit(self):
        return self.db.commit()

    '''
        回滚
    '''
    def rollback(self):
        return self.db.rollback()

    '''
        设置是否自动提交事务
        - flag int 0/1
        - return void
    '''
    def autocommit(self, flag):
        return self.db.autocommit(flag)

    '''
        检查连接是否可用 对应 MySQL API: mysql_ping()
        - return int 0正常 非0错误
    '''
    def ping(self):
        return self.db.ping()

    '''
        获取对象单态实例 (非线程安全)
        - id mixed 标识
        - return SKDB
    '''
    @staticmethod
    def getInstance(conf = None):
        if conf == None:
            _conf = SKDB.conf
        else:
            _conf = conf
        id = str(_conf)
        if not id in SKDB.db:
            SKDB.db[id] = SKDB(_conf)
        else:
            try:
                SKDB.db[id].ping()
            except MySQLdb.OperationalError, e:
                SKDB.db[id] = SKDB(_conf)
        return SKDB.db[id]

    @staticmethod
    def setDefaultConf(conf):
        SKDB.conf = conf

    '''
        返回单行结果
        - sql string
        - name list<string> 结果集每列下标
        - return list
    '''
    def getOneRow(self, sql, name = None):
        r, rnum = self.query(sql)
        result = []
        if rnum > 0:
            row = r.fetch_row()
            for i in range(len(row[0])):
                result.append(row[0][i])
        if name == None:
            return result
        else:
            return dict(zip(name, result))

    '''
        返回单列结果
        - sql string
        - return list
    '''
    def getOneCell(self, sql):
        r, rnum = self.query(sql)
        for i in xrange(rnum):
            row = r.fetch_row()
            yield row[0][0]

    '''
        返回单个结果
        - sql string
        - return None|mixed
    '''
    def getOne(self, sql):
        r, rnum = self.query(sql)
        result = None
        if rnum > 0:
            row = r.fetch_row()
            result = row[0][0]
        return result

    '''
        返回多行多列结果
        - sql string
        - name list<string> 结果集每列下标
        - return list< map<name, value> >
    '''
    def getMultiRows(self, sql, name):
        r, rnum = self.query(sql)
        crange = range(len(name))
        #result = []
        if rnum > 0:
            for i in range(rnum):
                row = r.fetch_row()
                dict = {}
                for j in crange:
                    dict[name[j]] = row[0][j]
                #result.append(dict)
                yield dict
        #return result

if __name__ == '__main__':

    # for unittest only

    dbConf = {
        'host' : '192.168.0.250',
        'user' : 'q3boy',
        'passwd' : '123',
        'db' : 'core',
        'port' : 5002,
        'charset' : 'utf8',
        'autocommit' : 0,
    }

    SKDB.setDefaultConf(dbConf)

    print SKDB.getInstance().escape_string('\' and \'')

    sql = 'SELECT COUNT(*) FROM `group`'
    print SKDB.getInstance().getOne(sql)
    sql = 'SELECT `id` FROM `useracct` WHERE `id` < 100002 LIMIT 10'
    print SKDB.getInstance().getOneCell(sql)
    sql = 'SELECT `id`, `loginname`, `realname` FROM `useracct` WHERE `id` = 114817'
    print SKDB.getInstance().getOneRow(sql)
    print SKDB.getInstance().getOneRow(sql, ['id', 'loginname', 'realname'])
    sql = 'SELECT `id`, `loginname`, `realname` FROM `useracct` WHERE `id` IN (114817, 100001)'
    print SKDB.getInstance().getMultiRows(sql, ['id', 'loginname', 'realname'])
