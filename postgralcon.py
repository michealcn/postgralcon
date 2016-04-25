#!/bin/env python
#-*- coding:utf-8 -*-

__author__ = 'diego.zhu'
__email__ = 'zhuhaiyang55@gmail.com'

import json
import time
import socket
import os
import re
import sys
import getopt
import commands
import urllib2, base64
import psycopg2
import psycopg2.extras
import traceback

debug = False
timestamp = int(time.time())
falconAgentUrl = 'http://127.0.0.1:1988/v1/push'
Step = 60
Metric = 'postgresql'
#send data when error happened
alwaysSend = True
defaultDataWhenFailed = -1
host = '127.0.0.1'
port = '5432'
user = 'postgres'
pswd = 'postgres'
db = 'postgres'
endPoint = socket.gethostname()

class Postgralcon:

    _conn = None
    _curs = None

    monit_keys = [
        'connections',
        'commits',
        'rollbacks',
        'disk_read',
        'buffer_hit',
        'rows_returned',
        'rows_fetched',
        'rows_inserted',
        'rows_updated',
        'rows_deleted',
        'database_size',
        'deadlocks',
        'temp_bytes',
        'temp_files',
        'bgwriter.checkpoints_timed',
        'bgwriter.checkpoints_requested',
        'bgwriter.buffers_checkpoint',
        'bgwriter.buffers_clean',
        'bgwriter.maxwritten_clean',
        'bgwriter.buffers_backend',
        'bgwriter.buffers_alloc',
        'bgwriter.buffersbackendfsync',
        'bgwriter.write_time',
        'bgwriter.sync_time',
        'locks',
        'seq_scans',
        'seqrowsread',
        'index_scans',
        'indexrowsfetched',
        'rowshotupdated',
        'live_rows',
        'dead_rows',
        'indexrowsread',
        'table_size',
        'index_size',
        'total_size',
        'table.count',
        'max_connections',
        'percentusageconnections',
        'heapblocksread',
        'heapblockshit',
        'indexblocksread',
        'indexblockshit',
        'toastblocksread',
        'toastblockshit',
        'toastindexblocks_read',
        'toastindexblocks_hit'
    ]

    def __init__(self):
        self.connected = False

    def tryConnect(self):
        if(self.connected):
            return
        self._conn = psycopg2.connect(host=host , user=user, password=pswd , port=port, database=db)
        self._curs = self._conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
        self.connected = True
         
    def __get(self,sql):
        self._curs.execute(sql)
        return self._curs.fetchone()

    def newFalconData(self,key,val,CounterType = 'GAUGE',TAGS = None):
        return {
                'Metric': '%s.%s' % (Metric, key),
                'Endpoint': endPoint,
                'Timestamp': timestamp,
                'Step': Step,
                'Value': val,
                'CounterType': CounterType,
                'TAGS': TAGS
            }
    
    def get_connections(self):
        v = self.__get('SELECT count(*) FROM pg_stat_activity WHERE NOT pid=pg_backend_pid();')[0]
        return self.newFalconData(key='connections',val=v);
    
    def get_commits(self):
        v = self.__get('SELECT count(*) FROM pg_stat_activity WHERE NOT pid=pg_backend_pid();')[0]
        return self.newFalconData(key='commits',val=v);
    
    def get_rollbacks(self):
        v = self.__get('SELECT count(*) FROM pg_stat_activity WHERE NOT pid=pg_backend_pid();')[0]
        return self.newFalconData(key='rollbacks',val=v);
    
    def get_disk_read(self):
        v = self.__get('SELECT count(*) FROM pg_stat_activity WHERE NOT pid=pg_backend_pid();')[0]
        return self.newFalconData(key='disk_read',val=v);
        
    def get_buffer_hit(self):
        v = self.__get('SELECT count(*) FROM pg_stat_activity WHERE NOT pid=pg_backend_pid();')[0]
        return self.newFalconData(key='buffer_hit',val=v);
        
    def get_rows_returned(self):
        v = self.__get('SELECT count(*) FROM pg_stat_activity WHERE NOT pid=pg_backend_pid();')[0]
        return self.newFalconData(key='rows_returned',val=v);
    
    def get_rows_fetched(self):
        v = self.__get('SELECT count(*) FROM pg_stat_activity WHERE NOT pid=pg_backend_pid();')[0]
        return self.newFalconData(key='rows_fetched',val=v);
        
    def get_rows_inserted(self):
        v = self.__get('SELECT count(*) FROM pg_stat_activity WHERE NOT pid=pg_backend_pid();')[0]
        return self.newFalconData(key='rows_inserted',val=v);
    
    def get_rows_updated(self):
        v = self.__get('SELECT count(*) FROM pg_stat_activity WHERE NOT pid=pg_backend_pid();')[0]
        return self.newFalconData(key='rows_updated',val=v);
        
    def get_rows_deleted(self):
        v = self.__get('SELECT count(*) FROM pg_stat_activity WHERE NOT pid=pg_backend_pid();')[0]
        return self.newFalconData(key='rows_deleted',val=v);
        
    def get_database_size(self):
        v = self.__get('SELECT count(*) FROM pg_stat_activity WHERE NOT pid=pg_backend_pid();')[0]
        return self.newFalconData(key='database_size',val=v);
        
    def get_deadlocks(self):
        v = self.__get('SELECT count(*) FROM pg_stat_activity WHERE NOT pid=pg_backend_pid();')[0]
        return self.newFalconData(key='deadlocks',val=v);
    
    def get_temp_bytes(self):
        v = self.__get('SELECT count(*) FROM pg_stat_activity WHERE NOT pid=pg_backend_pid();')[0]
        return self.newFalconData(key='temp_bytes',val=v);
    
    def get_temp_files(self):
        v = self.__get('SELECT count(*) FROM pg_stat_activity WHERE NOT pid=pg_backend_pid();')[0]
        return self.newFalconData(key='temp_files',val=v);
    
    def get_bgwriter.checkpoints_timed(self):
        v = self.__get('SELECT count(*) FROM pg_stat_activity WHERE NOT pid=pg_backend_pid();')[0]
        return self.newFalconData(key='bgwriter.checkpoints_timed',val=v);
    
    def get_bgwriter.checkpoints_requested(self):
        v = self.__get('SELECT count(*) FROM pg_stat_activity WHERE NOT pid=pg_backend_pid();')[0]
        return self.newFalconData(key='bgwriter.checkpoints_requested',val=v);
    
    def get_bgwriter.buffers_checkpoint(self):
        v = self.__get('SELECT count(*) FROM pg_stat_activity WHERE NOT pid=pg_backend_pid();')[0]
        return self.newFalconData(key='bgwriter.buffers_checkpoint',val=v);
    
    def get_bgwriter.buffers_clean(self):
        v = self.__get('SELECT count(*) FROM pg_stat_activity WHERE NOT pid=pg_backend_pid();')[0]
        return self.newFalconData(key='bgwriter.buffers_clean',val=v);
    
    def get_bgwriter.maxwritten_clean(self):
        v = self.__get('SELECT count(*) FROM pg_stat_activity WHERE NOT pid=pg_backend_pid();')[0]
        return self.newFalconData(key='bgwriter.maxwritten_clean',val=v);
    
    def get_bgwriter.buffers_backend(self):
        v = self.__get('SELECT count(*) FROM pg_stat_activity WHERE NOT pid=pg_backend_pid();')[0]
        return self.newFalconData(key='bgwriter.buffers_backend',val=v);
    
    def get_bgwriter.buffers_alloc(self):
        v = self.__get('SELECT count(*) FROM pg_stat_activity WHERE NOT pid=pg_backend_pid();')[0]
        return self.newFalconData(key='bgwriter.buffers_alloc',val=v);
        
    def get_bgwriter.buffersbackendfsync(self):
        v = self.__get('SELECT count(*) FROM pg_stat_activity WHERE NOT pid=pg_backend_pid();')[0]
        return self.newFalconData(key='bgwriter.buffersbackendfsync',val=v);
    
    def get_bgwriter.write_time(self):
        v = self.__get('SELECT count(*) FROM pg_stat_activity WHERE NOT pid=pg_backend_pid();')[0]
        return self.newFalconData(key='bgwriter.write_time',val=v);
    
    def get_bgwriter.sync_time(self):
        v = self.__get('SELECT count(*) FROM pg_stat_activity WHERE NOT pid=pg_backend_pid();')[0]
        return self.newFalconData(key='bgwriter.sync_time',val=v);
    
    def get_locks(self):
        v = self.__get('SELECT count(*) FROM pg_stat_activity WHERE NOT pid=pg_backend_pid();')[0]
        return self.newFalconData(key='locks',val=v);
        
    def get_seq_scans(self):
        v = self.__get('SELECT count(*) FROM pg_stat_activity WHERE NOT pid=pg_backend_pid();')[0]
        return self.newFalconData(key='seq_scans',val=v);
    
    def get_seqrowsread(self):
        v = self.__get('SELECT count(*) FROM pg_stat_activity WHERE NOT pid=pg_backend_pid();')[0]
        return self.newFalconData(key='seqrowsread',val=v);
    
    def get_index_scans(self):
        v = self.__get('SELECT count(*) FROM pg_stat_activity WHERE NOT pid=pg_backend_pid();')[0]
        return self.newFalconData(key='index_scans',val=v);
        
    def get_indexrowsfetched(self):
        v = self.__get('SELECT count(*) FROM pg_stat_activity WHERE NOT pid=pg_backend_pid();')[0]
        return self.newFalconData(key='indexrowsfetched',val=v);
        
    def get_rowshotupdated(self):
        v = self.__get('SELECT count(*) FROM pg_stat_activity WHERE NOT pid=pg_backend_pid();')[0]
        return self.newFalconData(key='rowshotupdated',val=v);
        
    def get_live_rows(self):
        v = self.__get('SELECT count(*) FROM pg_stat_activity WHERE NOT pid=pg_backend_pid();')[0]
        return self.newFalconData(key='live_rows',val=v);
        
    def get_dead_rows(self):
        v = self.__get('SELECT count(*) FROM pg_stat_activity WHERE NOT pid=pg_backend_pid();')[0]
        return self.newFalconData(key='dead_rows',val=v);
        
    def get_indexrowsread(self):
        v = self.__get('SELECT count(*) FROM pg_stat_activity WHERE NOT pid=pg_backend_pid();')[0]
        return self.newFalconData(key='indexrowsread',val=v);
    
    def get_table_size(self):
        v = self.__get('SELECT count(*) FROM pg_stat_activity WHERE NOT pid=pg_backend_pid();')[0]
        return self.newFalconData(key='table_size',val=v);
        
    def get_index_size(self):
        v = self.__get('SELECT count(*) FROM pg_stat_activity WHERE NOT pid=pg_backend_pid();')[0]
        return self.newFalconData(key='index_size',val=v);
        
    def get_total_size(self):
        v = self.__get('SELECT count(*) FROM pg_stat_activity WHERE NOT pid=pg_backend_pid();')[0]
        return self.newFalconData(key='total_size',val=v);
        
    def get_table.count(self):
        v = self.__get('SELECT count(*) FROM pg_stat_activity WHERE NOT pid=pg_backend_pid();')[0]
        return self.newFalconData(key='table.count',val=v);
        
    def get_max_connections(self):
        v = self.__get('SELECT count(*) FROM pg_stat_activity WHERE NOT pid=pg_backend_pid();')[0]
        return self.newFalconData(key='max_connections',val=v);
        
    def get_percentusageconnections(self):
        v = self.__get('SELECT count(*) FROM pg_stat_activity WHERE NOT pid=pg_backend_pid();')[0]
        return self.newFalconData(key='percentusageconnections',val=v);
        
    def get_heapblocksread(self):
        v = self.__get('SELECT count(*) FROM pg_stat_activity WHERE NOT pid=pg_backend_pid();')[0]
        return self.newFalconData(key='heapblocksread',val=v);
        
    def get_heapblockshit(self):
        v = self.__get('SELECT count(*) FROM pg_stat_activity WHERE NOT pid=pg_backend_pid();')[0]
        return self.newFalconData(key='heapblockshit',val=v);
        
    def get_indexblocksread(self):
        v = self.__get('SELECT count(*) FROM pg_stat_activity WHERE NOT pid=pg_backend_pid();')[0]
        return self.newFalconData(key='indexblocksread',val=v);
        
    def get_indexblockshit(self):
        v = self.__get('SELECT count(*) FROM pg_stat_activity WHERE NOT pid=pg_backend_pid();')[0]
        return self.newFalconData(key='indexblockshit',val=v);
        
    def get_toastblocksread(self):
        v = self.__get('SELECT count(*) FROM pg_stat_activity WHERE NOT pid=pg_backend_pid();')[0]
        return self.newFalconData(key='toastblocksread',val=v);
    
    def get_toastblockshit(self):
        v = self.__get('SELECT count(*) FROM pg_stat_activity WHERE NOT pid=pg_backend_pid();')[0]
        return self.newFalconData(key='toastblockshit',val=v);
    
    def get_toastindexblocks_read(self):
        v = self.__get('SELECT count(*) FROM pg_stat_activity WHERE NOT pid=pg_backend_pid();')[0]
        return self.newFalconData(key='toastindexblocks_read',val=v);
        
    def get_toastindexblocks_hit(self):
        v = self.__get('SELECT count(*) FROM pg_stat_activity WHERE NOT pid=pg_backend_pid();')[0]
        return self.newFalconData(key='toastindexblocks_hit',val=v);
        
    def get_database_size(self):
        v = self.__get('select pg_database_size(\''+self._db+'\');')[0]
        return self.newFalconData(key='database_size',val=v);
    
    def get_blocked(self):
        sql = 'SELECT count(*) FROM pg_locks bl'
        sql += ' JOIN pg_stat_activity a ON a.pid = bl.pid '
        sql += ' JOIN pg_locks kl ON kl.transactionid = bl.transactionid AND kl.pid != bl.pid '
        sql += ' JOIN pg_stat_activity ka ON ka.pid = kl.pid WHERE NOT bl.granted;'
        v = self.__get(sql)[0]
        return self.newFalconData(key='blocked',val=v)

if(debug): 
    print "psycopg2 version: "+psycopg2.__version__

def usage():
    print ''
    print 'python postgralcon.py [options]'
    print ''
    print '    -a always send if failed collectting data , kind of falcon nodata , default True'
    print '    -D debug , default False'
    print '    -v default data when failed , default -1'
    print '    -t time-interval , default 60 in second'
    print '    -f falcon-agent-push-url , default http://127.0.0.1:1988/v1/push'
    print '    -m metric , default postgresql'
    print '    -h host:port , default 127.0.0.1:5432'
    print '    -u database user , default postgresql'
    print '    -p database password , default postgresql'
    print '    -e end point , default hostname'
    sys.exit(2)

def main():
    if len(sys.argv[1:]) == 0:
        usage()

    try:
        opts, args = getopt.getopt(sys.argv[1:],"t:f:a:v:d:h:u:p:D:h:e",['--help'])
    except getopt.GetoptError:
        usage()

    global debug,timestamp,falconAgentUrl,Step,Metric,alwaysSend,defaultDataWhenFailed,host,port,user,pswd,db
    for opt, arg in opts:
        if opt in ('-h','--help'):
            usage()
        if opt == '-t':
            Step = arg
        elif opt == '-f':
            falconAgentUrl = arg
        elif opt == '-m':
            Metric = 'postgresql'
        elif opt == '-a':
            alwaysSend = arg
        elif opt == '-e':
            endPoint = arg
        elif opt == '-v':
            defaultDataWhenFailed = arg
        elif opt == '-h':
            if arg.find(":") == -1:
                print 'illegel param -h %s , should be host:port' % (arg)
                sys.exit(2)
            host = arg.split(':')[0]
            port = arg.split(':')[1]
        elif opt == '-u':
            user = arg
        elif opt == '-p':
            pswd = arg
        elif opt == '-D':
            db = arg
    print '%s@%s:%s/%s %s %s' %(user,host,port,db,falconAgentUrl,Metric)
    data = []
    monitor = Postgralcon()
    for key in Postgralcon.monit_keys:
        try:
            monitor.tryConnect();
            func_name = "get_"+key
            if hasattr(monitor, func_name):
                func = getattr(monitor, func_name)
                d = func()
                print '%s %s' % (key,d['Value'])
                data.append(d)
            else:
                print'[not supportted yet]'+key
        except Exception, e:
            print '%s %s' % ('[error happened]' , key)
            if(alwaysSend):
                data.append(monitor.newFalconData(key=key,val=defaultDataWhenFailed))
            if(debug):
                print traceback.format_exc()
            continue
    if(debug): 
        print json.dumps(data, sort_keys=True,indent=4)
    method = "POST"
    handler = urllib2.HTTPHandler()
    opener = urllib2.build_opener(handler)
    request = urllib2.Request(falconAgentUrl, data=json.dumps(data) )
    request.add_header("Content-Type",'application/json')
    request.get_method = lambda: method
    try:
        connection = opener.open(request)
    except urllib2.HTTPError,e:
        connection = e

    # check. Substitute with appropriate HTTP code.
    if connection.code == 200:
        print connection.read()
    else:
        print '{"err":1,"msg":"%s"}' % connection

main()
