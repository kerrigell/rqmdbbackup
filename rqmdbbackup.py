#!/usr/bin/python
# coding:utf-8
# Author:  Justin Ma
# Purpose:
# Created: 2014/5/15
__author__ = 'Justin Ma'
__version__ = '1.0.8'
import os
import sys
import time
import logging
from logging.handlers import RotatingFileHandler
import string
import json
import traceback
from datetime import datetime, date, timedelta
import hashlib
import uuid
import fcntl
import subprocess

try:
    import psutil
    # Kombu  replace pika
    import pika
    import netifaces
except Exception as e:
    print e
    sys.exit(status=-1)

reload(sys)
sys.setdefaultencoding('utf-8')


class LinkConfig(object):
    '''
    access privilges
    '''
    # rabbit mq server information
    rqmconn = {'host': 'rabbitmq.backup.db.local'
        , 'vhost': '/rqmbakup'
        , 'user': 'backup'
        , 'password': '123456'}
    # operator mysql server information
    mysqlConn = {'host': 'opsdb.db.local'
        , 'port': 3306
        , 'user': 'dbbackup'
        , 'passwd': 'dbbackup'
        , 'db': 'db_platform'
        , 'charset': 'utf8'}
    # oracle rman login
    oracle = {'user': 'sys'
        , 'password': 'sys'}
    # mongo login
    mongo = {'user': 'backup'
        , 'password': 'backup'}


class Logger(object):
    def __init__(self, log_name=''):
        self._log = logging.getLogger(name=log_name)
        self._log.setLevel(logging.INFO)
        self._formatter = logging.Formatter("%(asctime)s - %(clientip)s - %(levelname)s - %(message)s")
        self._fh_lists = {}
        self._extra = None

    def add_rotatingfile(self, log_path, maxBytes=1024 * 1024 * 10, backCount=2, level=logging.INFO):
        if log_path not in self._fh_lists.keys():
            fh = RotatingFileHandler(log_path, mode='a',
                                     maxBytes=maxBytes,
                                     backupCount=backCount)
            fh.setLevel(level)
            fh.setFormatter(self._formatter)
            self._log.addHandler(fh)
            self._fh_lists[log_path] = fh

    def add_logfile(self, log_path, level=logging.INFO):
        if log_path not in self._fh_lists.keys():
            fh = logging.FileHandler(log_path)
            fh.setLevel(level)
            fh.setFormatter(self._formatter)
            self._log.addHandler(fh)
            self._fh_lists[log_path] = fh

    def add_streamfile(self, log_path='stdout', level=logging.INFO):
        if log_path not in self._fh_lists.keys():
            ch = logging.StreamHandler()
            ch.setLevel(level)
            ch.setFormatter(self._formatter)
            self._log.addHandler(ch)
            self._fh_lists[log_path] = ch

    def remove_logfile(self, log_path):
        if self._fh_lists.has_key(log_path):
            self._log.removeHandler(self._fh_lists[log_path])
            fh = self._fh_lists.pop(log_path)
            fh.close()
            del fh

    def set_formatter_extra(self, **kwargs):
        self._extra = kwargs

    def log_info(self, msg, indent=0):
        self._log.info("%s%s" % (''.center(indent, ' '), msg), extra=self._extra)

    def log_error(self, msg, indent=0):
        self._log.error("%s%s" % (''.center(indent, ' '), msg), extra=self._extra)

    def log_switch(self, msg, switch=True):
        if switch:
            self.logger.log_info(msg)
        else:
            self.logger.log_error(msg)


def logit(template):
    def __decorator_fun(fun):
        def __decorator(*args, **kwargs):
            result = None
            logger.log_info(template % 'Start')
            try:
                begin = time.time()
                result = fun(*args, **kwargs)
                end = time.time()
                logger.log_info(template % ("[%ss]%s" % (string.zfill(str(end - begin), 4), 'Completed')))
                return result
            except KeyboardInterrupt as e:
                logger.log_error("Send CTRL+C. Process exit!!")
                sys.exit(5)
            except Exception as e:
                logger.log_error(template % e.message)
                logger.log_error(traceback.format_exc())
                #      sys.exit(4)

        return __decorator

    return __decorator_fun


logger = Logger(__name__)


class ExcuteOut(object):
    def __init__(self):
        self.succeed = False
        self.returncode = -99
        self.result = []
        self.error = None
        self.cost = 0


class DataBackup(object):
    _support_db = ['mysqld', 'mongod']  #,'tnslsnr']  #,'memcached','redis-server'

    def __init__(self, store_path, use_mq=False):
        try:
            self._pending_services = {}
            if not os.path.exists(store_path):
                os.mkdir(store_path)
            self.store_path = store_path
            self.addrs = CallbackMQ.get_ip_list()
            self.main_ip = [i for i in self.addrs if i.startswith('10.')][0]
            if use_mq:
                self.use_mq = use_mq
                logger.log_info('Initialize produce rabbit MQ,routing key: mysql')
                self.mq_query = RabbitMQ(conn_params=LinkConfig.rqmconn, exchange='backup', routing_key='mysql')
                logger.log_info('Initialize produce rabbit MQ,routing key: mysql')
                self.mq_rsync = RabbitMQ(conn_params=LinkConfig.rqmconn, exchange='backup', routing_key='rsync')
        except Exception as e:
            logger.log_error("class %s initialization fail: %s" % (DataBackup.__name__, e.message))
            exit()

    @classmethod
    @logit('Run commands in Bash >> %s')
    def run_sys_cmd(cls, cmd):
        def set_non_blockinf(fd):
            fileno = fd.fileno()
            fl = fcntl.fcntl(fileno, fcntl.F_GETFL)
            fcntl.fcntl(fileno, fcntl.F_SETFL, fl | os.O_NONBLOCK)

        out = ExcuteOut()
        logger.log_info("command:%s" % cmd)
        start = time.time()
        proc = subprocess.Popen(cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        #使用subprocess.Popen导致子进程hang住原因是stdout产生的内容太多，超过了系统的buffer
        map(set_non_blockinf, [proc.stdout, proc.stderr])
        while proc.poll() is None:
            try:
                for line in [string.strip(i) for i in proc.stdout.readlines() if len(string.strip(i)) > 0]:
                    out.result.append(line)
                    logger.log_info("result: %s" % line)
            except IOError:
                pass
        out.result += proc.stdout.readlines()
        out.returncode = proc.returncode
        stop = time.time()
        out.cost = stop - start
        logger.log_info("returncode: %s" % out.returncode)
        if out.returncode != 0:
            out.error = [string.strip(i) for i in proc.stderr.readlines()]
            logger.log_error('stderr: %s' % ''.join(out.error))
        else:
            out.succeed = True
        return out

    @classmethod
    @logit('Collect dump informations from mysqld >> %s')
    def get_info_from_mysqld(cls, proc, bind_addr, bind_port):
        @logit("Check mysqldump file >> %s")
        def check_mysqldump_file(dump_path):
            result = False
            try:
                logger.log_info("Checking %s" % dump_path)
                with open(dump_path, mode='r') as dfile:
                    dfile.seek(0, os.SEEK_END)
                    dfile.seek(-200, os.SEEK_CUR)
                    for line in dfile.readlines():
                        if string.find(line, 'Dump completed on') != -1:
                            result = True
            except Exception as e:
                logger.log_error("check error:%s" % e.message)
            finally:
                return result

        '''collect mysqld service information'''
        if proc.name() != 'mysqld': raise Exception("PID:%s is not mysqld process" % proc.pid)
        sock_path = os.path.join(proc.cwd(), 'mysql.sock')
        if not os.path.exists(sock_path): raise Exception("mysql socket not exists:%s" % sock_path)
        # save mysql instance information
        res = {}
        res['type'] = 'mysqld'
        res['tool'] = 'mysqldump'
        res['bind'] = bind_addr
        res['port'] = bind_port
        # dump_type: 1 instance 2 every databases
        res['dump_type'] = 2
        res['datadir'] = proc.cwd()
        res['auth'] = "-S %s" % sock_path
        res['client'] = ['mysql', '-N', res['auth'], '-e "%(sqlcmd)s"']
        # get basedir/datadir/port/version for mysql
        sql_cmd = ';'.join(["show variables like '%s'" % i for i in ['basedir', 'datadir', 'port',
                                                                     'version']])  #'''show variables where Variable_name in ('basedir','datadir','port','version');'''
        out = cls.run_sys_cmd('''%s | grep -v "Logging"''' % " ".join(res['client']) % {'sqlcmd': sql_cmd})
        if out and out.succeed:
            res.update(dict([string.split(string.strip(i), '\t') for i in out.result if len(string.strip(i)) > 0]))
            # update version with major release
            res['version'] = string.split(res['version'], '.')
        res['mysqldump'] = ['mysqldump'
            , '-q -Q --flush-logs'
            , '--force'
            , '' if res['version'][:2] < ['5', '0'] else '-R --opt --single-transaction'
            , res['auth']
            , '${dbname}' if res['dump_type'] == 2 else '--all-databases'
            , ' | gzip > ${bak_path}'
                            # ,'--result-file ${bak_path}'
        ]
        # update shell mysql/mysqldump with basedir path's
        res['mysql'] = res.pop('client')
        for i in ('mysql', 'mysqldump'):
            bin_path = os.path.join(res['basedir'], 'bin', i)
            if os.path.exists(bin_path):
                res[i][0] = bin_path
        res['dump_cmd'] = res.pop('mysqldump')
        res['client'] = res.pop('mysql')
        # get database list
        out = cls.run_sys_cmd(
            '''%s |  egrep -v "Logging|mysql|test|performance_schema|information_schema|Database" ''' % " ".join(
                res['client']) % {'sqlcmd': "show databases"})
        res['databases'] = out.result if out and out.succeed else []
        # change key name of mysqldump to general 
        # ${DATE}_${db}_${port}.sql
        # gzip
        res['dump_file'] = '_'.join(['${bak_date}', '${dbname}', res['port']]) + '.sql.gz'
        res['dump_compress'] = None  #'cd ${store_path} && gzip -f ${dump_file}'
        res['dump_cfile'] = res['dump_file'] + '.gz'
        res['dump_check'] = None  #check_mysqldump_file
        res['dump_md5'] = True
        res[
            'dump_select'] = ''' select id,ip,expired_role,has_rqmbackup from t_database where dbtype='${dbtype}' and ip in (${address}) and port=${dbport} and dbname='${dbname}' and is_online=1 and is_backup=1 order by id'''
        res['dump_expired'] = 1
        return res


    @classmethod
    @logit('Collect dump informations from mongod >> %s')
    def get_info_from_mongod(cls, proc, bind_addr, bind_port):
        '''collect mongod service information'''
        if proc.name() != 'mongod': raise Exception(
            "PID:%s is not mongod process\n%s\n%s" % (proc.pid, proc, ':'.join([bind_addr, bind_port])))
        res = {}
        res['type'] = 'mongod'
        res['tool'] = 'mongodump'
        res['bind'] = bind_addr
        res['port'] = bind_port
        # dump_type: 1 instance 2 every databases
        res['dump_type'] = 1
        res['auth'] = '--username=%s --password=%s' % (LinkConfig.mongo['user'], LinkConfig.mongo['password'])

        bin_path = os.path.dirname(proc.exe()) if os.path.isabs(proc.exe()) else ''
        res['client'] = ['echo "%(sqlcmd)s" |'
            , os.path.join(bin_path, 'mongo')
            , "%s:%s/admin" % (res['bind'], res['port'])
            , res['auth']
            , '--quiet']
        out = cls.run_sys_cmd(" ".join(res['client']) % {'sqlcmd': 'db.version()'})
        if out.succeed and len(out.result) > 0:
            if string.find(out.result[0], 'login failed') != -1 or string.find(out.result[0], 'auth fails') != -1:
                logger.log_error("Mongo Login fails：%s:%s" % (bind_addr, bind_port))
                return None
            res['version'] = string.split(out.result[0], '.')
        out = cls.run_sys_cmd(
            ''' %s | egrep -v "admin|test|local" ''' % " ".join(res['client']) % {'sqlcmd': 'show dbs'})
        res['databases'] = [string.split(i) for i in out.result[0]] if out and out.succeed else None
        res['dump_cmd'] = [os.path.join(bin_path, 'mongodump')
            , "--host %s --port %s" % (res['bind'], res['port'])
            , '--username=%s --password=%s' % (LinkConfig.mongo['user'], LinkConfig.mongo['password'])
            , '-o ${bak_path}']
        res['datadir'] = \
        [os.path.dirname(i.path) for i in proc.open_files() if os.path.basename(i.path) == 'mongod.lock'][0]

        res['dump_file'] = os.path.basename(res['datadir'])
        res['dump_cfile'] = '_'.join([res['dump_file'], '${bak_date}']) + '.tar.gz'
        res['dump_compress'] = 'cd ${store_path} && tar zcvfp ${dump_cfile} ${dump_file} && rm -rf ${dump_file}'
        res['dump_check'] = None
        res['dump_md5'] = True
        res['dump_expired'] = 1
        res[
            'dump_select'] = ''' select id,ip,expired_role,has_rqmbackup from t_database where dbtype='${dbtype}' and ip in (${address}) and port=${dbport}  and is_online=1 and is_backup=1 order by id limit 1'''
        return res


    @classmethod
    @logit('Collect dump informations from oracle >> %s')
    def get_info_from_tnslsnr(cls, proc, bind_addr, bind_port):
        '''collect oracle service information'''
        if proc.name() != 'tnslsnr': raise Exception(
            "PID:%s is not oracle process\n%s\n%s" % (proc.pid, proc, ':'.join([bind_addr, bind_port])))
        res = {}
        res['type'] = 'tnslsnr'
        res['tool'] = 'rman'
        res['bind'] = bind_addr
        res['port'] = bind_port
        # dump_type: 1 instance 2 every databases 3 oracle
        res['dump_type'] = 3
        bin_dir = os.path.dirname(proc.exe())
        res['auth'] = ''
        # su - oracle -c "echo 'select * from v$instance;'|sqlplus / as sysdba"
        res['client'] = ['su - oracle -c "'
            , "echo '%(cmd)s' | "
            , os.path.join(bin_dir, 'sqlplus')
            , '-s'
            , '/ as sysdba'
            , '"']
        out = cls.run_sys_cmd(''' %s | egrep -v "INSTANCE_NAME|--|^$" ''' % " ".join(res['client']) % {
        'cmd': r'select instance_name from v\$instance;'})
        res['databases'] = [string.upper(i) for i in out.result] if out and out.succeed else None
        #/opt/17173/oracle/u01/network/admin/tnsnames.ora judge tnsname, created 
        tnsfile = os.path.join(os.path.dirname(bin_dir), 'network/admin/tnsnames.ora')
        if not os.path.exists(tnsfile):
            raise Exception("%s://%s:%s tnsnames.ora not exists:%s") % (res['type'], res['bind'], res['port'], tnsfile)
        for dbname in res['databases']:
            no_tns = True
            tnsname = 'dbabackup_%s_%s' % (res['port'], dbname)
            with open(tnsfile, mode='r') as fr_tnsfile:
                for line in fr_tnsfile.readlines():
                    if line.startswith(tnsname):
                        has_tns = False
                        break
            if no_tns:
                tnstmpl = '''dbabackup_${dbport}_${dbname} =
  (DESCRIPTION =
    (ADDRESS = (PROTOCOL = TCP)(HOST = ${dbbind})(PORT = ${dbport}))
    (CONNECT_DATA =
      (SERVER = DEDICATED)
      (SID = ${dbname})
    )
  )'''
                with open(tnsfile, mode='a+') as fa_tnsfile:
                    fa_tnsfile.seek(os.SEEK_END)
                    fa_tnsfile.write('\n')
                    fa_tnsfile.write(string.Template(tnstmpl).safe_substitute(dbbind=res['bind']
                                                                              , dbport=res['port']
                                                                              , dbname=dbname))
                    fa_tnsfile.flush()


        #
        rcv_cmd = '''echo \\"connect target ${user}/${password}@dbabackup_${dbport}_${dbname};
        run{
        allocate  channel  cq  type  disk maxpiecesize 4000m;
        backup as compressed backupset database tag '${dbname}${bak_date}' format '${bak_path}/DBfull_%d_%I_%T_%u_%s_%p' include  current  controlfile;
        backup format '${bak_path}/DBctl_%d_%s_%p_%c_%t' current controlfile ;
        sql 'ALTER SYSTEM ARCHIVE LOG CURRENT';
        crosscheck  archivelog all;
        delete noprompt expired archivelog all;
        delete force noprompt obsolete;
        backup archivelog all  tag '${dbname}_ARCH${bak_date}' format '${bak_path}/ARCHfull_%d_%I_%T_%u_%s_%p';
        release channel cq;
        }\\" > ${bak_path}/backup_full.rcv && '''
        res['dump_cmd'] = ['chown -R oracle.oinstall ${bak_path} && '
            , 'su - oracle -c "'
            , string.Template(rcv_cmd).safe_substitute(user=LinkConfig.oracle['user']
                                                       , password=LinkConfig.oracle['password']
                                                       , tnsname=tnsname)
            , os.path.join(bin_dir, 'rman')
            , r'cmdfile=${bak_path}/backup_full.rcv'
            , r'log=${bak_path}/backup.log'
            , '"']
        res['dump_md5'] = False
        res['dump_file'] = ''
        res['dump_type'] = 3
        res['dump_expired'] = 1
        res[
            'dump_select'] = ''' select id,ip,expired_role,has_rqmbackup from t_database where dbtype='${dbtype}' and ip in (${address}) and port=${dbport} and is_online=1 and is_backup=1 order by id limit 1'''
        res['version'] = ['1', '2']
        return res


    @classmethod
    @logit('Calculate file MD5 >> %s')
    def calcmd5(cls, filepath):
        logger.log_info("Calculate %s" % filepath)
        with open(filepath, 'rb') as f:
            md5obj = hashlib.md5()
            md5obj.update(f.read())
            hash = md5obj.hexdigest()
            return hash

    @logit("Update backup table >> %s")
    def update_db(self, dbid, **kwargs):
        result = True
        try:
            values = []
            for key, value in kwargs.iteritems():
                values.append("%s='%s'" % (key, str(value)))
            dump_updte = " update t_database_backup_info set %s where id='%s' " % (','.join(values), dbid)
            logger.log_info(dump_updte)
            res = self.mq_query.produce(ack=True, query=dump_updte)
            if (not res) or res['count'] != 1:
                raise Exception("insert failed for id=%s:%s" % (dbid, res))
        except Exception as e:
            logger.log_error("update failed for id=%s:%s" % (dbid, e.message))
            result = False
        finally:
            return result

    @logit("Insert backup table >> %s")
    def insert_db(self, **kwargs):
        dbid = None
        try:
            fields = []
            values = []
            for key, value in kwargs.iteritems():
                fields.append(key)
                values.append(str(value))
            dump_insert = " insert into t_database_backup_info (%s) values ('%s')" % (','.join(fields)
                                                                                      , "','".join(values))
            logger.log_info(dump_insert)
            res = self.mq_query.produce(ack=True, query=dump_insert)
            if (not res) or res['count'] != 1 or res['result'] == []:
                raise Exception("insert failed for %s:%s" % (kwargs, res))
            dbid = res['result']
        except Exception as e:
            logger.log_error(e.message)
            dbid = None
        finally:
            return dbid

    @classmethod
    def get_expired_day(cls, expired_role):
        exp_list = string.split(expired_role, ';')
        count = len(exp_list)
        expired_day = 0
        try:
            if count >= 1:
                #day
                expired_day = int(exp_list[0])
            if count >= 2:
                weeks, wexp_day = string.split(exp_list[1], '-')
                if str(datetime.today().weekday()) in string.split(weeks, ','):
                    expired_day = int(wexp_day) if int(wexp_day) > expired_day else expired_day
            if count >= 3:
                days, mexp_day = string.split(exp_list[2], '-')
                if str(datetime.today().day) in string.split(days, ','):
                    if mexp_day == '#':
                        expired_day = 0
                    else:
                        expired_day = int(mexp_day) if int(mexp_day) > expired_day else expired_day
        except Exception as e:
            logger.log_error("analyzing expired role [%s]:%s" % (expired_role, e.message))
            expired_day = 0
        if expired_day == 0:
            return '9999-12-31'
        else:
            return str(date.today() + timedelta(days=expired_day))


    @logit('Backup database >> %s')
    def _backup_one(self, **keywords):
        info_key = ['type', 'bind', 'port', 'dump_cmd', 'dump_file', 'dump_type', 'dump_cfile', 'dump_compress',
                    'databases', 'dump_check', 'dump_md5', 'dump_expired', 'dump_select']
        dbtype, dbbind, dbport, tmpl_cmd, tmpl_file, tmpl_type, tmpl_cfile, tmpl_compress, databases, dump_check, dump_md5, dump_expired, dump_select = \
            [keywords[i] if keywords.has_key(i) else None for i in info_key]
        bak_date = time.strftime('%Y-%m-%d', time.localtime(time.time()))
        dbs = databases if tmpl_type in (2, 3) else ['%']
        #dump_select=''' select id,ip from t_database where dbtype='${dbtype}' and ip in (${address}) and port=${dbport} and dbname='${dbname}' and is_online=1 and is_backup=1'''  
        dbtype_store_path = os.path.join(self.store_path, CallbackMQ.exchange_engine_type(dbtype))
        #clean expired backup files
        if dump_expired:
            self.remove_expired_files(dbtype_store_path, dump_expired)
        logger.log_info("Prepare to backup databases:%s" % dbs)
        for dbname in dbs:
            try:
                dburl = "%s://%s:%s/%s" % (dbtype, dbbind, dbport, dbname)
                logger.log_info("backup database: %s" % dburl)
                start_backup_time = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(time.time()))
                logger.log_info("query database id")
                query_sql = string.Template(dump_select).safe_substitute(address="'%s'" % "','".join(self.addrs)
                                                                         , dbport=dbport
                                                                         , dbname=dbname
                                                                         ,
                                                                         dbtype=CallbackMQ.exchange_engine_type(dbtype))
                query_db_res = self.mq_query.produce(ack=True, query=query_sql)
                if (not query_db_res) or query_db_res['count'] != 1:
                    logger.log_info("No matching result: %s" % query_db_res)
                    continue
                dbid, serverip, expired_role, has_rqmbackup = query_db_res['result'][0]
                if has_rqmbackup == 0:
                    deploy_rqm_res = self.mq_query.produce(ack=True,
                                                           query="update t_database set has_rqmbackup=1 where id=%s" % dbid)

                dump_file = string.Template(tmpl_file).safe_substitute(bak_date=bak_date, dbname=dbname)
                store_path = os.path.join(dbtype_store_path
                                          , date.today().strftime('%A')
                )
                if not os.path.exists(store_path): os.makedirs(store_path)
                db_log_file = os.path.join(store_path, (dump_file if dump_file != '' else 'backup') + '.bklog')
                logger.add_logfile(db_log_file)
                bak_path = os.path.join(store_path
                                        , dump_file)
                # insert backup information to db
                bak_db_id = self.insert_db(did=dbid
                                           , bind_ip=serverip
                                           , inst_type=dbtype
                                           , inst_port=dbport
                                           , inst_version='.'.join(keywords['version'])
                                           , dbname=dbname
                                           , backup_tool=keywords['tool']
                                           , bak_start=start_backup_time
                                           , bak_status=0
                                           , rsync_expired=self.get_expired_day(expired_role)
                )
                if bak_db_id is None:
                    continue
                # start to backup this database
                cmd = string.Template(string.join(tmpl_cmd, ' ')).safe_substitute(bak_path=bak_path, dbname=dbname,
                                                                                  bak_date=bak_date, dbbind=dbbind,
                                                                                  dbport=dbport)
                logger.log_info("execute backup command")
                self.update_db(bak_db_id, bak_status=1)
                out = self.run_sys_cmd(cmd)
                if not out or not out.succeed:
                    logger.log_error("backup failed:%s" % dburl)
                    logger.remove_logfile(db_log_file)
                    self.update_db(bak_db_id, bak_status=-1)
                    continue
                if dump_check:
                    logger.log_info("check dump file:%s" % bak_path)
                    self.update_db(bak_db_id, bak_status=2)
                    if not dump_check(bak_path):
                        logger.log_error("check dump file failed,will terminate this backup:%s" % bak_path)
                        logger.remove_logfile(db_log_file)
                        self.update_db(bak_db_id, bak_status=-2)
                        continue
                if tmpl_compress:
                    dump_cfile = string.Template(tmpl_cfile).safe_substitute(bak_date=bak_date, dbname=dbname)
                    logger.log_info("Compress %s to %s" % (dump_file, dump_cfile))
                    self.update_db(bak_db_id, bak_status=3)
                    # compress the backup result file
                    cmd = string.Template(tmpl_compress).safe_substitute(store_path=store_path
                                                                         , dump_file=dump_file
                                                                         , dump_cfile=dump_cfile)
                    out = self.run_sys_cmd(cmd)
                    if not out or not out.succeed:
                        logger.log_error(
                            "compress backup file failed,will terminate this backup:%s" % os.path.join(store_path,
                                                                                                       dump_file))
                        logger.remove_logfile(db_log_file)
                        self.update_db(bak_db_id, bak_status=-3)
                        continue
                    dump_file = dump_cfile
                dump_path = os.path.join(store_path, dump_file)
                if not os.path.exists(dump_path):
                    logger.log_error("no exists backup file:%s" % dump_path)
                    self.update_db(bak_db_id, bak_status=-10)
                    continue
                # get the backup file size
                logger.log_info("count file size of backup result")
                if os.path.isdir(dump_path):
                    bak_size = CallbackMQ.getdirsize(dump_path)
                else:
                    bak_size = os.path.getsize(dump_path)
                # calculate md5 for the backup file
                md5 = None
                if dump_md5:
                    logger.log_info("calculate md5 for backup file")
                    self.update_db(bak_db_id, bak_status=4)
                    md5 = self.calcmd5(dump_path)
                    #bak_filds='did,bind_ip,inst_type,inst_port,inst_version,dbname,backup_tool,file_name,file_size,file_md5,bak_start,bak_end,bak_status'
                stop_backup_time = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(time.time()))
                #update db
                self.update_db(bak_db_id
                               , file_name=dump_file
                               , file_size=bak_size
                               , file_md5=md5
                               , bak_start=start_backup_time
                               , bak_stop=stop_backup_time
                               , bak_status=5)
                # query expired backup files
                expired_select = ''' select id,did,rsync_path,file_size,rsync_expired,bak_start from t_database_backup_info where did='${dbid}' and rsync_status=2 and is_deleted=0 and rsync_expired='${expired_day}' order by id '''
                expired_db_res = self.mq_query.produce(ack=True,
                                                       query=string.Template(expired_select).safe_substitute(dbid=dbid
                                                                                                             ,
                                                                                                             expired_day=bak_date))
                # start rsync
                logger.log_info("start rsync from this server")
                start_rsync = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(time.time()))
                self.update_db(bak_db_id, rsync_status=1, rsync_start=start_rsync)
                rsync_res = self.mq_rsync.produce(ack=True
                                                  , dbid=bak_db_id
                                                  , dbtype=dbtype
                                                  , server_ip=serverip
                                                  , source_path=store_path
                                                  , file_name=dump_file
                                                  , file_size=bak_size
                                                  , file_md5=md5
                                                  , expired_role=expired_role
                                                  , expired_files=expired_db_res['result'] if query_db_res else []
                )
                if rsync_res and rsync_res['status'] == True:
                    logger.log_info("rsync completely: %s" % rsync_res)
                    # update d_platform.t_database_backup_info with rsync result
                    self.update_db(bak_db_id
                                   , rsync_status=2
                                   , rsync_start=rsync_res['start_time']
                                   , rsync_stop=rsync_res['stop_time']
                                   , rsync_path=rsync_res['store_path']
                    )
                    # sign deleted file
                    if rsync_res.has_key('expired_result'):
                        for del_file in rsync_res['expired_result']:
                            #id,did,rsync_path,file_size,rsync_expired,is_deleted
                            if len(del_file) != 7:
                                logger.log_error("Cannot be deleted:%s" % del_file)
                                continue
                            if del_file[5] != 0:
                                self.update_db(del_file[0], is_deleted=del_file[6])
                else:
                    self.update_db(bak_db_id, rsync_status=-1)
                    logger.log_error("rsync failed")
                logger.remove_logfile(db_log_file)
            except Exception as e:
                logger.log_error("%s backup failed:%s" % (dburl, e.message))
                continue


    def backup_all(self):
        for proc, lnet, res in self._pending_services.values():
            proc_info = "PID:%s name:%s bind:%s port:%s tool:%s" % (
            proc.pid, res['type'], res['bind'], res['port'], res['tool'])
            try:
                logger.log_info(proc_info)
                self._backup_one(**res)
            except Exception as e:
                logger.log_error("%s:%s" % (proc_info, e.message))

    @logit('Search database serives >> %s')
    def get_pending_services(self, select_engine=None, select_port=None):
        conditions = list(set(self._support_db).intersection([select_engine])) if select_engine else self._support_db
        choiced_proc = [proc for proc in psutil.process_iter() if proc.name() in conditions]
        for proc in choiced_proc:
            try:
                proc_name = proc.name()
                listens = [i for i in proc.connections() if i.status == 'LISTEN']
                if proc_name == 'mongod' and len(listens) == 2:
                    lnet = listens[0] if listens[0].laddr[1] < listens[1].laddr[1] else listens[1]
                elif proc_name == 'tnslsnr' and len(listens) > 1:
                    lnet = [i for i in listens if i.laddr[0].startswith('10.')][0]
                else:
                    lnet = listens[0]
                if lnet:
                    bind_addr = '127.0.0.1' if lnet.laddr[0] in ('0.0.0.0', '::') else lnet.laddr[0]
                    bind_port = lnet.laddr[1]
                    if select_port is not None and bind_port != select_port: continue
                    query_count_sql = "select count(*) from t_database where ip='%s' and port=%s" % (
                    self.main_ip, bind_port)
                    count_res = self.mq_query.produce(ack=True, query=query_count_sql)
                    if (not count_res) or count_res['count'] != 1 or count_res['result'] is None or count_res['result'][
                        0] < 1:
                        logger.log_error(
                            "No enougth record in db for %s://%s:%s" % (proc_name, self.main_ip, bind_port))
                        continue
                    res = self.get_info_from_service(proc, bind_addr, bind_port)
                    if res:
                        self._pending_services[proc.pid] = [proc, lnet, res]
                        if select_port and bind_port == select_port:
                            break
            except Exception as e:
                logger.log_error("PID:%s(%):%s" % (proc.pid, proc_name, e.message))
                continue


    @classmethod
    def get_info_from_service(cls, proc, bind_addr, bind_port):
        pname = proc.name()
        method_name = "get_info_from_%s" % pname
        if hasattr(cls, method_name):
            res = getattr(cls, method_name)(proc, bind_addr, bind_port)
            return res
        else:
            return None

    @classmethod
    @logit("Remove expired backup files >>%s")
    def remove_expired_files(cls, store_path, days):
        try:
            logger.log_info("Search expired %s days files from %s" % (days, store_path))
            expire_time = 86400 * days
            now_time = time.time()
            #expire files
            for root, dirs, files in os.walk(store_path):
                for wfile in files:
                    fpath = os.path.join(root, wfile)
                    if (now_time - os.path.getctime(fpath)) >= expire_time:
                        os.remove(fpath)
                        logger.log_info("Remove file:%s" % fpath)
            for root, dirs, files in os.walk(store_path):
                if dirs == [] and files == []:
                    os.rmdir(root)
                    logger.log_info("Remove empty dir:%s" % root)
            logger.log_info("Clean finished:%s" % store_path)
        except Exception as e:
            logger.log_error("Delete expired file:%s" % e.message)


class RabbitMQ(object):
    def __init__(self, conn_params, exchange='', routing_key='', durable=False):
        self._conn_params = conn_params
        self._channels = {}
        self._exchange = exchange
        self._routing_key = routing_key
        self._durable = durable
        self._fn_do_something = None
        self._responses = {}
        #

    def _get_connect(self, corr_id):
        try:
            logger.log_info("Connect to RabbitMQ:%s/%s" % (self._conn_params['host'], self._conn_params['vhost']))
            conn = pika.BlockingConnection(pika.ConnectionParameters(host=self._conn_params['host'],
                                                                     virtual_host=self._conn_params['vhost'],
                                                                     credentials=pika.PlainCredentials(
                                                                         self._conn_params['user'],
                                                                         self._conn_params['password']),
                                                                     socket_timeout=20,
                                                                     heartbeat_interval=700
            ))
            self._channels[corr_id].append(conn)
            return conn
        except (pika.exceptions.AMQPConnectionError, pika.exceptions.AuthenticationError, Exception) as e:
            logger.log_error("%s/%s Failed: %s" % (self._conn_params['host'], self._conn_params['vhost'], e.message))
            logger.log_error(traceback.format_exc())
            exit(2)

    def _get_channel(self):
        try:
            corr_id = str(uuid.uuid4())
            self._channels[corr_id] = []
            conn = self._get_connect(corr_id)
            channel = conn.channel()
            self._channels[corr_id].append(channel)
            self._channels[corr_id].reverse()
            logger.log_info("Create a channel: %s" % corr_id)
            logger.log_info("Declare exchange: %s" % self._exchange)
            channel.exchange_declare(exchange=self._exchange, type='direct')
            res = channel.queue_declare(exclusive=True, durable=self._durable or False)
            queue_replay = res.method.queue
            logger.log_info("Declare replay queue: %s" % queue_replay)
            channel.basic_consume(self._do_response,
                                  no_ack=True,
                                  queue=queue_replay)
            return (conn, channel, corr_id, queue_replay)
        except (pika.exceptions.ChannelError, pika.exceptions.ChannelClosed, pika.exceptions.AMQPChannelError,
                pika.exceptions.AMQPConnectionError, Exception) as e:
            self.close_channel(corr_id)
            logger.log_error("%s/%s Failed: %s" % (self._conn_params['host'], self._conn_params['vhost'], e.message))
            logger.log_error(traceback.format_exc())
            exit(2)

    def close_channel(self, corr_id):
        if self._channels.has_key(corr_id):
            for item in self._channels[corr_id]:
                if item: item.close()
            self._channels.pop(corr_id)


    @logit('Send message to rabbit mq >> %s')
    def produce(self, ack=False, **kwargs):
        '''
        # 执行索引由调用方，创建使用dict方式传如，确保唯一
        # 返回结果： [索引]:[操作, 结果, ....]
        '''
        logger.log_info(str(kwargs))
        message = CallbackMQ.dump_json(**kwargs)
        conn, channel, corr_id, queue_replay = self._get_channel()
        if ack:
            _pubblish_props = {}
            _pubblish_props['reply_to'] = queue_replay
            if self._durable: _pubblish_props['delivery_mode'] = 2
            #corr_id=str(uuid.uuid4())
            logger.log_info("response id:%s" % corr_id)
            self._responses[corr_id] = None
            _pubblish_props['correlation_id'] = corr_id
        channel.basic_publish(exchange=self._exchange
                              , routing_key=self._routing_key
                              , body=message
                              , properties=pika.BasicProperties(**_pubblish_props) if ack else None
        )
        if ack:
            try:
                while self._responses[corr_id] is None:
                    conn.process_data_events()
                logger.log_info("[%s]=%s" % (corr_id, self._responses[corr_id]))
                if self._responses[corr_id] is None:
                    return None
                else:
                    return CallbackMQ.load_json(self._responses[corr_id])
            except Exception as e:
                logger.log_error("receiving returning value:%s" % e.message)
                logger.log_error(traceback.format_exc())
                return None
            finally:
                self.close_channel(corr_id)
        else:
            self.close_channel(corr_id)


    @logit('Accept message from rabbit mq >> %s')
    def consumer(self, ack=False, fn_respond=None, **kwargs):
        if fn_respond:
            self._fn_do_something = fn_respond
            self._fn_kwargs = kwargs
        conn, ch, corr_id, queue_replay = self._get_channel()
        #随机生成队列
        res = ch.queue_declare(exclusive=True, durable=self._durable or False)
        queue_request = res.method.queue
        logger.log_info("consumer push queue:%s" % res.method.queue)
        ch.queue_bind(exchange=self._exchange
                      , queue=queue_request
                      , routing_key=self._routing_key)
        # fair dispatch
        ch.basic_qos(prefetch_count=1)
        ch.basic_consume(self._do_request, queue=queue_request, no_ack=not ack)
        try:
            ch.start_consuming()
        except KeyboardInterrupt:
            ch.stop_consuming()
        finally:
            self.close_channel(corr_id)

    def _do_response(self, ch, method, props, body):
        '''call by produce replay'''
        if props.correlation_id:
            self._responses[props.correlation_id] = body

    def _do_request(self, ch, method, props, body):
        '''call by  consumer request queue'''
        if self._fn_do_something:
            response = str(self._fn_do_something(body, **self._fn_kwargs))

        if props.correlation_id and props.reply_to:
            logger.log_info("[%s]=%s -> %s " % (props.correlation_id, response, props.reply_to))
            ch.basic_publish(exchange=''
                             , routing_key=props.reply_to
                             , properties=pika.BasicProperties(correlation_id= \
                                                                   props.correlation_id)
                             , body=response
            )
            ch.basic_ack(delivery_tag=method.delivery_tag)
        else:
            logger.log_info("[%s]=%s" % (props.correlation_id, response))


########################################################################

class ComplexEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, datetime):
            return obj.strftime('%Y-%m-%d %H:%M:%S')
        elif isinstance(obj, date):
            return obj.strftime('%Y-%m-%d')
        else:
            return json.JSONEncoder.default(self, obj)


class CallbackMQ(object):
    @classmethod
    def clean_exists_process(cls, proc_name):
        cur_pid = os.getpid()
        logger.log_info("Current Thread Pid: %s" % cur_pid)
        logger.log_info("Searching same name process for %s" % proc_name)
        for proc in psutil.process_iter():
            pinfo = proc.as_dict(['pid', 'name', 'ppid'])
            if pinfo['name'] == proc_name and pinfo['pid'] != cur_pid:
                logger.log_info("Kill old process:%s" % proc)
                proc.kill()
                if pinfo['ppid']:
                    pproc = proc.parent()
                    if string.find(' '.join(pproc.cmdline()), proc_name) != -1:
                        logger.log_info("Kill old parent processs:%s" % pproc)
                        pproc.kill()


    @classmethod
    def get_ip_list(cls):
        addrs = []
        for iface in netifaces.interfaces():
            if iface.startswith('lo'): continue
            try:
                iaddrs = netifaces.ifaddresses(iface)[netifaces.AF_INET]
            except:
                continue
            for addr in iaddrs:
                addrs.append(addr['addr'])
        return list(set(addrs))

    @classmethod
    def exchange_engine_type(cls, stype):
        typemap = {'mysqld': 'mysql'
            , 'mongod': 'mongodb'
            , 'tnslsnr': 'oracle'
        }
        procmap = {'mysql': 'mysqld'
            , 'mongodb': 'mongod'
            , 'oracle': 'tnslsnr'}
        if typemap.has_key(stype):
            return typemap[stype]
        elif procmap.has_key(stype):
            return procmap[stype]
        else:
            return stype

    @classmethod
    @logit("Remove expired backup files >>%s")
    def remove_expired_files(cls, store_path, expired_date):
        logger.log_info("Search expired %s files from %s" % (expired_date, store_path))
        if not os.path.exists(store_path): return None
        #expire files
        for root, dirs, files in os.walk(store_path):
            for wfile in files:
                fpath = os.path.join(root, wfile)
                if os.path.getctime(fpath) <= time.mktime(
                        time.strptime("%s 00:00:00" % expired_date, '%Y-%m-%d %H:%M:%S')):
                    logger.log_info("Remove file:%s" % fpath)
                    os.remove(fpath)
        for root, dirs, files in os.walk(store_path):
            if dirs == [] and files == []:
                os.rmdir(root)
                logger.log_info("Remove empty dir:%s" % root)
        logger.log_info("Clean finished:%s" % store_path)

    @classmethod
    def load_json(cls, body):
        try:
            return json.loads(body)
        except Exception as e:
            logger.log_error("Load json failed:%s" % e.message)
            logger.log_error("The json: %s" % body)
            return None

    @classmethod
    def dump_json(cls, **kwargs):
        return json.dumps(kwargs, ensure_ascii=False, cls=ComplexEncoder, sort_keys=True, indent=4)

    @classmethod
    def getdirsize(cls, source_dir):
        size = 0L
        for root, dirs, files in os.walk(source_dir):
            size += sum([os.path.getsize(os.path.join(root, name)) for name in files])
        return size

    @classmethod
    @logit('run sql from d_platform server >> %s')
    def deal_sql(cls, body, **kwargs):
        import MySQLdb

        sql = cls.load_json(body)
        if not sql.has_key('query'): raise Exception('the json miss some query information:\n%s' % body)
        dbconn = MySQLdb.connect(**LinkConfig.mysqlConn)
        cur = dbconn.cursor()
        logger.log_info(sql['query'])
        count = cur.execute(sql['query'], sql['params'] if sql.has_key('params') else None)
        sql['count'] = count
        sql_type = sql['query'].strip()[0:6]
        sql_type = sql_type.upper()
        if sql_type == 'SELECT':
            sql['result'] = cur.fetchall()
        elif sql_type == 'UPDATE':
            sql['result'] = None
        elif sql_type == 'INSERT':
            sql['result'] = dbconn.insert_id()
        elif sql_type == 'DELETE':
            sql['result'] = None
        else:
            raise Exception("cannot deal the sql:%s" % sql_type)
        sql['type'] = sql_type
        dbconn.commit()
        if vars().has_key('cur'): cur.close()
        if vars().has_key('dbconn'): dbconn.close()
        jsql = cls.dump_json(**sql)
        logger.log_info("query result:%s" % jsql)
        return jsql


    @classmethod
    def deal_log(cls, body, **kwargs):
        pass

    @classmethod
    @logit('Rsync backup files from db server >> %s')
    def deal_rsync(cls, body, **kwargs):
        import shutil
        # receive info from DataBackup._backup_one
        store_addr = kwargs['clientip'] if kwargs.has_key('clientip') else ''
        store_path = kwargs['store_path'] if kwargs.has_key('store_path') and kwargs[
                                                                                  'store_path'] is not None else '/dbbackup/tmp/'
        info = cls.load_json(body)
        info_key = ['dbid', 'server_ip', 'source_path', 'file_name', 'file_size', 'file_md5', 'dbtype', 'expired_files',
                    'expired_role']
        dbid, server_ip, source_path, file_name, file_size, file_md5, dbtype, expired_files, expired_role = \
            [info[i] if info.has_key(i) else None for i in info_key]
        #rsync -e "ssh -c arcfour" -avzL --progress --delete --force root@${HOST_IP}:${REMOTE_DIR}/ ${BACKUP_DIR}${HOST_IP}/`date +"%A"`/;
        source_file = os.path.join(source_path, file_name)
        server_path = os.path.join(store_path
                                   , cls.exchange_engine_type(dbtype)
                                   , server_ip)
        store_path = os.path.join(server_path
                                  , date.today().strftime('%A') if expired_role == '7' else date.today().strftime(
                '%d/%Y%m%d' if date.today().day == 1 else '%A/%Y%m%d')
        )
        if not os.path.exists(store_path):
            os.makedirs(store_path)
        logger.log_info('started rsync from %s:%s to %s' % (server_ip, source_file, store_path))
        start_time = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(time.time()))
        rsync = ['rsync'
            , "-e 'ssh -c arcfour' -avzL --progress --delete --force"
            , '--bwlimit=30000' if (file_size > 15 * 1024 * 1024 * 1024) else ''
            , 'root@${server_ip}:${source_file}'
            , '${store_path}']
        rsync_cmd = string.Template(''' '''.join(rsync)).substitute(server_ip=server_ip
                                                                    , source_file=source_file
                                                                    , store_path=store_path)
        status = False
        res = DataBackup.run_sys_cmd(rsync_cmd)
        stop_time = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(time.time()))
        if res.succeed:
            logger.log_info("rsync finished from %s:%s\n%s" % (server_ip, source_file, '\n'.join(res.result)))
            status = True
            for do_order in range(len(expired_files)):
                #id,did,rsync_path,file_size,rsync_expired
                sid, did, rsync_path, file_size, rsync_expired, bak_start = expired_files[do_order]
                del_addr, del_path = string.split(rsync_path, ':')
                try:
                    logger.log_info("remove expired file at %s:%s" % (rsync_expired, rsync_path))
                    if del_addr != store_addr: raise Exception(
                        "The file is not exists in %s:%s" % (store_addr, rsync_path))
                    if not os.path.exists(del_path): raise Exception("not exists")
                    if os.path.isdir(del_path):
                        shutil.rmtree(del_path)
                    else:
                        os.remove(del_path)
                    expired_files[do_order].append(1)
                except Exception as e:
                    logger.log_error("error when delete file:%s" % e.message)
                    logger.log_error(traceback.format_exc())
                    expired_files[do_order].append(2)
        else:
            logger.log_error("rsync failed from %s:%s :%s" % (server_ip, source_file, ' '.join(res.error)))
            status = False

        return CallbackMQ.dump_json(dbid=dbid
                                    , status=status
                                    , start_time=start_time
                                    , stop_time=stop_time
                                    , store_path="%s:%s" % (store_addr, os.path.join(store_path, file_name))
                                    , operation='rsync'
                                    , expired_result=expired_files)


def cli(args):
    import argparse

    parser = argparse.ArgumentParser(description='databases backup tools', version='0.5', add_help=True)
    parser.add_argument('--debug', action='store_true', default=False, help='enable to pint running log in stdout')
    subparse = parser.add_subparsers(help='commands', dest='action')
    #backup tool commandline
    backup_parse = subparse.add_parser('backup', help='backup database service data tool')
    backup_parse.add_argument('--engine', nargs='?', action='store', choices=['mysql', 'mongodb', 'oracle'],
                              help='engine name')
    backup_parse.add_argument('--port', nargs='?', action='store', type=int, help='listen port')
    backup_parse.add_argument('--store_path', action='store', help='path for stored backup files')

    #service of rabbit mq to record backup logs
    logs_parse = subparse.add_parser('log_center', help='deal backup logs through rabbit mq')
    logs_parse.add_argument('--store_path', action='store', help='path for stored backup logs')

    #service of rabbit mq to rsync backup files from database servers to store server
    trans_parse = subparse.add_parser('rsync_center', help='rsync backup file')
    trans_parse.add_argument('--store_path', action='store', help='path for stored backup files')
    #service of rabbit mq to interactive data query processing with d_platform
    sql_parce = subparse.add_parser('query_center', help='query information from mysql')
    #
    return parser.parse_args(args)


if __name__ == '__main__':
    if sys.version.split('.')[:2] < ['2', '7']:
        print "This program need to running with python 2.7+"
        sys.exit(2)

    try:
        namespace = cli(sys.argv[1:])
        logger.add_rotatingfile(os.path.join(os.path.dirname(sys.argv[0]), namespace.action + '.log'))
        clientip = [i for i in CallbackMQ.get_ip_list() if i.startswith('10.')][0]
        logger.set_formatter_extra(clientip=clientip)
        if namespace.debug:
            logger.add_streamfile()
        if namespace.action == 'backup':
            if not namespace.store_path:
                raise Exception("Must provide the path to save backup files")
            CallbackMQ.clean_exists_process(os.path.basename(sys.argv[0]))
            logger.log_info("Start backup process")
            bak = DataBackup(namespace.store_path, use_mq=True)
            if bak:
                bak.get_pending_services(
                    select_engine=CallbackMQ.exchange_engine_type(namespace.engine) if namespace.engine else None
                    , select_port=namespace.port if namespace.port else None)
                bak.backup_all()

            logger.log_info("End backup process")
        elif namespace.action == 'log_center':
            logger.log_info('staring log center, deal backup logs')
            query_mq = RabbitMQ(conn_params=LinkConfig.rqmconn, exchange='backup', routing_key='log')
            logger.log_info('=>start consuming from rabbit mq.(stop Ctrl+C)')
            query_mq.consumer(ack=False, fn_respond=CallbackMQ.deal_log)
            logger.log_info('log center finished.')
        elif namespace.action == 'rsync_center':
            if not namespace.store_path:
                raise Exception("Must provide the path to save backup files")
            logger.log_info('staring rsync center, rsync backup file from db server')
            query_mq = RabbitMQ(conn_params=LinkConfig.rqmconn, exchange='backup', routing_key='rsync')
            logger.log_info('=>start consuming from rabbit mq.(stop Ctrl+C)')
            query_mq.consumer(ack=True, fn_respond=CallbackMQ.deal_rsync, clientip=clientip,
                              store_path=namespace.store_path)
            logger.log_info('rsync center finished.')
        elif namespace.action == 'query_center':
            logger.log_info('staring query center, used deal sql query')
            query_mq = RabbitMQ(conn_params=LinkConfig.rqmconn, exchange='backup', routing_key='mysql')
            logger.log_info('=>start consuming from rabbit mq.(stop Ctrl+C)')
            query_mq.consumer(ack=True, fn_respond=CallbackMQ.deal_sql)
            logger.log_info('query center finished.')
    except Exception as e:
        logger.log_error(traceback.print_exc())
        logger.log_error(e)
        
