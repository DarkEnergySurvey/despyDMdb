#!/usr/bin/env python2

import unittest
import os
import stat
import time
import despydmdb.dbsemaphore as semaphore
import despydmdb.desdmdbi as dmdbi
import despydb.desdbi as desdbi
from MockDBI import MockConnection

class TestSemaphore(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        print 'SETUP'
        cls.sfile = 'services.ini'
        open(cls.sfile, 'w').write("""

[db-maximal]
PASSWD  =   maximal_passwd
name    =   maximal_name_1    ; if repeated last name wins
user    =   maximal_name      ; if repeated key, last one wins
Sid     =   maximal_sid       ;comment glued onto value not allowed
type    =   POSTgres
server  =   maximal_server

[db-minimal]
USER    =   Minimal_user
PASSWD  =   Minimal_passwd
name    =   Minimal_name
sid     =   Minimal_sid
server  =   Minimal_server
type    =   oracle

[db-test]
USER    =   Minimal_user
PASSWD  =   Minimal_passwd
name    =   Minimal_name
sid     =   Minimal_sid
server  =   Minimal_server
type    =   test
port    =   0
""")
        os.chmod(cls.sfile, (0xffff & ~(stat.S_IROTH | stat.S_IWOTH | stat.S_IRGRP | stat.S_IWGRP )))

    @classmethod
    def tearDownClass(cls):
        os.unlink(cls.sfile)
        print 'DONE'
        MockConnection.destroy()

    def test_basic_operation(self):
        dbh = desdbi.DesDbi(self.sfile, 'db-test')
        cur = dbh.cursor()
        semname = 'mock-in'
        cur.execute("select count(*) from SEMINFO where name='%s'" % semname)
        count = cur.fetchall()[0][0]
        #self.assertEqual(count, 3)
        sem = semaphore.DBSemaphore('mock-in', 123456, self.sfile, 'db-test')
        res = dbh.query_simple('SEMINFO', ['ID', 'REQUEST_TIME', 'GRANT_TIME', 'RELEASE_TIME'], orderby='REQUEST_TIME')
        self.assertEqual(len(res) - count, 1)
        self.assertIsNotNone(res[-1]['request_time'])
        self.assertIsNotNone(res[-1]['grant_time'])
        self.assertIsNone(res[-1]['release_time'])
        id = res[-1]['id']
        cur.execute("select count(*) from semlock where name='%s' and in_use!=0" % semname)
        self.assertEqual(cur.fetchall()[0][0], 1)
        del sem
        cur.execute("select count(*) from semlock where name='%s' and in_use!=0" % semname)
        self.assertEqual(cur.fetchall()[0][0], 0)
        res = dbh.query_simple('SEMINFO', ['ID', 'REQUEST_TIME', 'GRANT_TIME', 'RELEASE_TIME'], {'ID': id})
        self.assertIsNotNone(res[-1]['request_time'])
        self.assertIsNotNone(res[-1]['grant_time'])
        self.assertIsNotNone(res[-1]['release_time'])


    def test_not_available(self):
        now = time.time()
        sem = semaphore.DBSemaphore('mock-in', 123456, self.sfile, 'db-test')
        self.assertTrue(time.time() - now < semaphore.TRYINTERVAL)
        now = time.time()
        sem1 = semaphore.DBSemaphore('mock-in', 123456, self.sfile, 'db-test')
        self.assertTrue(time.time() - now < semaphore.TRYINTERVAL)
        now = time.time()
        sem2 = semaphore.DBSemaphore('mock-in', 123456, self.sfile, 'db-test')
        self.assertTrue(time.time() - now < semaphore.TRYINTERVAL)
        now = time.time()
        MockConnection.mock_fail = True
        semfail = semaphore.DBSemaphore('mock-in', 123456, self.sfile, 'db-test')
        self.assertTrue(time.time() - now > semaphore.MAXTRIES * semaphore.TRYINTERVAL)
        self.assertTrue(time.time() - now < (semaphore.MAXTRIES + 1) * semaphore.TRYINTERVAL)
        MockConnection.mock_fail = False
        del sem1
        now = time.time()
        sem3 = semaphore.DBSemaphore('mock-in', 123456, self.sfile, 'db-test')
        self.assertTrue(time.time() - now < semaphore.TRYINTERVAL)

    def test_no_such_semaphore(self):
        self.assertRaises(ValueError, semaphore.DBSemaphore, 'mock-bad', 123456, self.sfile, 'db-test')

if __name__ == '__main__':
    unittest.main()
