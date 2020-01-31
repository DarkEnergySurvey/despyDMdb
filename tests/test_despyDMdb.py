#!/usr/bin/env python3


import unittest
import os
import stat
import time
import sys

from contextlib import contextmanager
from io import StringIO

import despydmdb.dbsemaphore as semaphore
import despydmdb.desdmdbi as dmdbi
import despydmdb.dmdb_defs as dmdbdefs
import despydb.desdbi as desdbi
from MockDBI import MockConnection


@contextmanager
def capture_output():
    new_out, new_err = StringIO(), StringIO()
    old_out, old_err = sys.stdout, sys.stderr
    try:
        sys.stdout, sys.stderr = new_out, new_err
        yield sys.stdout, sys.stderr
    finally:
        sys.stdout, sys.stderr = old_out, old_err

class TestSemaphore(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        print('SETUP')
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
        os.chmod(cls.sfile, (0xffff & ~(stat.S_IROTH | stat.S_IWOTH | stat.S_IRGRP | stat.S_IWGRP)))

    @classmethod
    def tearDownClass(cls):
        os.unlink(cls.sfile)
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
        myid = res[-1]['id']
        cur.execute("select count(*) from semlock where name='%s' and in_use!=0" % semname)
        self.assertEqual(cur.fetchall()[0][0], 1)
        del sem
        cur.execute("select count(*) from semlock where name='%s' and in_use!=0" % semname)
        self.assertEqual(cur.fetchall()[0][0], 0)
        res = dbh.query_simple('SEMINFO', ['ID', 'REQUEST_TIME', 'GRANT_TIME', 'RELEASE_TIME'], {'ID': myid})
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

    def test_str(self):
        with capture_output() as (out, err):
            sem = semaphore.DBSemaphore('mock-in', 123456, self.sfile, 'db-test')
            print(sem)
            output = out.getvalue().strip()
            self.assertTrue('mock-in' in output)
            self.assertTrue('slot' in output)


class TestDesdmdbi(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
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
        os.chmod(cls.sfile, (0xffff & ~(stat.S_IROTH | stat.S_IWOTH | stat.S_IRGRP | stat.S_IWGRP)))

    @classmethod
    def tearDownClass(cls):
        os.unlink(cls.sfile)
        MockConnection.destroy()

    def test_init(self):
        dbh = dmdbi.DesDmDbi(self.sfile, 'db-test')

    def test_get_metadata(self):
        dbh = dmdbi.DesDmDbi(self.sfile, 'db-test')
        data = dbh.get_metadata()
        self.assertTrue('ccdnum' in data)
        self.assertIsNone(data['ccdnum']['ccdnum']['data_type'])

    def test_get_all_filetype_metadata(self):
        dbh = dmdbi.DesDmDbi(self.sfile, 'db-test')
        data = dbh.get_all_filetype_metadata()
        self.assertTrue('cat_finalcut' in data)
        self.assertTrue('hdus' in data['cat_finalcut'])
        self.assertTrue('primary' in data['cat_finalcut']['hdus'])

    def test_get_site_info(self):
        dbh = dmdbi.DesDmDbi(self.sfile, 'db-test')
        data = dbh.get_site_info()
        self.assertTrue('descampuscluster' in data)
        self.assertTrue('gridtype' in data['descampuscluster'])

    def test_archive_info(self):
        dbh = dmdbi.DesDmDbi(self.sfile, 'db-test')
        data = dbh.get_archive_info()
        self.assertTrue('decarchive' in data)
        self.assertTrue('fileutils' in data['decarchive'])

    def test_get_archive_transfer_info(self):
        dbh = dmdbi.DesDmDbi(self.sfile, 'db-test')
        data = dbh.get_archive_transfer_info()
        self.assertEqual(len(data), 0)

    def test_get_job_file_mvmt_info(self):
        dbh = dmdbi.DesDmDbi(self.sfile, 'db-test')
        data = dbh.get_job_file_mvmt_info()
        self.assertTrue('descampuscluster' in data)
        self.assertTrue('desar2home' in data['descampuscluster'])
        self.assertTrue('no_archive' in data['descampuscluster']['desar2home'])
        self.assertTrue('mvmtclass' in data['descampuscluster']['desar2home']['no_archive'])

    def test_load_artifact_gtt(self):
        files = [{dmdbdefs.DB_COL_FILENAME: 'test.fits',
                  dmdbdefs.DB_COL_COMPRESSION: '.fz',
                  dmdbdefs.DB_COL_FILESIZE: 128,
                  dmdbdefs.DB_COL_MD5SUM: 'ab66249844ae'},
                 {'filename': 'test2.fits',
                  'compression': None,
                  dmdbdefs.DB_COL_FILESIZE.lower(): 112233,
                  dmdbdefs.DB_COL_MD5SUM.lower(): 'ab66249844'},
                 {'fullname': 'test3.fits.fz'},
                 {dmdbdefs.DB_COL_FILENAME: 'test4.fts.fz'}]
        dbh = dmdbi.DesDmDbi(self.sfile, 'db-test')
        tab = dbh.load_artifact_gtt(files)
        curs = dbh.cursor()
        curs.execute('select count(*) from %s' % tab)
        self.assertEqual(curs.fetchall()[0][0], len(files))
        curs.execute("select " + (',').join([dmdbdefs.DB_COL_FILENAME, dmdbdefs.DB_COL_COMPRESSION,
                                             dmdbdefs.DB_COL_MD5SUM, dmdbdefs.DB_COL_FILESIZE]) +
                     " from " + tab + " where " + dmdbdefs.DB_COL_FILENAME + "='test.fits'")
        res = curs.fetchall()
        self.assertEqual(res[0][-1], 128)
        dbh.commit()
        curs.execute('select count(*) from %s' % tab)
        self.assertEqual(curs.fetchall()[0][0], 0)

        files = [{dmdbdefs.DB_COL_FILENAME: 'test.fits',
                  dmdbdefs.DB_COL_COMPRESSION: '.fz',
                  dmdbdefs.DB_COL_FILESIZE: 128,
                  dmdbdefs.DB_COL_MD5SUM: 'ab66249844ae'},
                 {'filenam': 'test2.fits',
                  'compression': None,
                  dmdbdefs.DB_COL_FILESIZE.lower(): 112233,
                  dmdbdefs.DB_COL_MD5SUM.lower(): 'ab66249844'},
                 {'fullname': 'test3.fits.fz'},
                 {dmdbdefs.DB_COL_FILENAME: 'test4.fts.fz'}]
        self.assertRaises(ValueError, dbh.load_artifact_gtt, files)

    def test_load_filename_gtt(self):
        files = ['test1.fits.fz',
                 {dmdbdefs.DB_COL_FILENAME: 'test.fits',
                  dmdbdefs.DB_COL_COMPRESSION: '.fz'},
                 {'filename': 'test2.fits',
                  'compression': None},
                 {dmdbdefs.DB_COL_FILENAME: 'test4.fts.fz'},
                 {'filename': 'test3.fits.fz'}]
        dbh = dmdbi.DesDmDbi(self.sfile, 'db-test')
        tab = dbh.load_filename_gtt(files)
        curs = dbh.cursor()
        curs.execute('select count(*) from %s' % tab)
        self.assertEqual(curs.fetchall()[0][0], len(files))
        curs.execute("select " + (',').join([dmdbdefs.DB_COL_FILENAME, dmdbdefs.DB_COL_COMPRESSION]) +
                     " from " + tab + " where " + dmdbdefs.DB_COL_FILENAME + "='test4.fts'")
        self.assertEqual('.fz', curs.fetchall()[0][-1])

        dbh.commit()
        curs.execute('select count(*) from %s' % tab)
        self.assertEqual(curs.fetchall()[0][0], 0)

        self.assertRaises(ValueError, dbh.load_filename_gtt, [12345])

    def test_load_id_gtt(self):
        ids = [1, 5, 10, 15, 20, 25]
        dbh = dmdbi.DesDmDbi(self.sfile, 'db-test')
        tab = dbh.load_id_gtt(ids)
        curs = dbh.cursor()
        curs.execute('select count(*) from %s' % tab)
        self.assertEqual(curs.fetchall()[0][0], len(ids))
        dbh.commit()
        curs.execute('select count(*) from %s' % tab)
        self.assertEqual(curs.fetchall()[0][0], 0)

        ids = [1, 2, 3.5, 6]
        self.assertRaises(ValueError, dbh.load_id_gtt, ids)

    def test_empty_gtt(self):
        dbh = dmdbi.DesDmDbi(self.sfile, 'db-test')
        self.assertRaises(ValueError, dbh.empty_gtt, 'gt_tab')

    def test_task_interaction(self):
        dbh = dmdbi.DesDmDbi(self.sfile, 'db-test')
        root_id = dbh.create_task('root_task', None, i_am_root=True, do_begin=True)
        parent_id = dbh.create_task('parent_task', None, parent_task_id=root_id,
                                    root_task_id=root_id, label='wrapper', do_begin=True)
        child1 = dbh.create_task('exec1', None, parent_id, root_id, label='child1')
        dbh.begin_task(child1, True)
        dbh.end_task(child1, 0, True)
        child2 = dbh.create_task('exec1', None, parent_id, root_id, label='child2', do_begin=False, do_commit=True)
        curs = dbh.cursor()
        curs.execute("select start_time from task where id=%i" % child2)
        res = curs.fetchall()
        self.assertEqual(len(res), 1)
        self.assertIsNone(res[0][0])
        dbh.end_task(child2, 1)
        curs.execute("select name, end_time, status from task where root_task_id=%i" % root_id)
        res = curs.fetchall()
        self.assertEqual(len(res), 4)

        curs.execute("select status from task where id=%i" % child2)
        res = curs.fetchall()
        self.assertEqual(len(res), 1)
        curs.execute("select status from task where id=%i" % child1)
        res = curs.fetchall()
        self.assertEqual(len(res), 1)
        self.assertEqual(res[0][0], 0)
        dbh.rollback()
        curs.execute("select status from task where id=%i" % child2)
        res = curs.fetchall()
        self.assertEqual(len(res), 1)
        self.assertIsNone(res[0][0])

    def test_get_datafile_metadata(self):
        dbh = dmdbi.DesDmDbi(self.sfile, 'db-test')
        data = dbh.get_datafile_metadata('cat_finalcut')
        self.assertEqual(len(data), 2)
        self.assertEqual(data[0], 'SE_OBJECT')
        self.assertTrue('PRIMARY' in data[1].keys())

        self.assertRaises(ValueError, dbh.get_datafile_metadata, 'cat_something')

if __name__ == '__main__':
    unittest.main()
