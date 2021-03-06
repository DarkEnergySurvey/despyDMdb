# $Id: dbsemaphore.py 48543 2019-05-20 19:12:20Z friedel $
# $Rev:: 48543                            $:  # Revision of last commit.
# $LastChangedBy:: friedel                $:  # Author of last commit.
# $LastChangedDate:: 2019-05-20 14:12:20 #$:  # Date of last commit.

"""
    Using the database, provide semaphore capability.
"""
#pylint: disable=c-extension-no-member

__version__ = "$Rev: 48543 $"

import time

import despymisc.miscutils as miscutils

MAXTRIES = 5
TRYINTERVAL = 10


class DBSemaphore:
    """ Using the database, provide semaphore capability.
        Currently requires Oracle or the test infrastructure

        Parameters
        ----------
        semname : str
            The name of the semaphore to use

        task_id : int
            The id number of the tast requesting the semaphore lock

        desfile : str, optional
            The name of the services file to use. Default is None.

        section : str, optional
            The name of the section in the services file to use. Default is None.

        connection : database handle, optional
            The database handle to use for the operation. Default is None, it will
            initiate it's own handle.

        threaded : bool, False
            Whether to make the created handle thread safe. Default is False.
    """

    def __init__(self, semname, task_id, desfile=None, section=None, connection=None, threaded=False):
        """
        Create the DB connection and do the semaphore wait.
        """
        self.desfile = desfile
        self.section = section
        self.semname = semname
        self.task_id = task_id
        self.slot = None

        miscutils.fwdebug(3, f"SEMAPHORE_DEBUG", "SEM - INFO - semname {self.semname}")
        miscutils.fwdebug(3, "SEMAPHORE_DEBUG", "SEM - BEG - db-specific imports")
        import despydmdb.desdmdbi as desdmdbi
        import cx_Oracle
        miscutils.fwdebug(3, "SEMAPHORE_DEBUG", "SEM - END - db-specific imports")

        miscutils.fwdebug(3, "SEMAPHORE_DEBUG", "SEM - BEG - db connection")
        self.dbh = desdmdbi.DesDmDbi(desfile, section, threaded=threaded)
        miscutils.fwdebug(3, "SEMAPHORE_DEBUG", "SEM - END - db connection")

        curs = self.dbh.cursor()

        sql = f"select count(*) from semlock where name={self.dbh.get_named_bind_string('name')}"
        curs.execute(sql, {'name': semname})
        num_slots = curs.fetchone()[0]
        if num_slots == 0:
            miscutils.fwdebug(0, "SEMAPHORE_DEBUG", f"SEM - ERROR - no locks with name {semname}")
            raise ValueError(f'No locks with name {semname}')

        self.id = self.dbh.get_seq_next_value('seminfo_seq')
        self.dbh.basic_insert_row('seminfo', {'id': self.id,
                                              'name': self.semname,
                                              'request_time': self.dbh.get_current_timestamp_str(),
                                              'task_id': task_id,
                                              'num_slots': num_slots})
        self.dbh.commit()

        self.slot = curs.var(cx_Oracle.NUMBER)
        done = False
        trycnt = 1
        while not done and trycnt <= MAXTRIES:
            try:
                miscutils.fwdebug(3, "SEMAPHORE_DEBUG", "SEM - BEG - wait")
                curs.callproc("SEM_WAIT", [self.semname, self.slot])
                miscutils.fwdebug(3, "SEMAPHORE_DEBUG", "SEM - END - wait")
                miscutils.fwdebug(3, "SEMAPHORE_DEBUG", f"SEM - INFO - slot {self.slot}")
                done = True
                if not self.dbh.is_oracle():
                    self.dbh.commit() # test database must commit
            except Exception as e:
                miscutils.fwdebug(0, "SEMAPHORE_DEBUG", f"SEM - ERROR - {str(e)}")

                time.sleep(TRYINTERVAL)

                miscutils.fwdebug(3, "SEMAPHORE_DEBUG", "SEM - BEG - remake db connection")
                self.dbh = desdmdbi.DesDmDbi(desfile, section)
                miscutils.fwdebug(3, "SEMAPHORE_DEBUG", "SEM - END - remake db connection")

                curs = self.dbh.cursor()
                self.slot = curs.var(cx_Oracle.NUMBER)

                miscutils.fwdebug(3, "SEMAPHORE_DEBUG", "SEM - BEG - dequeue")
                curs.callproc("SEM_DEQUEUE", [self.semname, self.slot])
                miscutils.fwdebug(3, "SEMAPHORE_DEBUG", "SEM - END - dequeue")

                trycnt += 1

        if done:
            # need different connection to do the commit of the grant info as commit will release lock
            dbh2 = desdmdbi.DesDmDbi(desfile, section, connection)
            dbh2.basic_update_row('SEMINFO',
                                  {'grant_time': dbh2.get_current_timestamp_str(),
                                   'num_requests': trycnt,
                                   'slot': self.slot},
                                  {'id': self.id})
            dbh2.commit()

    def __del__(self):
        """
        Do the semaphore signal and close DB connection
        """
        if self.slot is not None and str(self.slot) != 'None':
            try:
                miscutils.fwdebug(3, "SEMAPHORE_DEBUG", "SEM - BEG - signal")
                curs = self.dbh.cursor()
                curs.callproc("SEM_SIGNAL", [self.semname, self.slot])
                miscutils.fwdebug(3, "SEMAPHORE_DEBUG", "SEM - END - signal")
                self.dbh.basic_update_row('SEMINFO',
                                          {'release_time': self.dbh.get_current_timestamp_str()},
                                          {'id': self.id})
                self.dbh.commit()
            except Exception as e:
                miscutils.fwdebug(0, "SEMAPHORE_DEBUG", "SEM - ERROR - " + str(e))

        self.slot = None
        self.dbh.close()

    def __str__(self):
        """
        x.__str__() <==> str(x)
        """
        return str({'name': self.semname, 'slot': self.slot})
