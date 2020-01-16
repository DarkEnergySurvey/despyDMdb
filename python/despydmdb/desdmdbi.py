# $Id: desdmdbi.py 48543 2019-05-20 19:12:20Z friedel $
# $Rev:: 48543                            $:  # Revision of last commit.
# $LastChangedBy:: friedel                $:  # Author of last commit.
# $LastChangedDate:: 2019-05-20 14:12:20 #$:  # Date of last commit.

""" Higher-level DB functions used across multiple svn projects of DESDM
    Modified more often than the lower-level despydb
"""

__version__ = "$Rev: 48543 $"

import socket
import collections

import despydb.desdbi as desdbi
import despydmdb.dmdb_defs as dmdbdefs
import despymisc.miscutils as miscutils

class DesDmDbi(desdbi.DesDbi):
    """ Build on base DES db class adding DB functions used across various DM projects

        Parameters
        ----------
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

    def __init__(self, desfile=None, section=None, connection=None, threaded=False):
        desdbi.DesDbi.__init__(self, desfile, section, retry=True, connection=connection, threaded=threaded)

    def get_metadata(self):
        """ Get and return the contents of the OPS_METADATA table as a dictionary

            Returns
            -------
            dict
                Dictionary containing the OPS_METADATA table, where the keys are the name of the
                header values, and the values are dictionaries with the column names as keys and
                the row contents as the values
        """
        sql = "select * from ops_metadata"
        curs = self.cursor()
        curs.execute(sql)
        desc = [d[0].lower() for d in curs.description]

        result = collections.OrderedDict()
        for line in curs:
            d = dict(zip(desc, line))
            headername = d['file_header_name'].lower()
            columnname = d['column_name'].lower()
            if headername not in result:
                result[headername] = collections.OrderedDict()
            if columnname not in result[headername]:
                result[headername][columnname] = d
            else:
                raise Exception(f"Found duplicate row in metadata({headername}, {columnname})")

        curs.close()
        return result


    def get_all_filetype_metadata(self):
        """ Gets a dictionary of dictionaries or string=value pairs representing
            data from the OPS_METADATA, OPS_FILETYPE, and OPS_FILETYPE_METADATA tables.
            This is intended to provide a complete set of filetype metadata required
            during a run.

            Returns
            -------
            dict
        """
        sql = """select f.filetype, f.metadata_table, f.filetype_mgmt,
                    nvl(fm.file_hdu, 'primary') file_hdu,
                    fm.status, fm.derived,
                    fm.file_header_name, m.column_name
                from OPS_METADATA m, OPS_FILETYPE f, OPS_FILETYPE_METADATA fm
                where m.file_header_name=fm.file_header_name
                    and f.filetype=fm.filetype
                    and fm.status != 'I'
                """
        curs = self.cursor()
        curs.execute(sql)
        desc = [d[0].lower() for d in curs.description]

        result = collections.OrderedDict()
        for row in curs:
            info = dict(zip(desc, row))
            ptr = result
            ftype = info['filetype'].lower()
            if ftype not in result:
                result[ftype] = collections.OrderedDict({'hdus': collections.OrderedDict()})
                if info['metadata_table'] is not None:
                    result[ftype]['metadata_table'] = info['metadata_table'].lower()
                if info['filetype_mgmt'] is not None:
                    result[ftype]['filetype_mgmt'] = info['filetype_mgmt']

            if info['file_hdu'].lower() not in result[ftype]['hdus']:
                result[ftype]['hdus'][info['file_hdu'].lower()] = collections.OrderedDict()

            ptr = result[ftype]['hdus'][info['file_hdu'].lower()]
            if info['status'].lower() not in ptr:
                ptr[info['status'].lower()] = collections.OrderedDict()

            ptr = ptr[info['status'].lower()]
            if info['derived'].lower() not in ptr:
                ptr[info['derived'].lower()] = collections.OrderedDict()

            ptr[info['derived'].lower()][info['file_header_name'].lower()] = info['column_name'].lower()

        curs.close()

        return result


    def get_site_info(self):
        """ Return contents of ops_site and ops_site_val tables as a dictionary

            Returns
            -------
            dict
        """
        # assumes foreign key constraints so cannot have site in ops_site_val that isn't in ops_site

        site_info = self.query_results_dict('select * from ops_site', 'name')

        sql = "select name,key,val from ops_site_val"
        curs = self.cursor()
        curs.execute(sql)
        for(name, key, val) in curs:
            site_info[name][key] = val
        return site_info


    def get_archive_info(self):
        """ Return contents of ops_archive and ops_archive_val tables as a dictionary

            Returns
            -------
            dict
        """
        # assumes foreign key constraints so cannot have archive in ops_archive_val that isn't in ops_archive

        archive_info = self.query_results_dict('select * from ops_archive', 'name')

        sql = "select name,key,val from ops_archive_val"
        curs = self.cursor()
        curs.execute(sql)
        for(name, key, val) in curs:
            archive_info[name][key] = val
        return archive_info


    def get_archive_transfer_info(self):
        """ Return contents of ops_archive_transfer and ops_archive_transfer_val tables as a dictionary

            Returns
            -------
            dict
        """

        archive_transfer = collections.OrderedDict()
        sql = "select src,dst,transfer from ops_archive_transfer"
        curs = self.cursor()
        curs.execute(sql)
        for row in curs:
            if row[0] not in archive_transfer:
                archive_transfer[row[0]] = collections.OrderedDict()
            archive_transfer[row[0]][row[1]] = collections.OrderedDict({'transfer':row[2]})

        sql = "select src,dst,key,val from ops_archive_transfer_val"
        curs = self.cursor()
        curs.execute(sql)
        for row in curs:
            if row[0] not in archive_transfer:
                miscutils.fwdebug(0, 'DESDBI_DEBUG', f"WARNING: found info in ops_archive_transfer_val for src archive {row[0]} which is not in ops_archive_transfer")
                archive_transfer[row[0]] = collections.OrderedDict()
            if row[1] not in archive_transfer[row[0]]:
                miscutils.fwdebug(0, 'DESDBI_DEBUG', f"WARNING: found info in ops_archive_transfer_val for dst archive {row[1]} which is not in ops_archive_transfer")
                archive_transfer[row[0]][row[1]] = collections.OrderedDict()
            archive_transfer[row[0]][row[1]][row[2]] = row[3]
        return archive_transfer


    def get_job_file_mvmt_info(self):
        """ Return contents of ops_job_file_mvmt and ops_job_file_mvmt_val tables as a dictionary

            Returns
            -------
            dict
        """
        # [site][home][target][key] = [val]  where req key is mvmtclass

        sql = "select site,home_archive,target_archive,mvmtclass from ops_job_file_mvmt"
        curs = self.cursor()
        curs.execute(sql)
        info = collections.OrderedDict()
        for(site, home, target, mvmt) in curs:
            if home is None:
                home = 'no_archive'

            if target is None:
                target = 'no_archive'

            if site not in info:
                info[site] = collections.OrderedDict()
            if home not in info[site]:
                info[site][home] = collections.OrderedDict()
            info[site][home][target] = collections.OrderedDict({'mvmtclass': mvmt})

        sql = "select site,home_archive,target_archive,key,val from ops_job_file_mvmt_val"
        curs = self.cursor()
        curs.execute(sql)
        for(site, home, target, key, val) in curs:
            if home is None:
                home = 'no_archive'

            if target is None:
                target = 'no_archive'

            if(site not in info or
               home not in info[site] or
               target not in info[site][home]):
                miscutils.fwdie(f"Error: found info in ops_job_file_mvmt_val({site}, {home}, {target}, {key}, {val}) which is not in ops_job_file_mvmt", 1)
            info[site][home][target][key] = val
        return info


    def load_artifact_gtt(self, filelist):
        """ insert file artifact information into global temp table

            Parameters
            ----------
            filelist : list
                List of dictionaries, one for each file, giving the file
                metadata to store.

            Returns
            -------
            str
                The name of the temp table
        """
        # filelist is list of file dictionaries
        # returns artifact GTT table name

        parsemask = miscutils.CU_PARSE_FILENAME | miscutils.CU_PARSE_EXTENSION

        # make sure table is empty before loading it
        self.empty_gtt(dmdbdefs.DB_GTT_ARTIFACT)

        colmap = [dmdbdefs.DB_COL_FILENAME, dmdbdefs.DB_COL_COMPRESSION,
                  dmdbdefs.DB_COL_MD5SUM, dmdbdefs.DB_COL_FILESIZE]
        rows = []
        for _file in filelist:
            miscutils.fwdebug(3, 'DESDBI_DEBUG', f"file = {_file}")
            fname = None
            comp = None
            md5sum = None
            filesize = None
            if dmdbdefs.DB_COL_FILENAME in _file or dmdbdefs.DB_COL_FILENAME.lower() in _file:
                if dmdbdefs.DB_COL_COMPRESSION in _file:
                    fname = _file[dmdbdefs.DB_COL_FILENAME]
                    comp = _file[dmdbdefs.DB_COL_COMPRESSION]
                elif dmdbdefs.DB_COL_COMPRESSION.lower() in _file:
                    fname = _file[dmdbdefs.DB_COL_FILENAME.lower()]
                    comp = _file[dmdbdefs.DB_COL_COMPRESSION.lower()]
                elif dmdbdefs.DB_COL_FILENAME in _file:
                    (fname, comp) = miscutils.parse_fullname(_file[dmdbdefs.DB_COL_FILENAME], parsemask)
                else:
                    (fname, comp) = miscutils.parse_fullname(_file[dmdbdefs.DB_COL_FILENAME.lower()], parsemask)
                miscutils.fwdebug(3, 'DESDBI_DEBUG', f"fname={fname}, comp={comp}")
            elif 'fullname' in _file:
                (fname, comp) = miscutils.parse_fullname(_file['fullname'], parsemask)
                miscutils.fwdebug(3, 'DESDBI_DEBUG', f"parse_fullname: fname={fname}, comp={comp}")
            else:
                miscutils.fwdebug(3, 'DESDBI_DEBUG', f"file={_file}")
                raise ValueError(f"Invalid entry filelist({_file})")

            if dmdbdefs.DB_COL_FILESIZE in _file:
                filesize = _file[dmdbdefs.DB_COL_FILESIZE]
            elif dmdbdefs.DB_COL_FILESIZE.lower() in _file:
                filesize = _file[dmdbdefs.DB_COL_FILESIZE.lower()]

            if dmdbdefs.DB_COL_MD5SUM in _file:
                md5sum = _file[dmdbdefs.DB_COL_MD5SUM]
            elif dmdbdefs.DB_COL_MD5SUM.lower() in _file:
                md5sum = _file[dmdbdefs.DB_COL_MD5SUM.lower()]

            miscutils.fwdebug(3, 'DESDBI_DEBUG', f"row: fname={fname}, comp={comp}, filesize={filesize}, md5sum={md5sum}")
            rows.append({dmdbdefs.DB_COL_FILENAME:fname, dmdbdefs.DB_COL_COMPRESSION:comp,
                         dmdbdefs.DB_COL_FILESIZE:filesize, dmdbdefs.DB_COL_MD5SUM:md5sum})

        self.insert_many(dmdbdefs.DB_GTT_ARTIFACT, colmap, rows)
        return dmdbdefs.DB_GTT_ARTIFACT


    def load_filename_gtt(self, filelist):
        """ insert filenames into filename global temp table

            Parameters
            ----------
            filelist : list
                List of strings of the file names, or of dictionaries describing the file names

            Returns
            -------
            str
                The temp table name
        """
        # returns filename GTT table name

        # make sure table is empty before loading it
        self.empty_gtt(dmdbdefs.DB_GTT_FILENAME)

        colmap = [dmdbdefs.DB_COL_FILENAME, dmdbdefs.DB_COL_COMPRESSION]
        rows = []
        for _file in filelist:
            fname = None
            comp = None
            if isinstance(_file, str):
                (fname, comp) = miscutils.parse_fullname(_file, miscutils.CU_PARSE_FILENAME | miscutils.CU_PARSE_EXTENSION)
            elif isinstance(_file, dict) and(dmdbdefs.DB_COL_FILENAME in _file or dmdbdefs.DB_COL_FILENAME.lower() in _file):
                if dmdbdefs.DB_COL_COMPRESSION in _file:
                    fname = _file[dmdbdefs.DB_COL_FILENAME]
                    comp = _file[dmdbdefs.DB_COL_COMPRESSION]
                elif dmdbdefs.DB_COL_COMPRESSION.lower() in _file:
                    fname = _file[dmdbdefs.DB_COL_FILENAME.lower()]
                    comp = _file[dmdbdefs.DB_COL_COMPRESSION.lower()]
                elif dmdbdefs.DB_COL_FILENAME in _file:
                    (fname, comp) = miscutils.parse_fullname(_file[dmdbdefs.DB_COL_FILENAME], miscutils.CU_PARSE_FILENAME | miscutils.CU_PARSE_EXTENSION)
                else:
                    (fname, comp) = miscutils.parse_fullname(_file[dmdbdefs.DB_COL_FILENAME.lower()], miscutils.CU_PARSE_FILENAME | miscutils.CU_PARSE_EXTENSION)
            else:
                raise ValueError(f"Invalid entry filelist({_file})")
            rows.append({dmdbdefs.DB_COL_FILENAME:fname, dmdbdefs.DB_COL_COMPRESSION:comp})
        self.insert_many(dmdbdefs.DB_GTT_FILENAME, colmap, rows)
        return dmdbdefs.DB_GTT_FILENAME

    def load_id_gtt(self, idlist):
        """ Insert a list of id's into a global temp table

            Parameters
            ----------
            idlist : list
                List of integers

            Returns
            -------
            str
                The name of the temp table
        """
        self.empty_gtt(dmdbdefs.DB_GTT_ID)
        colmap = [dmdbdefs.DB_COL_ID]
        rows = []
        for desfid in idlist:
            if isinstance(desfid, int):
                rows.append({dmdbdefs.DB_COL_ID: desfid})
            else:
                raise ValueError(f"invalid entry idlist({str(desfid)})")
        self.insert_many(dmdbdefs.DB_GTT_ID, colmap, rows)
        return dmdbdefs.DB_GTT_ID

    def empty_gtt(self, tablename):
        """ Clean out temp table for when one wants separate commit/rollback control

            Parameters
            ----------
            tablename : str
                The name of the global temp table to clear
        """
        # could be changed to generic empty table function, for now wanted safety check

        if 'gtt' not in tablename.lower():
            raise ValueError("Invalid table name for a global temp table(missing GTT)")

        sql = f"delete from {tablename}"
        curs = self.cursor()
        curs.execute(sql)
        curs.close()


    def create_task(self, name, info_table, parent_task_id=None, root_task_id=None,
                    i_am_root=False, label=None, do_begin=False, do_commit=False):
        """ Insert a row into the task table and return task id

            Parameters
            ----------
            name : str
                The name of the task

            info_table : str
                The name of the table associated with the task, if any

            parent_task_id : int, optional
                The task id of the parent (calling) task, default is None

            root_task_id : int, optional
                The task id of the primary task, the first task in the job,
                default is None

            i_am_root : bool, optional
                Specifies whether this task is the root task (True), default is False

            label : str, optional
                Any label for the task, default is None

            do_begin : bool, optional
                Specifies whether to mark the task as started (inserts a timestamp) (True),
                default is False (the task will be started later).

            do_commit : bool, optional
                Whether to commit the data to the database (True), default is False.

        """

        row = {'name':name, 'info_table':info_table}

        row['id'] = self.get_seq_next_value('task_seq') # get task id

        if parent_task_id is not None:
            row['parent_task_id'] = int(parent_task_id)

        if i_am_root:
            row['root_task_id'] = row['id']
        elif root_task_id is not None:
            row['root_task_id'] = int(root_task_id)


        if label is not None:
            row['label'] = label

        self.basic_insert_row('task', row)

        if do_begin:
            self.begin_task(row['id'])

        if do_commit:
            self.commit()
        #self.task_map[row['id']] = parent_task_id
        #self.current_task = row['id']
        return row['id']


    def begin_task(self, task_id, do_commit=False):
        """ Update a row in the task table with beginning of task info

            Parameters
            ----------
            task_id : int
                The id of the task to update

            do_commit : bool, optional
                Whether to commit the data to the database (True), default is False.
        """

        updatevals = {'start_time': self.get_current_timestamp_str(),
                      'exec_host': socket.gethostname()}
        wherevals = {'id': task_id} # get task id

        self.basic_update_row('task', updatevals, wherevals)
        if do_commit:
            self.commit()


    def end_task(self, task_id, status, do_commit=False):
        """ Update a row in the task table with end of task info

            Parameters
            ----------
            task_id : int
                The id of the task to update

            status : int
                The resulting status of the task: 0 = success, anything else indicates
                a failure.

            do_commit : bool, optional
                Whether to commit the data to the database (True), default is False.

        """
        wherevals = {}
        wherevals['id'] = task_id

        updatevals = {}
        updatevals['end_time'] = self.get_current_timestamp_str()
        updatevals['status'] = status

        self.basic_update_row('task', updatevals, wherevals)
        if do_commit:
            self.commit()

    def get_datafile_metadata(self, filetype):
        """ Gets a dictionary of all datafile(such as XML or fits table data files) metadata for the given filetype.
            Returns
            -------
            list
                [target_table_name, metadata]
        """
        TABLE = 0
        HDU = 1
        ATTRIBUTE = 2
        POSITION = 3
        COLUMN = 4
        DATATYPE = 5
        FORMAT = 6

        bindstr = self.get_named_bind_string("afiletype")
        sql = """select table_name, hdu, lower(attribute_name), position, lower(column_name), datafile_datatype, data_format
                from OPS_DATAFILE_TABLE df, OPS_DATAFILE_METADATA md
                where df.filetype = md.filetype and current_flag=1 and lower(df.filetype) = lower(""" + bindstr + """)
                order by md.attribute_name, md.POSITION"""
        result = collections.OrderedDict()
        curs = self.cursor()
        curs.execute(sql, {"afiletype": filetype})

        tablename = None
        for row in curs:
            if tablename is None:
                tablename = row[TABLE]
            if row[HDU] not in result:
                result[row[HDU]] = {}
            if row[ATTRIBUTE] not in result[row[HDU]]:
                result[row[HDU]][row[ATTRIBUTE]] = {}
                result[row[HDU]][row[ATTRIBUTE]]['datatype'] = row[DATATYPE]
                result[row[HDU]][row[ATTRIBUTE]]['format'] = row[FORMAT]
                result[row[HDU]][row[ATTRIBUTE]]['columns'] = []
            if len(result[row[HDU]][row[ATTRIBUTE]]['columns']) == row[POSITION]:
                result[row[HDU]][row[ATTRIBUTE]]['columns'].append(row[COLUMN])
            else:
                result[row[HDU]][row[ATTRIBUTE]]['columns'][row[POSITION]] = row[COLUMN]
        curs.close()
        if tablename is None:
            raise ValueError('Invalid filetype - missing entries in datafile tables')
        return [tablename, result]
