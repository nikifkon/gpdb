import base64
import pickle

from gppylib.commands.base import REMOTE, WorkerPool, Command
from gppylib.commands.unix import DiskFree, DiskUsage, MakeDirectory
from gppylib import gparray, gplog, userinput
from gppylib.db import dbconn
from gppylib.gplog import get_default_logger
from gppylib.operations.segment_tablespace_locations import get_tablespace_locations

logger = get_default_logger()

class RelocateSegmentPair:
    """
    RelocateSegmentPair maps source and target information for ease of use in 
    other functions. For example, it encapsulates the information needed to
    calculate disk usage on the source host to ensure enough free space on
    the target host.

    The parameter hostaddr is the hostname or ip address of the segment.
    """
    def __init__(self, source_hostaddr, source_data_dir, target_hostaddr, target_data_dir):
        self.source_hostaddr = source_hostaddr
        self.source_data_dir = source_data_dir
        self.target_hostaddr = target_hostaddr
        self.target_data_dir = target_data_dir


class RelocateDiskUsage:
    """
    RelocateDiskUsage validates if there is enough disk space on the target host
    for the source data directories and tablespaces.

    The parameter pairs is a list of RelocateSegmentPair().
    """
    def __init__(self, src_tgt_mirror_pair_list , batch_size):
        #Primary Mirror RelocateSegmentPair() list
        self.src_tgt_mirror_pair_list = src_tgt_mirror_pair_list
        self.batch_size = batch_size
        for pair in self.src_tgt_mirror_pair_list:
            #Map of tablespace location to disk usage
            pair.source_tablespace_usage = {}
            pair.source_data_dir_usage = 0
            pair.dirs_created = None

    def validate_disk_space(self):
        """
        Validate if the required disk space is available in target host to move mirror.
        """
        logger.info("Validating disk space requirement...")

        #Fetch Disk Usage of mirror in source host
        self._determine_source_disk_usage()

        #Fetch the free space available in target host and compare it with required disk space
        for hostaddr, filesystems in self._target_host_filesystems().items():
            for fs in filesystems:
                #Add 10% buffer in the free disk space available to warn/notice user if we are filling up the full disk space if
                #Disk free space is exactly same as disk space required
                disk_free_space_with_buff = int ((10 * fs.disk_required ) / 100)
                if fs.disk_free <= fs.disk_required:
                    logger.error("Not enough space on host {} for directories {}." .format(hostaddr, ', '.join(map(str, fs.directories))))
                    logger.error("Filesystem {} has {} kB available, but requires {} kB." .format(fs.name, fs.disk_free, fs.disk_required))
                    return False
                elif fs.disk_free < (disk_free_space_with_buff + fs.disk_required) :
                    if not userinput.ask_yesno(None, "\nLess than 10% of disk space will be avialable after mirror is moved."\
                                                      "Continue with segment move procedure", 'N'):
                        raise UserAbortedException()

        return True

    # Calculate disk usage for the data directory, and all user defined
    # tablespaces for the source host.
    # Note: The user can specify a different target data directory from
    # the source. However, we do not allow them to specify different tablespace
    # locations from the source to target since the primary and mirror tablespace
    # locations must match.
    def _determine_source_disk_usage(self):
        logger.debug("Determinig source disk usage of mirrors...")
        for pair in self.src_tgt_mirror_pair_list:
            #Key being data dir and value being disk usage
            pair.source_data_dir_usage = self._disk_usage(pair.source_hostaddr, [pair.source_data_dir]) [pair.source_data_dir]
            pair.source_tablespace_usage = self._disk_usage(pair.source_hostaddr, get_tablespace_locations(False, pair.source_data_dir))
            #pair.source_tablespace_usage = self._disk_usage(pair.source_hostaddr, get_segment_tablespace_dirs(pair.source_data_dir))


    """ Determine the Disk usage of mirror directory in source host """
    def _disk_usage(self, hostaddr, dirs):
        #map of directories to disk usage
        disk_usage_dirs = {}
        num_dir = len(dirs)

        #In case there are no input dirs return xfrom here
        if num_dir <= 0:
            return disk_usage_dirs

        pool = WorkerPool(numWorkers=min(num_dir, self.batch_size))
        try:
            for directory in dirs:
                cmd = DiskUsage('Check Source Segment disk space usage', directory, ctxt=REMOTE, remoteHostAddr=hostaddr)
                pool.addCommand(cmd)
            pool.join()
        finally:
            pool.haltWork()
            pool.joinWorkers()


        for cmd in pool.getCompletedItems():
            if not cmd.was_successful():
                raise Exception("Unable to check disk usage on source segment:" + cmd.get_results().stderr )

            disk_usage_dirs[cmd.directory] = cmd.kbytes_used()

        return disk_usage_dirs


    #create a host to directories mapping
    def _get_hostaddr_directories(self):
        #Create a map of host with all it's directories
        hostaddr_to_dirs_mapping = {}
        for pair in self.src_tgt_mirror_pair_list:
            tblSpcDirs = list(pair.source_tablespace_usage.keys())
            if pair.target_hostaddr not in hostaddr_to_dirs_mapping:
                hostaddr_to_dirs_mapping[pair.target_hostaddr] = [pair.target_data_dir]  
                if len(tblSpcDirs) > 0 :
                    hostaddr_to_dirs_mapping[pair.target_hostaddr].append(tblSpcDirs)
                continue
            hostaddr_to_dirs_mapping[pair.target_hostaddr].append(pair.target_data_dir)
            if len(tblSpcDirs) > 0:
                hostaddr_to_dirs_mapping[pair.target_hostaddr].append(tblSpcDirs)
            
        logger.debug("Target Host Address to Directory Mapping is {} ." .format(hostaddr_to_dirs_mapping))
        return hostaddr_to_dirs_mapping
    
    # Return a map of hostaddr to target filssytem list containing disk space available and
    # disk space required statistics.
    def _target_host_filesystems(self):
        
        logger.debug("Determinig Target host available free disk space.")
        
        # hostaddr to Filesystem List mapping
        hostaddr_to_filesystems_mapping = {}

        #for hostaddr, dirs in self._get_hostaddr_directories().items():
        target_filesystems = self._get_target_filesystem()

        #list of list
        for tgt_fs in target_filesystems:

            for fs in tgt_fs:
                # if host addr is not allready present , create one
                if fs.hostaddr not in hostaddr_to_filesystems_mapping:
                        hostaddr_to_filesystems_mapping[fs.hostaddr] = []

                # Add first filesystem to list if filesystem is not present
                tgt_host_filesystems = hostaddr_to_filesystems_mapping[fs.hostaddr]
                if not tgt_host_filesystems:
                    tgt_host_filesystems.append(fs)
                    continue


                for known_host_fs in tgt_host_filesystems:
                    if fs.name == known_host_fs.name:
                        known_host_fs.disk_required += fs.disk_required
                        known_host_fs.directories += set(fs.directories)

                #Add the filesytem directory if it is different
               # dirs_to_add = set(tgt_fs).difference(set(tgt_host_filesystems))
                #tgt_host_filesystems.extend(dirs_to_add)

        return hostaddr_to_filesystems_mapping



    #For each source target pair fetch the target filesystem with free disk space available
    def _get_target_filesystem(self):
        #List of filesystem info encapsulated in FileSystem()
        tgt_filesystems_list = []
        
        hostaddr_directories = self._get_hostaddr_directories()
        num_dir = len(hostaddr_directories)

        #Return if there are zero directories to query free disk space
        if num_dir <= 0:
            return tgt_filesystems_list
        
        pool = WorkerPool( numWorkers=min(num_dir, self.batch_size))
        try:
            for hostaddr, dirs in hostaddr_directories.items():
                cmd = DiskFree(hostaddr, dirs)
                pool.addCommand(cmd)
            pool.join()
        finally:
            pool.haltWork()
            pool.joinWorkers()


        for cmd in pool.getCompletedItems():
            if not cmd.was_successful():
                raise Exception("Failed to check disk free on target segment: " + cmd.get_results().stderr)

            tgt_filesystems = pickle.loads(base64.urlsafe_b64decode(cmd.get_results().stdout))

            for fs in tgt_filesystems:
                fs.add_disk_usage(self.src_tgt_mirror_pair_list)
            
            tgt_filesystems_list.append(tgt_filesystems)
            

        return tgt_filesystems_list



class FileSystem:
    """
    Information about filesystem free disk space and directory list for mirror 
    All free disk space and disk space required will be in kB
    """
    def __init__(self, name, disk_free=0):
        self.name = name
        self.disk_free = disk_free  
        self.disk_required = 0
        self.directories = None
        self.hostaddr = None

    # Add Required disk space in target filesystem by comparing filesystem 
    # directory list with target directory 
    def add_disk_usage(self, src_tgt_mirror_pair_list):
        #Walk-through the pair list to get the matching pair
        for pair in src_tgt_mirror_pair_list:
            tblSpcDirs = list(pair.source_tablespace_usage.keys())
            #walk through target data directory
            for dir in self.directories:
                if dir == pair.target_data_dir:
                    self.disk_required += pair.source_data_dir_usage
                else: #TODO - Check below else without condition
                    #check for Tablespace dirs for  that pair of mirror
                    for tblspcdir in tblSpcDirs:
                        if dir == tblspcdir:
                            self.disk_required += pair.source_tablespace_usage[dir]


class InsufficientDiskSpaceError(Exception):
    pass



#FIXME:filter based on content not datadirectory
def get_segment_tablespace_dirs(dataDirectory):
    """
    Create List of user created tablespace locations for a segment data directory
    """

    tblspcLocs = []
    tblspcOids = []

    getTblespcOids_sql="SELECT oid FROM pg_tablespace where spcname not in ('pg_default', 'pg_global')"
    getTblespcLocs_sql="SELECT t.tblspc_loc ||'/'||dbid from gp_tablespace_locations(%s) t, gp_segment_configuration c where t.gp_segment_id=c.content and c.datadir='%s'"

    with dbconn.connect(dbconn.DbURL()) as conn:
        result = dbconn.execSQL(conn, getTblespcOids_sql)
        if result:
            tblspcOids = dbconn.execSQL(conn, getTblespcOids_sql).fetchall()


    #USe tablespace OOIDS to get the tablespace locations
    for row in tblspcOids:
        oid = row[0]
        with dbconn.connect(dbconn.DbURL()) as conn:
            result=dbconn.execSQL(conn, getTblespcLocs_sql % (oid, dataDirectory)).fetchall()
            tblspcLocs.append(result)

    return tblspcLocs




