#!/usr/bin/env python3


import base64
import os
import pickle
import subprocess
import sys


from gppylib.operations.validate_disk_space import FileSystem
from gppylib.gpparseopts import OptParser, OptChecker
from gppylib.mainUtils import addStandardLoggingAndHelpOptions



# for each directory the filesystem and calculate the free disk space.
# Return a list of Filesystem() objects.
def calculate_disk_free(directories):
    #map fo FileSystem() to directory List
    filesystem_to_dirs = {}

    for dir in directories:
        cmd = __disk_free(dir)
        if cmd.returncode > 0:
            sys.stderr.write("Failed to calculate free disk space : %s" % cmd.stderr)
            return []

        #skip the header , which is the first line
        for line in cmd.stdout.split('\n')[1:]:
            if line != "":
                parts = line.split()
                fs = FileSystem(parts[0] , disk_free=int(parts[3]))
                filesystem_to_dirs.setdefault(fs, []).append(dir)


    #embedding directory in filesystem
    filesystems = []
    for fs , directories in filesystem_to_dirs.items():
        fs.directories = directories
        filesystems.append(fs)

    return filesystems



# Since the input directory may not have been created df will fail.Thus, take each path element
# starting at the end and execute df untill it succeds in order to find the filesystem and free space
def __disk_free(directory):
    #the -P flag is for POSIX formatting to prevent errors on lines that would wrap
    cmd =  subprocess.run(["df", "-Pk", directory],
                          stdout=subprocess.PIPE,
                          stderr=subprocess.PIPE,
                          universal_newlines=True)

    if directory == os.sep:
        return cmd

    if cmd.returncode > 0:
        path, last_dir = os.path.split(directory)
        return __disk_free(path)

    return cmd


def create_parser():
    parser = OptParser(option_class=OptChecker,
                       description='Calculates the disk free for the filesystem given the input directory. '
                                    'Returns a list of base64 encoded pickled Filesystem Objects.')

    addStandardLoggingAndHelpOptions(parser, includeNonInteractiveOption=True)

    parser.add_option('-d', '--directories',
                       help='list of directories to calculate the disk usage for the filesystem',
                       type='string',
                       action='callback',
                       callback= lambda option, opt, value, parser: setattr(parser.values, option.dest, value.split(',')),
                       dest='directories')

    return parser




# NOTE: The Caller uses the Command Framework such as CommandResult which assumes that the **only** thing 
# written to stdout is the result. Thus, do not use a logger to print to stdout as that would affect the
# deserialiozation of actual results
def main():
    parser = create_parser()
    (options, args) = parser.parse_args()

    filesystems = calculate_disk_free(options.directories)
    sys.stdout.write(base64.urlsafe_b64encode(pickle.dumps(filesystems)).decode('UTF-8'))
    return


if __name__ == "__main__":
    main()


