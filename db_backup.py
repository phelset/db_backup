#!/usr/bin/env python
#
# Database weekly rolling backup script
# Supports MySQL and PostgreSQL
# Petter Aksnes Helset <petter@helset.eu>
#
from datetime import datetime
import sys, os, subprocess, gzip, argparse, ConfigParser, pwd, grp

class DatabaseBackupConfig:
    def __init__(self, config, section):
        # parse config
        try:
            self.dbtype = config.get(section, 'type')
            self.database = section
            self.hostname = config.get(section, 'hostname')
            self.username = config.get(section, 'username')
            self.password = config.get(section, 'password')
            self.chown = config.get(section, 'chown')
            self.chgrp = config.get(section, 'chgrp')
            self.chmod = config.get(section, 'chmod')
            self.outputdir = config.get(section, 'outputdir')
        except ConfigParser.NoOptionError as e:
            sys.exit("Failed to parse config (%s)" % e)

        # validate chown
        try:
            if self.chown != None:
                self.chown_uid = pwd.getpwnam(self.chown).pw_uid
            else:
                self.chown_uid = -1
        except KeyError:
            sys.exit("User %s in section %s does not exist" % (self.chown, self.database))

        # validate chgrp
        try:
            if self.chgrp != None:
                self.chgrp_gid = grp.getgrnam(self.chgrp).gr_gid
            else:
                self.chgrp_gid = -1
        except KeyError:
            sys.exit("Group %s in section %s does not exist" % (self.chgrp, self.database))

        # validate output directory
        if not os.access(self.outputdir, os.W_OK):
            sys.exit("Output directory %s in section %s is not writeable" % (self.outputdir, self.database))

def parse_args():
    # Parse arguments
    parser = argparse.ArgumentParser(description="Database weekly rolling backup script. Supports MySQL and PostgreSQL.", formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    parser.add_argument('-c', default='backup.ini', metavar='file', help='Configuration file')
    parser.add_argument('-o', default=os.curdir, metavar='directory', help='Output directory')
    #parser.add_argument('-s', help='Section(s), comma separated list of sections to run')
    args = parser.parse_args()
    conf = args.c
    output = args.o

    # Validate arguments
    if not os.path.isfile(conf):
        sys.exit("Configuration file '%s' not found" % conf)
        
    if not os.access(conf, os.R_OK):
        sys.exit("Configuration file '%s' is not readable" % conf)
        
    if not os.path.exists(output):
        sys.exit("Output directory not found" % output)
        
    return conf, output

def dump_database(config):
    # Create sql file
    date = datetime.now().strftime('%w')
    filename =  '%s-%s.sql' % (config.database, date)
    file = os.path.join(config.outputdir, filename)
    dumpfile = open(file, 'w')
    os.chmod(file, 0600)

    # Create command
    if config.dbtype == 'mysql':
        cmd = ['mysqldump', '-h', config.hostname, '-u', config.username, '-p%s' % config.password, config.database]
    elif config.dbtype == 'pgsql':
        os.putenv('PGPASSWORD', config.password)
        cmd = ['pg_dump', '-h', config.hostname, '-U', config.username, config.database]
    else:
        sys.exit("Unsupported database type '%s'" % config.dbtype)

    # Execute command w/stdout to sql file
    proc = subprocess.Popen(cmd, stdout=dumpfile, stderr=subprocess.PIPE)
    retcode = proc.wait()
    dumpfile.close()
    if retcode > 0:
        print "Failed to backup database '%s'" % config.database
        stderr = proc.stderr.read()
        print stderr.strip()
        os.remove(file)
    else:
        compress_file(config, filename)

    if config.dbtype == 'pgsql':
        del os.environ['PGPASSWORD']
 
def compress_file(config, filename):
    file = os.path.join(config.outputdir, filename)
    filegz = '%s.gz' % file
    with open(file, 'rb') as f:
        with gzip.open(filegz, 'wb') as c:
            c.writelines(f)
            c.close()
        f.close()
        os.remove(file)
        os.chmod(filegz, int(config.chmod, 8))
        os.chown(filegz, config.chown_uid, config.chgrp_gid)

if __name__ == '__main__':
    cnf, output = parse_args()

    config = ConfigParser.ConfigParser({'chown': None, 'chgrp': None, 'chmod': '0600', 'outputdir': output})
    config.read(cnf)
    for section in config.sections():
        dump_database(DatabaseBackupConfig(config, section))
