#!/usr/bin/python2.7
# -*- coding: utf-8 -*-

import locale
import sys
import getopt
import argparse
import os.path
import os
import re
import codecs
import pickle
import StringIO
import time
import logging

import cfv

# enable utf8 encoding when piped
sys.stdout = codecs.getwriter(locale.getpreferredencoding())(sys.stdout);
#sys.setdefaultencoding('utf8')

def formattime(timestamp):
    if timestamp is None:
        return None
    return unicode(time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(timestamp)))

class MyError(Exception):
    def __init__(s, msg=""):
        Exception.__init__(s)
        s.msg = msg
    def __str__(s):
        return str(s.msg)

class Usage(MyError): pass

class ChecksumFile(object):
    filename = None
    filelist = None
    timestamp = None
    type = None

    def __init__(s, filename):
        if filename is None or not os.path.exists(filename):
            raise MyError("Given checksum file '{0}' does "
                          "not exist!".format(filename))
        statdata = os.stat(filename)
        s.timestamp = statdata.st_mtime
        s.filelist = s._getChecksums(filename)
        s.filename = filename

    # parses the checksum files *.md5 or *.sha
    def _getChecksums(s, fname):
        dname = os.path.dirname(fname)
        lst = []
        with codecs.open(fname, encoding='utf8') as fd:
            s.type = cfv.auto_chksumfile_match(cfv.PeekFile(fd))
            for line in fd:
                try:
                    if len(line) < 2 or line.startswith(u'#'):
                        continue
                    fields = line.split()
                    if len(fields) < 2:
                        continue
                    checksum = fields[0].strip()
                    if len(checksum) < 2:
                        continue
                    filename = line[len(checksum):].strip().lstrip(u'*')
                    filename = os.path.join(dname, filename)
                except Exception, e:
                    logging.error(str(e)+'\n'+line)
                    raise
                else:
                    lst.append((filename, checksum))
        return lst

    def empty(s):
        if s.timestamp is None or s.timestamp <= 0 or \
           s.filelist is None or len(s.filelist) <= 0 or \
           s.filename is None or len(s.filename) <= 0:
            return True
        return False

    def __str__(s):
        output = StringIO.StringIO()
        for fn,chk in s.filelist:
            print >>output, chk +u' *'+ fn
        print >>output, unicode(s.timestamp), formattime(s.timestamp)
        return output.getvalue()

class ChecksumDB(object):
    dirname = None
    watchlist = None

    def __init__(s, directory, pattern):
        s.dirname = directory
        s.watchlist = dict()

        generator = os.walk(directory, followlinks=True)
        i = 0
        for (dirpath, dirnames, filenames) in generator:
            # add all files within the current directory
            for fname in filenames:
                match = re.search(pattern, fname)
                if match is not None:
                    # why does it work with decode?
                    fullname = os.path.join(dirpath,fname)
                    checksumFile = ChecksumFile(fullname)
                    s.update(checksumFile)
#                    if i == 22: return
                    i += 1

    def store(s, outfile):
        if s.empty():
            raise MyError("Checksum DB is empty, nothing to save.")
        pickle.dump(s, outfile)
        logging.info("Saved {0} entries.".format(len(s.watchlist)))

    @staticmethod
    def load(infile):
        db = pickle.load(infile)
        return db

    def empty(s):
        return len(s.watchlist) <= 0

    def __str__(s):
        return u"{0} ({1})".format(s.dirname, len(s.watchlist))

    def update(s, checksumFile):
        if checksumFile is None or checksumFile.empty():
            return
        logging.info(u"Adding checksums from file '{0}' .."
                     .format(checksumFile.filename))
        newTime = checksumFile.timestamp
        for filename, newChecksum in checksumFile.filelist:
            if s.watchlist.has_key(filename):  # resolve conflicts
                oldChecksum, oldTime, type = s.watchlist[filename]
#                logging.info(type)
                # TODO: convert checksums to integer for comparison?
                if oldChecksum == newChecksum: # ignore identical checksums
                    continue
                if oldTime >= newTime:
                    # don't replace a recent checksum with an old one
                    logging.warning(u"Skipping '{0}': "
                                 u"outdated checksum {1} ({2}) "
                                 u"vs. {3} ({4}) from DB)"\
                                 .format(filename, 
                                         newChecksum, formattime(newTime), 
                                         oldChecksum, formattime(oldTime)))
                    continue

            s.watchlist[filename] = (newChecksum, newTime, checksumFile.type)
        logging.info("done.")

## commands ##

def verify(args):
    print "verify"
    if not hasattr(args, 'infile'):
#       not hasattr(args, 'outfile') or \
#       not hasattr(args, 'pattern'):
        return

    db = ChecksumDB.load(args.infile)
    for k,v in db.watchlist.items():
        print k,v
    print "Loaded checksums for {0}.".format(db)

# parses existing checksum files and generates a database from them
# TODO: process checksum files in parallel
def create(args):
    if not hasattr(args, 'directory') or \
       not hasattr(args, 'outfile') or \
       not hasattr(args, 'pattern'):
        return

    directory, pattern, outfile = args.directory, args.pattern, args.outfile

    msg = "Creating DB from files in '{0}' with pattern '{1}'. Saving to '{2}'.".\
        format(directory, pattern.pattern, outfile.name)
    print msg, "continue ? [Yn]"
    answer = sys.stdin.readline()
    if len(answer) > 1:
        return 1

    db = ChecksumDB(directory, pattern)
    db.store(outfile)

def logFormatter():
    fmtr = logging.Formatter(fmt='%(asctime)s %(levelname)-8s %(message)s',
                             datefmt='%Y-%m-%d %H:%M:%S')
    return fmtr

def main():
    # configure console logging
    handler = logging.StreamHandler(sys.stdout)
    handler.setFormatter(logFormatter())
    logging.getLogger().addHandler(handler)
    logging.getLogger().setLevel(logging.NOTSET)

    parser = argparse.ArgumentParser(description=
                                     "Maintain data consistency of a "
                                     "directory file structure")
    subparsers = parser.add_subparsers(title='available commands',
                                      help="Run 'COMMAND -h' for more specific help")

    parser_create = subparsers.add_parser("create")
    parser_create.description = "Create database from checksum files "+\
                                "by recursive directory search."
    parser_create.set_defaults(func=create)
    parser_create.add_argument('-d', '--dir', dest='directory', 
                               default=os.getcwdu(),
                               metavar='DIR',
                               help='working directory, current otherwise')
    parser_create.add_argument('-p', '--pattern', dest='pattern',
                               default=u".*\\.sha|.*\\.md5",
                               metavar='REGEX', 
                               help='regular expression pattern of checksum files')
    parser_create.add_argument('-o', '--outfile', dest='outfile',
                               default=u"checksum.db",
                               type=argparse.FileType('w'),
                               metavar='OUTFILE',
                               help='output filename for checksum database')

    parser_verify = subparsers.add_parser("verify")
    parser_verify.description = "Verify a directory structure based on an "+\
            "existing checksum database and sync with changes"
    parser_verify.set_defaults(func=verify)
    parser_verify.add_argument('-i', '--infile', dest='infile',
                               default=u"checksum.db",
                               type=argparse.FileType('rw'),
                               metavar='INFILE',
                               help='filename for checksum database to update')

    if len(sys.argv) <= 1:
        parser.print_help()
        return 1

    args = parser.parse_args()
    # check for a valid regex
    if hasattr(args, 'pattern'):
        try:
            pattern = re.compile(args.pattern)
        except Exception, e:
            raise MyError("Erroneous regular expression specified: '{0}'"
                      .format(args.pattern))
        else:
            args.pattern = pattern

    # call selected function
    return args.func(args)

if __name__ == "__main__":
    sys.exit(main())
# vim: set ts=8 sw=4 tw=0: 
