#!/usr/bin/python2.7
# -*- coding: utf-8 -*-
#
#    dataverifier.py
#    Copyright (C) 2011 Ingo Breßler (dev at ingobressler.net)
#
#    This program is free software: you can redistribute it and/or modify
#    it under the terms of the GNU General Public License as published by
#    the Free Software Foundation, either version 3 of the License, or
#    (at your option) any later version.
#
#    This program is distributed in the hope that it will be useful,
#    but WITHOUT ANY WARRANTY; without even the implied warranty of
#    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
#    GNU General Public License for more details.
#
#    You should have received a copy of the GNU General Public License
#    along with this program.  If not, see <http://www.gnu.org/licenses/>.


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
import binascii

# enable utf8 encoding when piped
sys.stdout = codecs.getwriter(locale.getpreferredencoding())(sys.stdout);
#sys.setdefaultencoding('utf8')

def formattime(timestamp):
    if timestamp is None:
        return None
    return unicode(time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(timestamp)))

# TODO
def uniqueTemporaryFilename():
    return "unittestfilename"

class MyError(Exception):
    def __init__(s, msg=""):
        Exception.__init__(s)
        s.msg = msg
    def __str__(s):
        return str(s.msg)

class Usage(MyError): pass
class FileNotFound(MyError): pass

class ChecksumFile(object):
    """
    
    Test blank checksum file.
    >>> try: cf = ChecksumFile(None)
    ... except Exception, e: e.__class__.__name__
    'FileNotFound'

    Prepare checksum file for testing.
    >>> import time
    >>> filename = uniqueTemporaryFilename()
    >>> testdata = ("40c6f45b5673a3cc023eb175ea9e8c4e496c3217  bla.txt\\na6348cdee4b941e888d9a92c3cd67f4fa46e7156 *blub.bin\\n")
    >>> timestamp = time.time()
    >>> with open(filename, 'w') as fd:
    ...     fd.write(testdata)

    Test previously created checksum file.
    >>> cf = ChecksumFile(filename)
    >>> cf.filename == filename
    True
    >>> abs(cf.timestamp - timestamp) < 2.0 # max 2 sec diff., OS dependent
    True
    >>> cf.checksumType.__name__
    'SHA1'
    >>> cf.filelist
    [(u'bla.txt', u'40c6f45b5673a3cc023eb175ea9e8c4e496c3217'), (u'blub.bin', u'a6348cdee4b941e888d9a92c3cd67f4fa46e7156')]

    Remove checksum file from testing.
    >>> if os.path.isfile(filename):
    ...     os.remove(filename)
    """
    _filename = None
    _filelist = None
    _timestamp = None
    _type = None

    @property
    def filename(self):
        return self._filename

    @property
    def filelist(self):
        return self._filelist

    @property
    def timestamp(self):
        return self._timestamp

    @property
    def checksumType(self):
        return self._type

    def __init__(self, filename):
        if filename is None or not os.path.isfile(filename):
            raise FileNotFound("Given checksum file '{0}' does "
                               "not exist!".format(filename))
        statdata = os.stat(filename)
        self._timestamp = statdata.st_mtime
        self._filelist = self._getChecksums(filename)
        self._filename = filename

    # parses the checksum files *.md5 or *.sha
    def _getChecksums(self, fname):
        dname = os.path.dirname(fname)
        lst = []
        with codecs.open(fname, encoding='utf8') as fd:
            self._type = cfv.auto_chksumfile_match(cfv.PeekFile(fd))
            fd.seek(offset = 0, whence = 0) # rewind
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

    def __str__(self):
        output = StringIO.StringIO()
        for fn,chk in self._filelist:
            print >>output, chk +u' *'+ fn
        print >>output, unicode(self._timestamp), formattime(self._timestamp)
        return output.getvalue()

class ChecksumDB(object):
    """
    >>> print "bla"
    """
    _directory = None
    _watchlist = None
    _excludes = None # which files not to monitor
    _checkInterval = float(3600*24 * 14)
    # for store/load consistency tests
    _count = None

    @property
    def directory(self):
        return self._directory

    def __init__(self, directory, pattern):
        directory = os.path.abspath(directory)
        if not os.path.isdir(directory):
            raise MyError(u"Provided directory '{0}' does not exist!"
                         .format(directory))

        self._directory = directory
        self._watchlist = dict()
        self._excludes = set()

        # add all checksum files within the current directory
        for fn in self.treeFiles(pattern):
            match = pattern.search(fn)
            if match is None:
                continue
            fullname = os.path.join(path,fn)
            checksumFile = ChecksumFile(fullname)
            self.addFromFile(checksumFile)

    def exclude(self, pattern):
        self.excludes.add(pattern)

    def treeFiles(self, pattern = None):
        if not os.path.isdir(self.directory):
            return
        if pattern is not None:
            pattern = re.compile(pattern)
        for (path, dirnames, filenames) in os.walk(self.directory,
                                                   followlinks=True):
            for fn in filenames:
                if pattern is not None and pattern.search(fn) is None:
                    continue
                fullname = os.path.join(path,fn)
                yield fullname

    # filename: file to add checksum for
    # type: cfv checksum type (e.g. cfv.SHA1())
    def _updateEntry(s, filename, type):
        if filename is None or type is None:
            return
        if not hasattr(s, 'checksumTypes') or not hasattr(s, 'currentTime'):
            return
        checksumType = s.checksumTypes[type]
        if checksumType is None:
            return
        (newChecksum, filesize), dat = checksumType.make_addfile(filename)
        s.watchlist[filename] = (newChecksum, s.currentTime, type)
        logging.info(u"NEW: '{0}' '{1}'".format(filename, s.watchlist[filename][0]))
        s.newFiles.append((newChecksum, filename))

    def check(s):
        # traverse the filesystem and lookup each visited file in the DB
        # faster on disk (?) than random picking of files
        logging.info(u"Starting check ..")
        visited = set()
        s.currentTime = time.time()
        s.checksumTypes = { cfv.SHA1: cfv.SHA1() } # default type for creating
        s.newFiles = []
        s.mismatchFiles = []
        cfv.chdir(s.dirname)
        for filename in s.treeFiles():
            filename = os.path.relpath(filename, s.dirname)
            logging.debug(filename)
            visited.add(filename)

            if filename in s.watchlist:
                logging.debug(u"found")
                oldChecksum, oldTime, oldType = s.watchlist[filename]

                # ignore if recently tested
                if oldTime+s.checkInterval > s.currentTime:
                    #continue
                    pass

                # get checksum algorithm instance from cfv
                checksumType = None
                if oldType in s.checksumTypes:
                    checksumType = s.checksumTypes[oldType]
                else:
                    checksumType = oldType()
                    s.checksumTypes[oldType] = checksumType

                # calc checksum and compare 
                retval = checksumType.test_file(filename, binascii.a2b_hex(oldChecksum))
                if retval is not None: # crc mismatch
                    logging.warning(u"Checksum for '{0}' did not match.".format(filename))
                    s.mismatchFiles.append((oldChecksum, filename))
                    s._updateEntry(filename, cfv.SHA1)
                else:
                    #logging.info(u"OK: '{0}'".format(filename))
                    pass

            elif filename not in s.excludes: # not in s.watchlist
                s._updateEntry(filename, cfv.SHA1)

        deleted = s.watchlist.viewkeys() - visited
        logging.info(u"{0} files do not exist".format(len(deleted)))

        logging.info(u"done.")
        for checksum, filename in s.newFiles:
            logging.info(u"NEW: '{0}' '{1}'".format(checksum, filename))
        for checksum, filename in s.mismatchFiles:
            logging.warning(u"MISMATCH: '{0}' '{1}'".format(checksum, filename))
        for filename in deleted:
            logging.info(u"DELETED: '{0}'".format(filename))

    def store(s, outfile):
        outfile = os.path.abspath(outfile)
        dirname = os.path.dirname(outfile)
        if not os.path.isdir(dirname):
            raise MyError(u"Directory for database file '{0}' does not exist!"
                         .format(dirname))
        s._count = len(s.watchlist)
        if s.empty():
            raise MyError("Checksum DB is empty, nothing to save.")
        with open(outfile, 'w') as fd:
            pickle.dump(s, fd)
        logging.info("Saved {0} entries.".format(len(s.watchlist)))

    def isValid(s):
        if s._count == len(s.watchlist):
            return True
        return False

    @staticmethod
    def load(filename):
        filename = os.path.abspath(filename)
        if not os.path.isfile(filename):
            raise MyError(u"Database file '{0}' does not exist!"
                         .format(filename))
        with open(filename, 'r') as fd:
            db = pickle.load(fd)
        if not db.isValid():
            raise MyError("Loading DB file '{0}' failed!".format(infile.name))
        relname = os.path.relpath(filename, db.dirname)
        if not relname.startswith(u'..'):
            # infile is part of monitored directory tree
            db.exclude(relname)
        return db

    def empty(s):
        return len(s.watchlist) <= 0

    def __str__(s):
        return u"{0} ({1})".format(s.dirname, len(s.watchlist))

    def addFromFile(s, checksumFile):
        if checksumFile is None:
            return
        logging.info(u"Adding checksums from file '{0}' .."
                     .format(checksumFile.filename))
        newTime = checksumFile.timestamp
        newType = checksumFile.checksumType
        for filename, newChecksum in checksumFile.filelist:
            # getting absolute filenames here, making them local to DB.dirname
            filename = os.path.relpath(filename, s.dirname)
            logging.debug(filename)
            if filename in s.watchlist:  # resolve conflicts
                oldChecksum, oldTime, oldType = s.watchlist[filename]
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

            s.watchlist[filename] = (newChecksum, newTime, newType)
        logging.info("done.")

## commands ##

def doctest(dummy):
    import doctest
    doctest.testmod()

def verify(args):
    print "verify"
    if not hasattr(args, 'filename'):
        return

    db = ChecksumDB.load(args.filename)
    print "Loaded checksums for {0}.".format(db)
    db.check()
    db.store(args.filename) # replace with updated db

# parses existing checksum files and generates a database from them
# TODO: process checksum files in parallel
def create(args):
    if (   not hasattr(args, 'directory')
        or not hasattr(args, 'filename')
        or not hasattr(args, 'pattern')):
        return

    directory, pattern, filename = args.directory, args.pattern, args.filename
    msg = "Creating DB from files in '{0}' with pattern '{1}'. Saving to '{2}'.".\
        format(directory, pattern.pattern, filename)
    print msg, "continue ? [Yn]"
    answer = sys.stdin.readline()
    if len(answer) > 1:
        return 1

    db = ChecksumDB(directory, pattern)
    db.store(filename)

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
    parser.add_argument("-l", "--loglevel", dest = "loglevel",
                        default = logging.getLevelName(logging.INFO),
                        metavar = 'LEVEL',
                        help = ("one of CRITICAL, ERROR, WARNING, INFO, DEBUG "
                                "(default: '%(default)s')"))
    subparsers = parser.add_subparsers(title = "available commands",
                                      help = ("Run 'COMMAND -h' for more "
                                              "specific help"))

    parser_create = subparsers.add_parser("create")
    parser_create.description = ("Create database from checksum files "
                                 "by recursive directory search.")
    parser_create.set_defaults(func = create)
    parser_create.add_argument("-d", "--dir", dest = "directory",
                               default = os.getcwdu(),
                               metavar = "DIR",
                               help = ("working directory (default: "
                                       "'%(default)s')"))
    parser_create.add_argument("-p", "--pattern", dest = "pattern",
                               default = r".*\.sha|.*\.md5",
                               metavar = "REGEX",
                               help = ("regular expression pattern of checksum "
                                       "files (default: '%(default)s')"))
    parser_create.add_argument("-f", "--filename", dest = "filename",
                               default = u"checksum.db",
                               metavar = "FILENAME",
                               help = ("output filename for checksum database "
                                       "(default: '%(default)s')"))

    parser_verify = subparsers.add_parser("verify")
    parser_verify.description = ("Verify a directory structure based on an "
                                 "existing checksum database and sync with "
                                 "changes")
    parser_verify.set_defaults(func = verify)
    parser_verify.add_argument("-f", "--filename", dest = "filename",
                               default = u"checksum.db",
                               metavar = "FILENAME",
                               help = ("filename for checksum database to update "
                                       "(default: '%(default)s')"))

    parser_create = subparsers.add_parser("unittest")
    parser_create.description = "Run all unit tests to verify code integrity."
    parser_create.set_defaults(func=doctest)

    if len(sys.argv) <= 1:
        parser.print_help()
        return 1

    args = parser.parse_args()
    # set log level
    if hasattr(args, 'loglevel') and hasattr(logging, args.loglevel):
        logging.getLogger().setLevel(getattr(logging, args.loglevel))

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
