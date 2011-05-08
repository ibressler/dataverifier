#!/usr/bin/python2.7
# -*- coding: utf-8 -*-

import locale
import sys
import getopt
import argparse
import os.path

import os, os.path, re
import codecs
import pickle

# enable utf8 encoding when piped
sys.stdout = codecs.getwriter(locale.getpreferredencoding())(sys.stdout);
#sys.setdefaultencoding('utf8')

class MyError(Exception):
    def __init__(s, msg=""):
        Exception.__init__(s)
        s.msg = msg
    def __str__(s):
        return str(s.msg)

class Usage(MyError): pass

class ChecksumDB():
    dirname = None
    watchlist = None

    def __init__(s, path):
        s.dirname = path
        s.watchlist = dict()

    def empty(s):
        return len(s.watchlist) <= 0

    def __str__(s):
        return "{0} ({1})".format(s.dirname, len(s.watchlist))

def debugPrint(arg):
    s = str(arg[:])
    print s.decode('ascii', 'replace')    

def verify(args):
    print "verify"
    if not hasattr(args, 'infile'):
#       not hasattr(args, 'outfile') or \
#       not hasattr(args, 'pattern'):
        return

    db = pickle.load(args.infile)
    print "Loaded checksums for {0}.".format(db)

def getChecksums(fname):
    if fname is None or not os.path.exists(fname):
        return
    dname = os.path.dirname(fname)
    lst = []
    with codecs.open(fname, encoding='utf8') as fd:
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
                print e
                print line
                raise
            else:
                lst.append((filename, checksum))
    return lst

def updateWatchlist(watchlist, newList):
    for filename, checksum in newList:
        if watchlist.has_key(filename):
            oldChecksum = watchlist[filename]
            # prefer longer (sha) checksums and ignore duplicates
            if len(oldChecksum) > len(checksum):
                continue
            elif len(oldChecksum) == len(checksum) and \
                    oldChecksum != checksum:
                print filename, watchlist[filename], checksum
                continue

        watchlist[filename] = checksum

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

    db = ChecksumDB(directory)
    generator = os.walk(directory, followlinks=True)
    for (dirpath, dirnames, filenames) in generator:
        # add all files within the current directory
        for fname in filenames:
            match = re.search(pattern, fname)
            if match is not None:
                # why does it work with decode?
                fullname = os.path.join(dirpath,fname).decode('utf8')
                print fullname
                lst = getChecksums(fullname)
                updateWatchlist(db.watchlist, lst)

    if db.empty():
        raise MyError("Checksum list is empty, nothing to save.")
    pickle.dump(db, outfile)
    print "Saved {0} entries.".format(len(db.watchlist))

    return

    countMissing = 0
    for filename, checksum in watchlist.items():
        if not os.path.exists(filename):
            countMissing += 1
            print filename

    print countMissing, 'missing files', len(watchlist), 'overall'

def main():
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
