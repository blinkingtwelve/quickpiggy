#!/usr/bin/env python
"""
QuickPiggy - launch an impromptu PostgreSQL server from Python, hassle free.
"""

from __future__ import print_function
import sys
import os
import tempfile
import atexit
import subprocess
import shutil
from time import sleep

PY3 = (sys.version_info.major == 3)

input = input if PY3 else raw_input


class PiggyError(Exception):
    """Exception class for stuff that may go wrong while launching"""

    def __init__(self, msg):
        self.msg = msg

    def __str__(self):
        return self.msg


class Piggy():
    """
    On instantiation, launches a PostgreSQL instance.
    Without parameters, it will create a datadir in a temporary directory,
    and run postgres until you call terminate() or your program quits.
    """

    def __init__(self, extraPATHs=[], datadir=None, sockdir=None, listen_addresses='', port=5432, create_db='', keeprunning=False, volatile=False, extra_args=None):
        """
        @param extraPATHs: Extra paths to search for PostgreSQL binaries
        @param datadir: Directory to use as postgres datadir. Will be initialized if nonexistent or empty. If unspecified, a temporary directory will be used.
        @param sockdir: Store socket in this directory. Default: datadir.
        @param listen_addresses: Listen on these IP addresses (separate with commas). You'll need a pg_hba.conf for this to be useful. Default: do not create TCP socket at all.
        @param port: Which TCP port to listen on (only effective with listen_addresses specified, but it also determines the socket path).
        @param create_db: Create an actual database which you can later connect to.
        @param keeprunning: Don't terminate PostgreSQL server when your program does.
        @param volatile: Remove datadir and its contents after terminating the server.
        @param extra_args: Popen-list of extra arguments to start the PostgreSQL server server with.
        """

        self.cmd_postgres, self.cmd_initdb, self.cmd_createdb, self.cmd_psql = [self._which_panic(ex, extraPATHs) for ex in ('postgres', 'initdb', 'createdb', 'psql')]
        self.listen_addresses = listen_addresses
        self.port = port
        self.datadir = datadir if datadir else tempfile.mkdtemp()
        self.sockdir = sockdir if sockdir else self.datadir
        self.volatile = volatile
        self.extra_args = extra_args if isinstance(extra_args, list) else []
        self.params = {}

        self._initdb(self.datadir)
        self.postgres = self._startserver()
        self.params['host'] = self.sockdir
        self.params['port'] = str(self.port)

        if not keeprunning:
            atexit.register(self._cleanup, self.datadir, self.postgres, self.volatile)

        if create_db:
            try:
                self.createdb(create_db)
                self.params['dbname'] = create_db
            except PiggyError as pe:
                self._cleanup(self.datadir, self.postgres, self.volatile)
                raise pe

    def dsnstring(self):
        """
        Returns dsnstring for use in psycopg2.connect().
        If you didn't create your Piggy with the 'create_db'-parameter, 
        you'll need to add the "dbname='foo'"-part yourself.
        """
        return ' '.join(["%s='%s'" % (k, v) for (k, v) in self.params.items()])

    def uri(self):
        return "postgresql:///{dbname}?host={host}&port={port}".format(**self.params)

    def createdb(self, dbname):
        """On current running server, create db with name dbname"""
        try:
            subprocess.check_output([self.cmd_createdb, '-h', self.sockdir, '-p', str(self.port), dbname], stderr=subprocess.STDOUT)
        except(subprocess.CalledProcessError) as err:
            raise PiggyError('Failed to run createdb, complaint:\n%s' % _popen_out2str(err.output))

    def _startserver(self):
        """Start server. The ugly part is to determine when it's actually ready to serve requests."""
        pidfilepath = os.path.join(self.datadir, 'postmaster.pid')
        if os.path.isfile(pidfilepath):
            raise PiggyError('Failed to start server, datadir locked by %s' % pidfilepath)

        popenargs = [self.cmd_postgres, '--listen_addresses=%s' % self.listen_addresses, '--port=%d' % self.port, '-D', self.datadir, '-k', self.sockdir] + self.extra_args
        try:
            with open(os.devnull, 'w') as devnull:
                sockpath = os.path.join(self.sockdir, '.s.PGSQL.%d' % self.port)
                postgres = subprocess.Popen(popenargs, stdout=devnull, stderr=devnull, stdin=None)

                # Wait for socket to become ready, or subprocess to return unexpectedly
                while ((postgres.poll() == None) and (not os.access(sockpath, os.W_OK))):
                    sleep(0.1)

                # Did the server quit?
                if postgres.poll() != None:
                    raise PiggyError('Server quit unexpectedly')

                # Don't return until the server starts to accept connections
                while (subprocess.call([self.cmd_psql, '-h', self.sockdir, '-p', str(self.port), '-l'], stdout=devnull, stderr=devnull, stdin=None) != 0):
                    # Wait for DB to start accepting connections
                    sleep(0.1)

        except(subprocess.CalledProcessError) as err:
            raise PiggyError('Failed to start server, postgres complained:\n%s ' % _popen_out2str(err.output))
        return postgres

    def _initdb(self, datadir):
        """
        Creates a datadir for Postgres to run from.
        Assumption: if there's a datadir/postgresql.conf, then it's an already initialized datadir.
        If datadir is worthless, it's your problem - but _runserver will most probably raise an error in that case, so you'll notice.
        """
        if not os.path.isfile(os.path.join(datadir, 'postgresql.conf')):
            try:
                subprocess.check_output([self.cmd_initdb, '-E UTF8', datadir], stderr=subprocess.STDOUT)
            except(subprocess.CalledProcessError) as err:
                raise PiggyError('Failed to run initdb, complaint:\n%s ' % _popen_out2str(err.output))

    def _which_panic(self, *args, **kwargs):
        """Finds executable in PATH environment variable, raises an error if it can't be found"""
        thepath = self._which(*args, **kwargs)
        if not thepath:
            raise PiggyError('Could not locate "%s" binary' % args)
        else:
            return thepath

    def _which(self, executable, extrapaths=None):
        """Finds executable in PATH environment variable."""
        if not extrapaths: extrapaths = []
        for directory in extrapaths + os.environ['PATH'].split(os.path.pathsep):
            trypath = os.path.join(directory, executable)
            if os.path.isfile(trypath) and os.access(trypath, os.X_OK):
                return trypath

    def terminate(self):
        """Terminate this instance. Blocks while there are still connections to this database server."""
        self._cleanup(self.datadir, self.postgres, self.volatile)

    def _cleanup(self, datadir, postgres, volatile):
        """Stop server, also remove datadir if volatile=True."""
        try:
            # send 'fast' shutdown, eg, don't block while waiting for all connections to terminate.
            postgres.send_signal(2)
            postgres.wait()
            if volatile:
                shutil.rmtree(datadir, ignore_errors=True)
        except OSError:
            pass  # Already dead. Maybe someone used cleanup().


def _popen_out2str(popen_out):
    """Py3 popen() returns byte strings for program output. Convert that to strings."""
    return PY3 and popen_out.decode() or popen_out


def demo(dbname):
    pig = Piggy(volatile=True, create_db=dbname, port=4444)
    print("""
    This is a QuickPiggy demo run in ephemeral ("leave no traces") mode.
    Use the quickpiggy.Piggy class for integrating tuned behaviour into your Python code.
    Connecting to this demo instance:

        Python:
    
            psycopg2.connect("{dsnstring}")

        Shell:

            psql '{uri}'

    """.format(dsnstring=pig.dsnstring(), uri=pig.uri()))
    _ = input("Press Enter to terminate & clean up...")


def main():
    dbname = 'demo'
    if len(sys.argv) > 1:
        dbname = sys.argv[1]
    else:
        print("No database name argument passed. Using '%s'." % dbname)
    demo(dbname)


if __name__ == '__main__':
    main()
