import atexit
import http.server
import os
import pathlib
import pkg_resources
import random
import threading
import webbrowser

# required to get extern eggs on sys.path. This has to come first, before any
# modules that depend on them.
from . import extern

from . import environment
from . import infrastructure

from .infrastructure import Pipeline, Context

import pipeline.h
import pipeline.hif
import pipeline.hifa
import pipeline.hsd
import pipeline.hifv
import pipeline.hsdn

from .domain import measures
from casashell.private.stack_manip import find_frame

LOG = infrastructure.get_logger(__name__)

__pipeline_documentation_weblink_alma__ = "http://almascience.org/documents-and-tools/pipeline-documentation-archive"


WEBLOG_LOCK = threading.Lock()
HTTP_SERVER = None


def show_weblog(index_html=None,
                handler_class=http.server.SimpleHTTPRequestHandler,
                server_class=http.server.HTTPServer,
                bind='127.0.0.1'):
    """
    Locate the most recent web log and serve it via a HTTP server running on
    127.0.0.1 using a random port 30000-32768.

    The function arguments are not exposed in the CASA CLI interface, but are
    made available in case that becomes necessary.
    """
    global HTTP_SERVER

    if index_html is None:
        # find all t1-1.html files
        index_files = {p.name: p for p in pathlib.Path('.').rglob('t1-1.html')}
        if len(index_files) == 0:
            LOG.info('No weblog detected')
            return None

        # sort files by date, newest first
        by_date = sorted(pathlib.Path('.').rglob('t1-1.html'),
                         key=os.path.getmtime,
                         reverse=True)

        LOG.info('Found weblogs at:%s', ''.join([f'\n\t{p.name}' for p in by_date]))

        if len(index_files) > 1:
            LOG.info('Multiple web logs detected. Selecting most recent version')

        index_html = by_date[0]

    with WEBLOG_LOCK:
        if HTTP_SERVER is None:
            httpd = None
            while httpd is None:
                # multiple users concurrently viewing the weblog will each
                # need a different weblog port.
                # create a HTTP server on a port between 30000-32768
                port = random.randrange(30000, 32768)
                server_address = (bind, port)
                try:
                    httpd = server_class(server_address, handler_class)
                except OSError as e:
                    # Errno 48 port already taken
                    if e.errno == 48:
                        LOG.info('Port %s already in use. Selecting a different port...', port)
                    else:
                        raise

            sa = httpd.socket.getsockname()
            serve_message = 'Serving web log on {host} port {port} (http://{host}:{port}/) ...'
            LOG.info(serve_message.format(host=sa[0], port=sa[1]))

            thread = threading.Thread(target=httpd.serve_forever)
            thread.daemon = True
            thread.start()

            HTTP_SERVER = httpd

        else:
            sa = HTTP_SERVER.socket.getsockname()
            serve_message = 'Using existing HTTP server at {host} port {port} ...'
            LOG.info(serve_message.format(host=sa[0], port=sa[1]))

    server_root, _ = os.path.split(index_html)

    atexit.register(stop_weblog)

    url = 'http://{}:{}/{}'.format(bind, port, index_html)
    LOG.info('Opening {}'.format(url))
    webbrowser.open(url)


def stop_weblog():
    global HTTP_SERVER
    with WEBLOG_LOCK:

        if HTTP_SERVER is not None:
            sa = HTTP_SERVER.socket.getsockname()

            HTTP_SERVER.shutdown()

            serve_message = "HTTP server on {host} port {port} shut down"
            LOG.info(serve_message.format(host=sa[0], port=sa[1]))
            HTTP_SERVER = None


#def initcli():
#    print "Initializing cli..."
#    mypath = pkg_resources.resource_filename(__name__, '')
#    hifpath = mypath + "/hif/cli/hif.py"
#    hpath = mypath + "/h/cli/h.py"
#    hsdpath = mypath + "/hsd/cli/hsd.py"
#    hifapath = mypath + "/hifa/cli/hifa.py"
#    hifvpath = mypath + "/hifv/cli/hifv.py"
#    hsdnpath = mypath + "/hsdn/cli/hsdn.py"
#    myglobals = stack_frame_find()
#
#    execfile(hpath, myglobals)
#    execfile(hifpath, myglobals)
#    execfile(hsdpath, myglobals)
#    execfile(hifapath, myglobals)
#    execfile(hifvpath, myglobals)
#    execfile(hsdnpath, myglobals)
#    # exec('import pipeline.infrastructure.executeppr', myglobals)


def initcli():
    LOG.info('Initializing cli...')
    my_globals = find_frame()
    exec('from casashell import extra_task_modules', my_globals)
    for package in ['h', 'hif', 'hifa', 'hifv', 'hsd', 'hsdn']:
        abs_cli_package = 'pipeline.{package}.cli'.format(package=package)
        abs_gotasks_package = 'pipeline.{package}.cli.gotasks'.format(package=package)
        try:
            # Check the existence of the generated __init__ modules
            path_to_cli_init = pkg_resources.resource_filename(abs_cli_package, '__init__.py'.format(package))
            path_to_gotasks_init = pkg_resources.resource_filename(abs_gotasks_package, '__init__.py'.format(package))
        except ImportError as e:
            LOG.debug('Import error: {!s}'.format(e))
            LOG.info('No tasks found for package: {!s}'.format(package))
        else:
            # Instantiate the pipeline tasks for the given package
            exec('from {} import *'.format(abs_gotasks_package), my_globals)
            # Add the tasks to taskhelp()
            exec('import {} as {}_cli'.format(abs_cli_package, package), my_globals)
            exec('extra_task_modules.append({}_cli)'.format(package), my_globals)
            LOG.info('Loaded CASA tasks from package: {!s}'.format(package))


revision = environment.pipeline_revision


def log_host_environment():
    LOG.info('Pipeline version {!s} running on {!s}'.format(environment.pipeline_revision, environment.hostname))
    try:
        host_summary = '{!s} memory, {!s} x {!s} running {!s}'.format(
            measures.FileSize(environment.memory_size, measures.FileSizeUnits.BYTES),
            environment.logical_cpu_cores,
            environment.cpu_type,
            environment.host_distribution)

        LOG.info('Host environment: {!s}'.format(host_summary))
    except NotImplemented:
        pass


log_host_environment()

# FINALLY import executeppr. Do so as late as possible in pipeline module
# because executeppr make use of a part of pipeline module.
import pipeline.infrastructure.executeppr
