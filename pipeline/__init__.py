import atexit
import http.server
import os
import pkg_resources
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


def show_weblog(context, handler_class=http.server.SimpleHTTPRequestHandler,
                server_class=http.server.HTTPServer,
                protocol='HTTP/1.0', bind='127.0.0.1', port=8000):
    """
    This runs an HTTP server on 127.0.0.1:8000 (or the port argument).
    """
    global HTTP_SERVER

    server_address = (bind, port)

    with WEBLOG_LOCK:
        if HTTP_SERVER is None:
            # handler_class.protocol_version = protocol
            httpd = server_class(server_address, handler_class)

            sa = httpd.socket.getsockname()
            serve_message = 'Serving HTTP on {host} port {port} (http://{host}:{port}/) ...'
            LOG.info(serve_message.format(host=sa[0], port=sa[1]))

            thread = threading.Thread(target=httpd.serve_forever)
            thread.daemon = True
            thread.start()

            HTTP_SERVER = httpd

        else:
            sa = HTTP_SERVER.socket.getsockname()
            serve_message = 'Using existing HTTP server at {host} port {port} ...'
            LOG.info(serve_message.format(host=sa[0], port=sa[1]))

    index_html = os.path.join(context.report_dir, 't1-1.html')
    rel_index = os.path.relpath(index_html, context.output_dir)

    atexit.register(stop_weblog)

    url = 'http://{}:{}/{}'.format(bind, port, rel_index)
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
