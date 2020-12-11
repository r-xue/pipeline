import html
import itertools
import os
from typing import List, Optional

import numpy as np

import pipeline.infrastructure as infrastructure
import pipeline.infrastructure.logging as logging
import pipeline.infrastructure.utils as utils
from pipeline.infrastructure import casa_tools
from pipeline.infrastructure.pipelineqa import QAScore, WebLogLocation

LOG = infrastructure.get_logger(__name__)

SCORE_THRESHOLD_ERROR = 0.33
SCORE_THRESHOLD_WARNING = 0.66
SCORE_THRESHOLD_SUBOPTIMAL = 0.9


def printTsysFlags(tsystable, htmlreport):
    """Method that implements a version of printTsysFlags by Todd Hunter.
    """
    with casa_tools.TableReader(tsystable) as mytb:
        spws = mytb.getcol("SPECTRAL_WINDOW_ID")

    with casa_tools.TableReader(tsystable+"/ANTENNA") as mytb:
        ant_names = mytb.getcol("NAME")

    with open(htmlreport, 'w') as stream:
        stream.write('<html>')

        with casa_tools.TableReader(tsystable) as mytb:
            for iant in range(len(ant_names)):
                for spw in np.unique(spws):

                    # select rows from table for specified antenna and spw
                    zseltb = mytb.query("SPECTRAL_WINDOW_ID=={0} && ANTENNA1=={1}".format(spw, iant))
                    try:
                        flags = zseltb.getcol("FLAG")
                        times = zseltb.getcol("TIME")
                        fields = zseltb.getcol("FIELD_ID")
                    finally:
                        zseltb.close()

                    npol = len(flags)
                    nchan = len(flags[0])
                    uniqueTimes = np.unique(times)

                    for pol in range(npol):
                        for t in range(len(times)):
                            zflag = np.where(flags[pol, :, t])[0]

                            if len(zflag) > 0:
                                if len(zflag) == nchan:
                                    chans = 'all channels'
                                else:
                                    chans = '%d channels: ' % (
                                      len(zflag)) + str(zflag)

                                time_index = list(uniqueTimes).index(times[t])
                                myline = ant_names[iant] + \
                                  " (#%02d), field %d, time %d, pol %d, spw %d, "%(
                                  iant, fields[t], time_index, pol, spw) + \
                                  chans + "<br>\n"

                                stream.write(myline)

                # format break between antennas
                stream.write('<br>\n')


def renderflagcmds(flagcmds, htmlflagcmds):
    """Method to render a list of flagcmds into html format.
    """
    lines = []
    for flagcmd in flagcmds:
        lines.append(flagcmd.flagcmd)

    with open(htmlflagcmds, 'w') as stream:
        stream.write('<html>')
        stream.write('<head/>')
        stream.write('<body>')
        stream.write('''This is the list of flagcmds created by this stage.
          <br>''')

        for line in lines:
            stream.write('%s<br>' % line)
        stream.write('</body>')
        stream.write('</html>')


def get_bar_class(pqascore):
    score = pqascore.score
    if score in (None, '', 'N/A'):
        return ''
    elif score <= SCORE_THRESHOLD_ERROR:
        return ' progress-bar-danger'
    elif score <= SCORE_THRESHOLD_WARNING:
        return ' progress-bar-warning'
    elif score <= SCORE_THRESHOLD_SUBOPTIMAL:
        return ' progress-bar-info'
    else:
        return ' progress-bar-success'


def get_badge_class(pqascore):
    score = pqascore.score
    if score in (None, '', 'N/A'):
        return ''
    elif score <= SCORE_THRESHOLD_ERROR:
        return ' alert-danger'
    elif score <= SCORE_THRESHOLD_WARNING:
        return ' alert-warning'
    elif score <= SCORE_THRESHOLD_SUBOPTIMAL:
        return ' alert-info'
    else:
        return ' alert-success'


def get_bar_width(pqascore):
    if pqascore.score in (None, '', 'N/A'):
        return 0
    else:
        return 5.0 + 95.0 * pqascore.score


def format_score(pqascore):
    if pqascore.score in (None, '', 'N/A'):
        return 'N/A'
    return '%0.2f' % pqascore.score


def get_sidebar_style_for_task(result):
    if all(qa_score.score in (None, '', 'N/A') for qa_score in result.qa.pool):
        return 'text-muted'
    return 'text-dark'


def get_symbol_badge(result):
    if get_failures_badge(result):
        symbol = '<span class="glyphicon glyphicon-minus-sign alert-danger transparent-bg" aria-hidden="true"></span>'
    elif get_errors_badge(result):
        symbol = '<span class="glyphicon glyphicon-remove-sign alert-danger transparent-bg" aria-hidden="true"></span>'
    elif get_warnings_badge(result):
        symbol = '<span class="glyphicon glyphicon-exclamation-sign alert-warning transparent-bg" aria-hidden="true"></span>'
    elif get_suboptimal_badge(result):
        symbol = '<span class="glyphicon glyphicon-question-sign alert-info transparent-bg" aria-hidden="true"></span>'
    else:
        return '<span class="glyphicon glyphicon-none" aria-hidden="true"></span>'
    return symbol


def get_failures_badge(result):
    failure_tracebacks = utils.get_tracebacks(result)
    n = len(failure_tracebacks)
    if n > 0:
        return '<span class="badge alert-important pull-right">%s</span>' % n
    else:
        return ''


def get_warnings_badge(result):
    warning_logrecords = utils.get_logrecords(result, logging.WARNING)
    warning_qascores = utils.get_qascores(result, SCORE_THRESHOLD_ERROR, SCORE_THRESHOLD_WARNING)
    l = len(warning_logrecords) + len(warning_qascores)
    if l > 0:
        return '<span class="badge alert-warning pull-right">%s</span>' % l
    else:
        return ''


def get_errors_badge(result):
    error_logrecords = utils.get_logrecords(result, logging.ERROR)
    error_qascores = utils.get_qascores(result, -0.1, SCORE_THRESHOLD_ERROR)
    l = len(error_logrecords) + len(error_qascores)
    if l > 0:
        return '<span class="badge alert-important pull-right">%s</span>' % l
    else:
        return ''


def get_suboptimal_badge(result):
    suboptimal_qascores = utils.get_qascores(result, SCORE_THRESHOLD_WARNING, SCORE_THRESHOLD_SUBOPTIMAL)
    l = len(suboptimal_qascores)
    if l > 0:
        return '<span class="badge alert-info pull-right">%s</span>' % l
    else:
        return ''


def get_command_markup(ctx, command):
    if not command:
        return ''
    stripped = command.replace('%s/' % ctx.report_dir, '')
    stripped = stripped.replace('%s/' % ctx.output_dir, '')
    escaped = html.escape(stripped, True).replace('\'', '&#39;')
    return escaped


def format_shortmsg(pqascore):
    # First check against None. Comparisons of None and float are no longer
    # allowed in Python 3.
    if pqascore.score is None:
        return pqascore.shortmsg
    if pqascore.score > SCORE_THRESHOLD_SUBOPTIMAL:
        return ''
    else:
        return pqascore.shortmsg


def sort_row_by(row, axes):
    # build primary, secondary, tertiary, etc. axis sorting functions
    def f(axis):
        def g(plot):
            return plot.parameters.get(axis, '')
        return g

    # create a parameter getter for each axis
    accessors = [f(axis) for axis in axes.split(',')]

    # sort plots in row, using a generated tuple (p1, p2, p3, ...) for
    # secondary sort
    return sorted(row, key=lambda plot: tuple([fn(plot) for fn in accessors]))


def group_plots(data, axes):
    if data is None:
        return []

    # build primary, secondary, tertiary, etc. axis sorting functions
    def f(axis):
        def g(plot):
            return plot.parameters.get(axis, '')
        return g

    keyfuncs = [f(axis) for axis in axes.split(',')]
    return _build_rows([], data, keyfuncs)


def _build_rows(rows, data, keyfuncs):
    # if this is a leaf, i.e., we are in the lowest level grouping and there's
    # nothing further to group by, add a new row
    if not keyfuncs:
        rows.append(data)
        return

    # otherwise, this is not the final sorting axis and so proceed to group
    # the results starting with the first (or next) axis...
    keyfunc = keyfuncs[0]
    data = sorted(data, key=keyfunc)
    for group_value, items_with_value_generator in itertools.groupby(data, keyfunc):
        # convert to list so we don't exhaust the generator
        items_with_value = list(items_with_value_generator)
        # ... , creating sub-groups for each group as we go
        _build_rows(rows, items_with_value, keyfuncs[1:])

    return rows


def sanitize_data_selection_string(text):
    split_text = utils.safe_split(text)
    sanitized_text = "[{}]".format(", ".join(["&quot;{}&quot;".format(field) for field in split_text]))
    return sanitized_text


def num_lines(path):
    """
    Report number of non-empty non-comment lines in a file specified by the
    path argument. If the file does not exist, report N/A.
    """
    if os.path.exists(path):
        return sum(1 for line in open(path) if line.strip() and not line.startswith('#'))
    else:
        return 'N/A'


def scores_in_range(pool: List[QAScore], lo: float, hi: float) -> List[QAScore]:
    """
    Filter QA scores by range.
    """
    return [score for score in pool
            if score.score not in ('', 'N/A', None)
            and lo < score.score <= hi]


def scores_with_location(pool: List[QAScore],
                         locations: Optional[List[WebLogLocation]] = None) -> List[QAScore]:
    """
    Filter QA scores by web log location.
    """
    if not locations:
        locations = list(WebLogLocation)

    return [score for score in pool if score.weblog_location in locations]


def get_notification_trs(result, alerts_info, alerts_success):
    # suppress scores not intended for the banner, taking care not to suppress
    # legacy scores with a default message destination (=UNSET) so that old
    # tasks continue to render as before
    all_scores: List[QAScore] = result.qa.pool
    banner_scores = scores_with_location(all_scores, [WebLogLocation.BANNER, WebLogLocation.UNSET])

    notifications = []

    if banner_scores:
        for qa_score in scores_in_range(banner_scores, -0.1, SCORE_THRESHOLD_ERROR):
            n = format_notification('danger alert-danger', 'QA', qa_score.longmsg, 'glyphicon glyphicon-remove-sign')
            notifications.append(n)
        for qa_score in scores_in_range(banner_scores, SCORE_THRESHOLD_ERROR, SCORE_THRESHOLD_WARNING):
            n = format_notification('warning alert-warning', 'QA', qa_score.longmsg, 'glyphicon glyphicon-exclamation-sign')
            notifications.append(n)

    for logrecord in utils.get_logrecords(result, logging.ERROR):
        n = format_notification('danger alert-danger', 'Error!', logrecord.msg)
        notifications.append(n)
    for logrecord in utils.get_logrecords(result, logging.WARNING):
        n = format_notification('warning alert-warning', 'Warning!', logrecord.msg)
        notifications.append(n)

    if alerts_info:
        for msg in alerts_info:
            n = format_notification('info alert-info', '', msg)
            notifications.append(n)
    if alerts_success:
        for msg in alerts_success:
            n = format_notification('success alert-success', '', msg)
            notifications.append(n)

    return notifications


def format_notification(tr_class, alert, msg, icon_class=None):
    if icon_class:
        icon = '<span class="%s"></span> ' % icon_class
    else:
        icon = ''
    return '<tr class="%s"><td>%s<strong>%s</strong> %s</td></tr>' % (tr_class, icon, alert, msg)
