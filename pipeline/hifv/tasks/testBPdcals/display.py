
import pipeline.infrastructure as infrastructure
from pipeline.hifv.tasks.common.displays import display as baseDisplay

LOG = infrastructure.get_logger(__name__)


class SummaryChart(baseDisplay.SummaryChart):
    def __init__(self, context, result, spw='', suffix='', taskname=None):
        super().__init__(context, result, spw=spw, suffix=suffix, taskname=taskname)

    def plot(self):
        plots = super().plot()
        return plots

    def create_plot(self, prefix=''):
        super().create_plot(prefix)

    def get_figfile(self, prefix=''):
        filename = super().get_figfile(prefix)
        return filename

    def get_plot_wrapper(self, prefix=''):
        wrapper = super().get_plot_wrapper(prefix)
        return wrapper


class AntennaChart(baseDisplay.PerAntennaChart):
    def __init__(self, context, result, suffix='', taskname=None, plottype=None, perSpwChart=False):
        super().__init__(context, result, suffix, taskname, plottype, perSpwChart)

    def plot(self):
        plots = super().plot()
        return plots
