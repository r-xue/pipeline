
import pipeline.infrastructure as infrastructure
from pipeline.hifv.tasks.testBPdcals import baseDisplay

LOG = infrastructure.get_logger(__name__)


class semifinalBPdcalsSummaryChart(baseDisplay.SummaryChart):
    def __init__(self, context, result, suffix='', taskname=None):
        super().__init__(context, result, suffix, taskname)

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


class semifinalBPdcalsSpwSummaryChart(baseDisplay.PerSpwSummaryChart):
    def __init__(self, context, result, spw=None, suffix='', taskname=None):
        super().__init__(context, result, spw=spw, suffix=suffix, taskname=taskname)

    def plot(self):
        plots = super().plot()
        return plots

    def create_plot(self,  prefix=''):
        super().create_plot(prefix)

    def get_figfile(self, prefix=''):
        filename = super().get_figfile(prefix)
        return filename

    def get_plot_wrapper(self, prefix=''):
        wrapper = super().get_plot_wrapper(prefix)
        return wrapper


class DelaysPerAntennaChart(baseDisplay.PerAntennaChart):
    def __init__(self, context, result, suffix='', taskname=None, plottype=None, perSpwChart=False):
        super().__init__(context, result, suffix, taskname, plottype, perSpwChart)

    def plot(self):
        plots = super().plot()
        return plots


class semifinalphaseGainPerAntennaChart(baseDisplay.PerAntennaChart):
    def __init__(self, context, result, suffix='', taskname=None, plottype=None, perSpwChart=False):
        super().__init__(context, result, suffix, taskname, plottype, perSpwChart)

    def plot(self):
        plots = super().plot()
        return plots


class semifinalbpSolAmpPerAntennaChart(baseDisplay.PerAntennaChart):
    def __init__(self, context, result, suffix='', taskname=None, plottype=None, perSpwChart=False):
        super().__init__(context, result, suffix, taskname, plottype, perSpwChart)

    def plot(self):
        plots = super().plot()
        return plots


class semifinalbpSolPhasePerAntennaChart(baseDisplay.PerAntennaChart):
    def __init__(self, context, result, suffix='', taskname=None, plottype=None, perSpwChart=False):
        super().__init__(context, result, suffix, taskname, plottype, perSpwChart)

    def plot(self):
        plots = super().plot()
        return plots


class semifinalbpSolAmpPerAntennaPerSpwChart(baseDisplay.PerAntennaChart):
    def __init__(self, context, result, suffix='', taskname=None, plottype=None, perSpwChart=True):
        super().__init__(context, result, suffix, taskname, plottype, perSpwChart)

    def plot(self):
        plots = super().plot()
        return plots


class semifinalbpSolPhasePerAntennaPerSpwChart(baseDisplay.PerAntennaChart):
    def __init__(self, context, result, suffix='', taskname=None, plottype=None, perSpwChart=True):
        super().__init__(context, result, suffix, taskname, plottype, perSpwChart)

    def plot(self):
        plots = super().plot()
        return plots
