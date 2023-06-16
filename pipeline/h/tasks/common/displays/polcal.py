from . import applycal


# TODO: move parent chart to common module?
class AmpVsParangSummaryChart(applycal.SpwSummaryChart):
    """
    Plotting class that creates an amplitude vs. parallactic angle plot for
    each spw.
    """
    def __init__(self, context, output_dir, calto, **overrides):
        plot_args = {
            'ydatacolumn': 'data',
            'correlation': 'XX,YY',
            'averagedata': True,
            'avgchannel': '9999',
            'avgbaseline': True,
            'plotrange': [0, 0, 0, 0],
            'coloraxis': 'corr',
            'overwrite': True
        }
        plot_args.update(**overrides)

        super().__init__(context, output_dir, calto, xaxis='parang', yaxis='amp',
                         intent='POLARIZATION,POLANGLE,POLLEAKAGE', **plot_args)
