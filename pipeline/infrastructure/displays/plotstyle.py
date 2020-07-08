import matplotlib.pyplot as plt
import matplotlib.style

def casa5style_plot(func):
    style_dict = {
        'lines.linewidth' : 1.0,
        'lines.markeredgewidth' : 0.5,
        'lines.dash_joinstyle' : 'miter',
        'lines.solid_joinstyle' : 'miter',
        'font.size' : 12.0,
        'axes.linewidth' : 1.0,
        'axes.titlepad' : 4.0,
        'axes.labelpad' : 2.0,
        'figure.figsize' : [8, 6],
        'figure.dpi' : 80,
        'figure.subplot.bottom' : 0.1,
        'figure.subplot.top' : 0.9,
        'savefig.dpi' : 100
    }
    def wrapper( *args, **kwargs ):
        style = [ 'classic', style_dict ]
        with plt.style.context( style ):
            return func( *args, **kwargs )
    return wrapper
