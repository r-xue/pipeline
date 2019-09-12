import string
import glob
import os

import pipeline.infrastructure.casatools as casatools
import pipeline.infrastructure as infrastructure

# the logger for this module
LOG = infrastructure.get_logger(__name__)


def generate_template(filename):
    with open(filename, 'r') as f:
        txt = f.read()

    template = string.Template(txt)
    return template


def export_template(filename, txt):
    with open(filename, 'w') as f:
        f.write(txt)


def indent(level=0):
    return '    ' * level


def space(n=0):
    return ' ' * n


def get_template():
    script_dir = os.path.dirname(os.path.abspath(__file__))
    script_name = 'template.txt'
    return os.path.join(script_dir, script_name)


def generate(context, scriptname):
    tmp = get_template()
    template = generate_template(tmp)

    myms = context.observing_run.measurement_sets[0]
    spws = [s.id for s in myms.get_spectral_windows(science_windows_only=True)]
    nspw = len(spws)

    # processing flags
    processspw = '\n'.join([indent(level=3) + 'True' + indent(level=3) + '# SPW{}'.format(i) for i in spws])
    processspw = processspw.replace('True ', 'True,', nspw - 1)

    # baseline masks
    blrange = '\n'.join([indent(level=5) + "''" + indent(level=2) + '# baseline_range for spw{}'.format(i) for i in spws])
    blrange = blrange.replace("'' ", "'',", nspw - 1)

    # rest frequencies
    images = glob.glob('*.spw*.cube.I.iter*.image.sd')
    rest_freqs = ['' for _ in spws]
    nchan = 0
    cell = []
    imsize = []
    phasecenter = ''
    qa = casatools.quanta
    for image in images:
        s = image.split('.')[-6]
        i = int(s.replace('spw', ''))
        if i in spws:
            index = spws.index(i)
            with casatools.ImageReader(image) as ia:
                coordsys = ia.coordsys()
                try:
                    imshape = ia.shape()
                    imsize = list(imshape[:2])
                    nchan = imshape[coordsys.findaxisbyname('Spectral')]
                    rest_freq = coordsys.restfrequency()
                    increments = coordsys.increment()['numeric']
                    units = coordsys.units()
                    _cell = map(lambda x: qa.convert(qa.quantity(abs(x[0]), x[1]), 'arcsec'), (x for x in zip(increments[:2], units[:2])))
                    cell = map(lambda x: '{value}{unit}'.format(**x), _cell)
                    refcode = coordsys.referencecode()[0]
                    dummy = [0] * len(imshape)
                    dummy[0] = float(imshape[0]) / 2
                    dummy[1] = float(imshape[1]) / 2
                    world = coordsys.toworld(dummy, format='s')['string']
                    # Coordinate values may be in numerical value with unit
                    # instead of HMS/DMS format. Even so, leave the values as
                    # they are to keep precision
                    lon = world[0].replace(' ', '')
                    lat = world[1].replace(' ', '')
                    phasecenter = "'{} {} {}'".format(refcode, lon, lat)
                finally:
                    coordsys.done()

            if 'unit' in rest_freq and 'value' in rest_freq:
                rest_freqs[index] = '{}{}'.format(rest_freq['value'][0], rest_freq['unit'])

    restfreqs = '\n'.join([indent(level=5) + "'{}'".format(f) + indent(level=1) + '# Rest frequency of SPW{}'.format(i) for f, i in zip(rest_freqs, spws)])
    restfreqs = restfreqs.replace("' ", "',", nspw - 1)

    vis = myms.basename
    antennalist = [a.id for a in myms.antennas]
    source = myms.get_fields(intent='TARGET')[0].clean_name

    s = template.safe_substitute(processspw=processspw,
                                 baselinerange=blrange,
                                 restfreqs=restfreqs,
                                 nchan=nchan,
                                 cell=cell,
                                 phasecenter=phasecenter,
                                 imsize=imsize,
                                 vis=vis,
                                 antennalist=antennalist,
                                 source=source)

    export_template(scriptname, s)

    return os.path.exists(scriptname)
