###
###  Make a PB
###  - MS and selections
###  - Defineimage (try to just reuse coordinatesystem)
###  - im.makePB

#!/usr/bin/env python

import os;
import shutil;
from  casac import *;
from tasks import imregrid

def makePB(vis='',field='',spw='',timerange='',uvrange='',antenna='',observation='',intent='',scan='', imtemplate='',outimage='',pblimit=0.2):
    
    """ Make a PB image using the imager tool, onto a specified image coordinate system 

         This function can be used along with tclean to make .pb images for gridders that
         do not already do it (i.e. other than mosaic, awproject)

         This script takes an image to use as a template coordinate system, 
         attempts to set up an identical coordinate system with the old imager tool, 
         makes a PB for the telescope listed in the MS observation subtable, and 
         regrids it (just in case) to the target coordinate system). This can be used for
         single fields and mosaics.

    """

    tb = casac.table()
    im = casac.imager()
    ia = casac.image()
    me = casac.measures()
    qa = casac.quanta()

    print 'MAKEPB : Making a PB image using the imager tool'

    tb.open(vis+'/OBSERVATION')
    tel = tb.getcol('TELESCOPE_NAME')[0]
    tb.close()

    print 'MAKEPB : Making PB for ', tel

    ia.open(imtemplate)
    csysa = ia.coordsys()
    csys = csysa.torecord()
    shp = ia.shape()
    ia.close()
    stokes = 'I'
    dirs = csys['direction0']
    phasecenter = me.direction(dirs['system'], qa.quantity(dirs['crval'][0],dirs['units'][0]) , qa.quantity(dirs['crval'][1],dirs['units'][1]) )
    cellx=qa.quantity(dirs['cdelt'][0],dirs['units'][0])
    celly=qa.quantity(dirs['cdelt'][1],dirs['units'][1])
    nchan=shp[3]
    start=qa.quantity( csysa.referencevalue()['numeric'][3], csysa.units()[3] )  ## assumes refpix is zero
    step=qa.quantity( csysa.increment()['numeric'][3], csysa.units()[3] )

    print 'MAKEPB : Starting imager tool'

    im.open(vis)
    im.selectvis(field=field,spw=spw,time=timerange,intent=intent,scan=scan,uvrange=uvrange,baseline=antenna,observation=observation)
    im.defineimage(nx=shp[0],ny=shp[0],phasecenter=phasecenter,cellx=qa.tos(cellx),celly=qa.tos(celly),nchan=nchan,start=start,step=step)
    im.setvp(dovp=True,telescope=tel)
    im.makeimage(type='pb',image=outimage+'.tmp')
    im.close()

    print 'MAKEPB : Regrid to desired coordinate system'
    
    imregrid(imagename=outimage+'.tmp', template=imtemplate,output=outimage,overwrite=True,asvelocity=False)

    shutil.rmtree(outimage+'.tmp')

    print 'MAKEPB : Set mask to pblimit'

    ia.open(outimage)
    ia.calcmask('"%s" > %s' % (outimage, str(pblimit)))
    ia.close()

