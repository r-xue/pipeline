import os

from casatasks import casalog

from pipeline.infrastructure import casa_tools


# Notes
#    It is not clear what field is for. It is not selected for in the
#    caltable computation, but it is used to computed channel ranges
#    Leave in place for now
def uvcontfit(vis=None, caltable=None, field=None, intent=None, spw=None, combine=None, solint=None, fitorder=None,
              append=None):

    # Python script
    casalog.origin('uvcontfit')

    # Get instance of the 'calibrater' CASA tool.
    mycb = casa_tools.calibrater

    # Run normal code
    try:
        # Determine the channels to be used in the fit
        #    Not sure why this was needed but leave code in place for now. What is wrong with frequency ranges ?
        # if spw.count('Hz'):
        #     locfitspw = _new_quantityRangesToChannels(vis,field,fitspw,False)
        # else:
        #     locfitspw=spw
        locfitspw = spw

        if isinstance(vis, str) and os.path.isdir(vis):
            mycb.setvi(old=True, quiet=False)
            mycb.open(filename=vis, compress=False, addcorr=False, addmodel=False)
        else:
            raise Exception('Visibility data set not found - please verify the name')

        # Select the data for continuum subtraction
        #   Intent forces the selection to be on TARGET data only
        #   Field is needed because the continuum regions will be different for different target fields
        #   Spw selection will include an spw and frequency change   
        mycb.reset()
        mycb.selectvis(field=field, intent=intent, spw=locfitspw)

        # Add append parameter because it may be needed to deal with data sets with multiple
        # targets.
        if not combine:
            mycombine = ''
        else:
            mycombine = combine
        mycb.setsolve(type='A', t=solint, table=caltable, combine=mycombine, fitorder=fitorder, append=append)

        # Solve for the continuum
        mycb.solve()

    except Exception as instance:
        casalog.post('Error in uvcontfit: ' + str(instance), 'SEVERE')
        raise Exception('Error in uvcontfit: ' + str(instance))
    finally:
        # Ensure that the calibrator tool gets closed.
        mycb.close()
