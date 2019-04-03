from __future__ import absolute_import

import os
import shutil

import pipeline.infrastructure as infrastructure
import pipeline.infrastructure.basetask as basetask
import pipeline.infrastructure.vdp as vdp
import pipeline.infrastructure.casatools as casatools
import pipeline.infrastructure.imagelibrary as imagelibrary
from pipeline.infrastructure import casa_tasks
from .resultobjects import SDImagingResultItem

LOG = infrastructure.get_logger(__name__)


class SDImageCombineInputs(vdp.StandardInputs):
    """
    Inputs for image plane combination
    """
    inimages = vdp.VisDependentProperty(default='')
    outfile = vdp.VisDependentProperty(default='')
    
    @inimages.convert
    def inimages(self, value):
        if isinstance(value, str):
            _check_image(value)
        else:
            for v in value:
                _check_image(v)
        return value
    
    def __init__(self, context, inimages, outfile):
        super(SDImageCombineInputs, self).__init__()

        self.context = context
        self.inimages = inimages
        self.outfile = outfile


class SDImageCombine(basetask.StandardTaskTemplate):
    Inputs = SDImageCombineInputs
    
    is_multi_vis_task = True

    def prepare(self):
        infiles = self.inputs.inimages
        outfile = self.inputs.outfile
        inweights = [name+".weight" for name in infiles]
        outweight = outfile + ".weight"
        num_in = len(infiles)
        if num_in == 0:
            LOG.warning("No input image to combine. %s is not generated." % outfile)
            result = SDImagingResultItem(task=None,
                                         success=False, outcome=None)
            return result

        # combine weight images
        LOG.info("Generating combined weight image.")
        expr = [ ("IM%d" % idx) for idx in range(num_in) ]
        status = self._do_combine(inweights, outweight, str("+").join(expr))
        if status is True:
            # combine images with weight
            LOG.info("Generating combined image.")
            in_images = list(infiles) + list(inweights) + [outweight]
            expr = [ "IM%d*IM%d" % (idx, idx+num_in) for idx in range(num_in) ]
            expr = "(%s)/IM%d" % (str("+").join(expr), len(in_images)-1)
            status = self._do_combine(in_images, outfile, expr)

        if status is True:
            # Need to replace NaNs in masked pixels
            with casatools.ImageReader(outfile) as ia:
                stat = ia.statistics()
                shape = ia.shape()
                # replacemaskedpixels fails if all pixels are valid
                if len(stat['npts']) > 0 and shape.prod() > stat['npts'][0]:
                    ia.replacemaskedpixels(0.0, update=False)

            image_item = imagelibrary.ImageItem(imagename=outfile,
                                                sourcename='', # will be filled in later
                                                spwlist=[],  # will be filled in later
                                                specmode='cube',
                                                sourcetype='TARGET')
            outcome = {'image': image_item}
            result = SDImagingResultItem(task=None,
                                         success=True,
                                         outcome=outcome)
        else:
            # Combination failed due to missing valid data
            result = SDImagingResultItem(task=None,
                                         success=False,
                                         outcome=None)

        if self.inputs.context.subtask_counter is 0: 
            result.stage_number = self.inputs.context.task_counter - 1
        else:
            result.stage_number = self.inputs.context.task_counter

        return result

    def analyse(self, result):
        return result

    def _do_combine(self, infiles, imagename, expr):
        if os.path.exists(imagename):
            shutil.rmtree(imagename)
        combine_args = dict(imagename=infiles, outfile=imagename,
                            mode='evalexpr', expr=expr)
        LOG.debug('Executing immath task: args=%s'%(combine_args))
        combine_job = casa_tasks.immath(**combine_args)

        # execute job
        self._executor.execute(combine_job)

        return True
    
def _check_image(imagename):
    assert os.path.exists(imagename), 'Input image "{0}" does not exist.'.format(imagename)
    assert os.path.exists(imagename.rstrip('/') + '.weight'), 'Weight image for "{0}" does not exist.'.format(imagename)
    
