from __future__ import absolute_import

import pipeline.infrastructure as infrastructure
import pipeline.infrastructure.basetask as basetask
import pipeline.infrastructure.imagelibrary as imagelibrary
import pipeline.infrastructure.vdp as vdp
from pipeline.infrastructure import casa_tasks
from pipeline.infrastructure import task_registry

LOG = infrastructure.get_logger(__name__)


class PbcorResults(basetask.Results):
    def __init__(self, final=[], pool=[], preceding=[], pbcorimagelist=[], pbcorimagenames=[]):
        super(PbcorResults, self).__init__()
        self.pool = pool[:]
        self.final = final[:]
        self.preceding = preceding[:]
        self.pbcorimagelist = pbcorimagelist[:]
        self.pbcorimagenames = pbcorimagenames[:]
        self.error = set()

    def merge_with_context(self, context):
        """
        See :method:`~pipeline.infrastructure.api.Results.merge_with_context`
        """
        # if not self.pbcorimagenames:
        #     LOG.warn('No makepbcorimages results')
        #     return

        # pbcorimagelist is a list of dictionaries
        # Use the same format and information from sciimlist, save for the image name and image plot

        for pbcoritem in self.pbcorimagelist:
            try:
                imgname = pbcoritem['imagename']
                imageitem = imagelibrary.ImageItem(imagename=imgname[:imgname.rfind('.image')]+'.pb.tt0',
                                                   sourcename=pbcoritem['sourcename'],
                                                   spwlist=pbcoritem['spwlist'],
                                                   specmode=pbcoritem['specmode'],
                                                   sourcetype=pbcoritem['sourcetype'],
                                                   multiterm=pbcoritem['multiterm'],
                                                   imageplot=pbcoritem['imageplot'])
                context.pbcorimlist.add_item(imageitem)
                if 'TARGET' in pbcoritem['sourcetype']:
                    LOG.info('Added Image Item')
                    context.pbcorimlist.add_item(imageitem)
            except Exception as ex:
                LOG.debug(str(ex))
                pass

    def __repr__(self):
        # return 'PbcorResults:\n\t{0}'.format(
        #    '\n\t'.join([ms.name for ms in self.mses]))
        return 'PbcorResults:'


class PbcorInputs(vdp.StandardInputs):
    def __init__(self, context, vis=None):
        super(PbcorInputs, self).__init__()
        self.context = context
        self.vis = vis


@task_registry.set_equivalent_casa_task('hifv_pbcor')
class Pbcor(basetask.StandardTaskTemplate):
    Inputs = PbcorInputs

    def prepare(self):

        imlist = self.inputs.context.sciimlist.get_imlist()
        pbcor_list = []
        for image in imlist:
            imgname = image['imagename']
            outname = imgname+'.pbcor.tt0'
            pbname = imgname[:imgname.rfind('.image')]+'.pb.tt0'
            task = casa_tasks.impbcor(imagename=imgname+'.tt0', pbimage=pbname,
                                      outfile=outname, mode='divide', cutoff=-1.0, stretch=False)
            self._executor.execute(task)
            pbcor_list.append(outname)

            pbcor_list.append(pbname)

            outname = imgname+'.residual.pbcor.tt0'
            task = casa_tasks.impbcor(imagename=imgname[:imgname.rfind('.image')]+'.residual.tt0', pbimage=pbname,
                                      outfile=outname, mode='divide', cutoff=-1.0, stretch=False)
            self._executor.execute(task)
            pbcor_list.append(outname)

            LOG.info("PBCOR image names: " + ','.join(pbcor_list))

        return PbcorResults(pbcorimagenames=pbcor_list)

    def analyse(self, results):
        return results



