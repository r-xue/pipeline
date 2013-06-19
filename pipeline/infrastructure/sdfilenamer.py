from __future__ import absolute_import
import os
import shutil
import string
import pipeline.infrastructure.filenamer as filenamer

_valid_chars = "_.%s%s" % (string.ascii_letters, string.digits)    

class SDFileNameComponentBuilder(filenamer.FileNameComponentBuilder):
    def __init__(self):
        super(SDFileNameComponentBuilder, self).__init__()
        self._source = None
        self._antenna_name = None
        self._product = None
        self._sd = None
        self._output_dir = None
    
    def build(self):
        # The file names will be assembled using the order of the attributes 
        # given here
        attributes = (self._asdm,
                      self._source,
                      self._antenna_name,
                      self._product,
                      self._spectral_window,
                      self._polarization,
                      self._sd,
                      self._format)
        basename = '.'.join([filenamer.sanitize(x) for x in attributes 
                         if x not in ('', None)])

        if self._output_dir is not None:
            return os.path.join(self._output_dir, basename)
        else:
            return basename
        
    def asdm(self, uid):
        self._asdm = uid
        return self
    
    def source(self, source_name):
        self._source = source_name
        return self
    
    def antenna_name(self,antenna_name):
        self._antenna_name = antenna_name
        return self
    
    def spectral_window(self, window):
        if window not in [None, 'None', '']:
            self._spectral_window = 'spw' + str(window)
        else:
            self._spectral_window = None
        return self
    
    def product(self,product):
        self._product = product
        return self
    
    def polarization(self, polarization):
        self._polarization = polarization
        return self
    
    def sd(self,sd):
        self._sd = sd
        return self

class SDNamingTemplate(object):
    '''Base class used for all naming templates.
    '''
    dry_run = True
    
    def __init__(self):
        self._associations = SDFileNameComponentBuilder()
    
    def get_filename(self, delete=False):
        '''Assembles and returns the final filename.
        ''' 
        filename_components = self._associations.build()
        filename = '.'.join([filename_components])
    
        if delete and not SDNamingTemplate.dry_run:
            # .. and remove any old table with this name
            shutil.rmtree(filename, ignore_errors=True)

        return filename

    def output_dir(self, output_dir):
        '''Set the base output directory for this template
        '''
        self._associations.output_dir(output_dir)
        return self
    
    def __repr__(self):
        return self.get_filename()
    
class CalibrationTable(SDNamingTemplate):

    def __init__(self, other=None):
        super(CalibrationTable, self).__init__()
        self._associations.format('asap')
    
    def asdm(self, uid):
        '''Set the ASDM UID for this template, eg. uid://X03/XA83C/X02'''
        self._associations.asdm(uid)
        return self
    
    def antenna_name(self, antenna_name):
        self._associations.antenna_name(antenna_name)
        return self
    
    def sky_cal(self):
        self._associations.format('asap_sky')
        return self
        
    def tsys_cal(self):
        self._associations.format('asap_tsys')
        return self

class ProductTable(CalibrationTable):
    
    _extensions = ['product']
    def __init__(self, other=None):
        super(ProductTable, self).__init__()
        self._associations.format('product.tbl')
    
    def asdm(self, uid):
        '''Set the ASDM UID for this template, eg. uid://X03/XA83C/X02'''
        self._associations.asdm(uid)
        return self
    
    def antenna_name(self, antenna_name):
        self._associations.antenna_name(antenna_name)
        return self
    
    def spectral_window(self, window):
        self._associations.spectral_window(window)
        return self

class TsysCalibrationTable(CalibrationTable):
    def __init__(self, other=None):
        super(TsysCalibrationTable, self).__init__(other)
        self.tsys_cal()

class SkyCalibrationTable(CalibrationTable):
    def __init__(self, other=None):
        super(SkyCalibrationTable, self).__init__(other)
        self.sky_cal()

class CASALog(SDNamingTemplate):
    def __init__(self, other=None):
        super(CASALog, self).__init__()
        self._associations.extension('log')
        self._associations.format('txt')
        self._associations.type('casapy')

class Image(SDNamingTemplate):
    
    def __init__(self):
        super(Image, self).__init__()
    
    def source(self, source_name):
        self._associations.source(source_name)
        return self
    
    def antenna_name(self, antenna_name):
        self._associations.antenna_name(antenna_name)
        return self
    
    def spectral_window(self, window):
        self._associations.spectral_window(window)
        return self
    
    def polarization(self, polarization):
        self._associations.polarization(polarization)
        self._associations.sd('sd')
        return self
        
    def fits_image(self):
        self._associations.format('fits')
        return self
    
    def casa_image(self):
        self._associations.format('im')
        return self
