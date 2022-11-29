##################### generated by xml-casa (v2) from uvcontfit.xml #################
##################### 5a367cdd5124dad67c3b70eafd7d5465 ##############################
from __future__ import absolute_import
import numpy
from casatools.typecheck import CasaValidator as _val_ctor
_pc = _val_ctor( )
from casatools.coercetype import coerce as _coerce
from casatools.errors import create_error_string
#from .uvcontfit import uvcontfit as _uvcontfit_t
from casatasks.private.task_logging import start_log as _start_log
from casatasks.private.task_logging import end_log as _end_log
from casatasks.private.task_logging import except_log as _except_log

class _uvcontfit:
    """
    uvcontfit ---- Fit the continuum in the UV plane

    
    Fit the continuum in the UV plane using polynomials.

    --------- parameter descriptions ---------------------------------------------

    vis      The name of the input visibility file
    caltable Name of output mueller matrix calibration table
    field    Select field(s) using id(s) or name(s)
    intent   Select intents
    spw      Spectral window / channels for fitting the continuum
    combine  Data axes to combine for the continuum estimation (none, spw and/or scan)
    solint   Time scale for the continuum fit
    fitorder Polynomial order for the continuum fits
    append   Append to a pre-existing table

    --------- examples -----------------------------------------------------------

    
    
    This task estimates the continuum emission by fitting polynomials to
    the real and imaginary parts of the spectral windows and channels
    selected by spw and exclude spw. This fit represents a model of
    the continuum in all channels. Fit orders less than 2 are strongly
    recommended.


    """

    _info_group_ = """modeling"""
    _info_desc_ = """Fit the continuum in the UV plane"""

    def __call__( self, vis='', caltable='', field='', intent='', spw='', combine='', solint='int', fitorder=int(0), append=False ):
        schema = {'vis': {'type': 'cReqPath', 'coerce': _coerce.expand_path}, 'caltable': {'type': 'cStr', 'coerce': _coerce.to_str}, 'field': {'type': 'cStr', 'coerce': _coerce.to_str}, 'intent': {'type': 'cStr', 'coerce': _coerce.to_str}, 'spw': {'type': 'cStr', 'coerce': _coerce.to_str}, 'combine': {'type': 'cStr', 'coerce': _coerce.to_str}, 'solint': {'type': 'cVariant', 'coerce': [_coerce.to_variant]}, 'fitorder': {'type': 'cInt'}, 'append': {'type': 'cBool'}}
        doc = {'vis': vis, 'caltable': caltable, 'field': field, 'intent': intent, 'spw': spw, 'combine': combine, 'solint': solint, 'fitorder': fitorder, 'append': append}
        assert _pc.validate(doc,schema), create_error_string(_pc.errors)
        _logging_state_ = _start_log( 'uvcontfit', [ 'vis=' + repr(_pc.document['vis']), 'caltable=' + repr(_pc.document['caltable']), 'field=' + repr(_pc.document['field']), 'intent=' + repr(_pc.document['intent']), 'spw=' + repr(_pc.document['spw']), 'combine=' + repr(_pc.document['combine']), 'solint=' + repr(_pc.document['solint']), 'fitorder=' + repr(_pc.document['fitorder']), 'append=' + repr(_pc.document['append']) ] )
        task_result = None
        try:
            task_result = _uvcontfit_t( _pc.document['vis'], _pc.document['caltable'], _pc.document['field'], _pc.document['intent'], _pc.document['spw'], _pc.document['combine'], _pc.document['solint'], _pc.document['fitorder'], _pc.document['append'] )
        except Exception as exc:
            _except_log('uvcontfit', exc)
            raise
        finally:
            task_result = _end_log( _logging_state_, 'uvcontfit', task_result )
        return task_result

uvcontfit = _uvcontfit( )

