"""
SingleDish tools related to QA
"""
import copy
import math

import pipeline.infrastructure as infrastructure
import pipeline.infrastructure.pipelineqa as pqa

LOG = infrastructure.get_logger(__name__)


class QAScoreAggregator():
    """
    Class for QA score aggregation
    """
    def __init__( self,
                  keys_to_aggregate: list[str],
                  longmsg_format: str | dict[str, str] | None = None,
                  longmsg_keys: list[str] | None = None,
                  preserve_original: bool = True,
                  precision: int = 2,
                  always_update_longmsg: bool = True ):
        """
        Construct QAScoreAggregator instance

        keys_to_aggregate:     List of keys to aggregate.
                                   Hierarchial matches will be done in the order of the list (list is higher to lower hierarchy)
        longmsg_format:        Template of the long msg, either a dict or a string.
                                   if string, uniform template applies to all scores.
                                   if dict, template is specified per metric_name.
                                   Default is None, which uses a standard fornat.
        longmsg_keys:          List of keys to show on the updated longmsg of QA score,
                                   default is None which applies all keys in applies_to of QA score
        preserve_original:     Whether to attach the list of original QA scores with WebLogLocation.HIDDEN. Default is True.
        precision:             Number of decimal places to round score values. Default is 2.
        always_update_longmsg: Update longmsg and round the score regardless the aggregation. Default is True.
        """
        self.keys_to_aggregate = keys_to_aggregate
        self.longmsg_format = longmsg_format
        self.longmsg_keys = longmsg_keys
        self.preserve_original = preserve_original,
        self.precision = precision
        self.always_update_longmsg = always_update_longmsg

    def update_longmsg( self, qascore: pqa.QAScore ):
        """
        Update longmsg of QA score with asocciated keys in applies_to

        If no template is provided, the default template will be generated in this method.

        Args:
            qascore: QA score
        """
        if isinstance( self.longmsg_format, dict ):
            if qascore.origin.metric_name in self.longmsg_format.keys():
                longmsg_format = self.longmsg_format[ qascore.origin.metric_name ]
            else:
                longmsg_format = None
        elif isinstance( self.longmsg_format, str ):
            longmsg_format = self.longmsg_format
        else:
            longmsg_format = None

        if longmsg_format is None:
            if self.longmsg_keys is None:
                longmsg_keys = list( vars(qascore.applies_to).keys() ) if self.longmsg_keys is None else self.longmsg_keys
            else:
                longmsg_keys = self.longmsg_keys

            template = '{shortmsg}'
            for key in longmsg_keys:
                match key:
                    case "vis":
                        template +=  '  MS: {vis}' if len(qascore.applies_to.vis) > 0 else ''
                    case "antenna":
                        template +=  '  Antenna: {ant}' if len(qascore.applies_to.ant) > 0 else ''
                    case _:
                        template += f'  {key.capitalize()}: {{{key}}}' if len(getattr(qascore.applies_to, key)) > 0 else ''
        else:
            template = longmsg_format

        qascore.longmsg = template.format( shortmsg=qascore.shortmsg,
                                           score_metric=qascore.origin.metric_name,
                                           vis=', '.join( sorted(qascore.applies_to.vis) ),
                                           field=', '.join( sorted(qascore.applies_to.field) ),
                                           intent=', '.join( sorted(qascore.applies_to.intent) ),
                                           spw=', '.join( sorted(str(v) for v in qascore.applies_to.spw) ),
                                           ant=', '.join( sorted(qascore.applies_to.ant) ),
                                           pol=', '.join( sorted(qascore.applies_to.pol) ) )

    def update_origin( self, destination: pqa.QAScore, qascores: list[pqa.QAScore], matched_idxes: list[int] ):
        """
        Update orogin of a QA score to accommodate aggregated metric_scores

        The aggregation will simply concatinate the metric scores

        Args:
            destination:   QA score to update origin field
            qascores:      List of QA scores
            matched_idxes: List of indexes of QA scores to aggregate
        """
        names   = [ qascores[idx].origin.metric_name for idx in matched_idxes ]
        mscores = [ qascores[idx].origin.metric_score for idx in matched_idxes ]
        units   = [ qascores[idx].origin.metric_units for idx in matched_idxes ]

        assert len(set(names)) == 1
        assert len(set(units)) == 1
        newscore = ", ".join( str(s) for s in mscores )
        new_origin = pqa.QAOrigin( metric_name = names[0],
                                   metric_score = newscore,
                                   metric_units = units[0] )
        destination.origin = new_origin

    def compare_applies_to( self, qascore1: pqa.QAScore, qascore2: pqa.QAScore, keys_to_compare: list[str] ) -> bool:
        """
        Compare the specific attribute in applies_to of qascores

        Args:
            qascore1, qascore2: QAScores to compare.
            keys_to_compare:    List of keys to participate in the comparizon.
        Returns:
            Whether specified attributes in two QAScores agree.
        """
        return all( getattr(qascore1.applies_to, key) == getattr(qascore2.applies_to, key)
                    for key in keys_to_compare )

    def single_aggregation( self,
                            qascores: list[pqa.QAScore],
                            key: str ) -> pqa.QAScore | None:
        """
        Aggregate QA messages for a single key

        Args:
            qascores:              List of QA scores
            key:                   Key to aggregate (single key)
        Returns:
            Aggregated QA scores
        """
        # get a full list of attributes in QAScore.applies_to if 'self.keys_to_aggregate' is not given.
        all_keys = list( vars(qascores[0].applies_to).keys() ) if self.keys_to_aggregate is None \
            else self.keys_to_aggregate

        # exclude the specified key to get the list of keys to compare during aggregation
        keys_to_compare = all_keys.copy()
        keys_to_compare.remove( key )

        # torelance
        eps = pow( 10, -(1 + self.precision) )

        # scan through QA scores
        for target_qascore in qascores[:]:
            # go next if the target_qascore is already removed during former aggregation
            if target_qascore not in qascores:
                continue
            target_idx = qascores.index(target_qascore)
            # go through qascores and find matches to aggregate
            matched_keys = []
            matched_idxes = []
            for idx, qascore in enumerate(qascores):
                if idx < target_idx:  # always search forward
                    continue
                if math.fabs(qascore.score - target_qascore.score) < eps \
                   and qascore.origin.metric_name == target_qascore.origin.metric_name \
                   and qascore.origin.metric_units == target_qascore.origin.metric_units:
                    if self.compare_applies_to( qascore, target_qascore, keys_to_compare ):
                        matched_keys.append( getattr( qascore.applies_to, key ) )
                        matched_idxes.append( idx )
            if len(matched_idxes) > 1:
                # replace the first QAScore with the aggregated one, remove the other matches from qascores
                setattr( qascores[matched_idxes[0]].applies_to, key, set().union(*matched_keys) )
                self.update_origin(qascores[matched_idxes[0]], qascores, matched_idxes )
                self.update_longmsg(qascores[matched_idxes[0]])
                # remove
                for idx in reversed(matched_idxes[1:]):   # removing should happen in reversed order
                    qascores.pop(idx)
            if self.always_update_longmsg:
                self.update_longmsg(target_qascore)
        return qascores

    def aggregate_qascores( self,
                            orig_qascores: list[pqa.QAScore] ) -> list[pqa.QAScore]:
        """
        Args:
            orig_qascores: Original list of QA scores
        Returns:
            Aggregated List of QA scores (and, if specified, the original QA scores with WegLogLocation.HIDDEN)
        """
        # round score valuesa
        qascores = copy.deepcopy(orig_qascores)
        for qascore in qascores:
            qascore.score = round( qascore.score, self.precision )

        for key in reversed(self.keys_to_aggregate):  # reverse the list to process lower hierarchy first
            qascores = self.single_aggregation( qascores, key )

        # attach original qascores
        if self.preserve_original:
            for qascore in orig_qascores:
                qascore.weblog_location = pqa.WebLogLocation.HIDDEN
                qascores.append(qascore)

        return qascores
