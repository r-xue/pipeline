from pipeline import environment
from pipeline.infrastructure.renderer import stats_extractor
from . import logging

LOG = logging.get_logger(__name__)


class PipelineStatistics(object):
    """A unit of pipeline statistics information"""
    def __init__(self, name, value, longdesc, origin='', units='',
                    score='', level=''):
        self.name = name
        self.value = value
        self.longdesc = longdesc
        self.origin = origin
        self.units = units
        self.score = score
        self.level = level

        # TODO: if the structure is be not nested, some information about the mous, spw, etc needs to be added

        if type(value) is set:
            self.value = list(self.value)

    def to_dict(self):
        stats_dict = {}

#        stats_dict['name'] = self.name is the next level up in the dict.
        stats_dict['longdescription'] = self.longdesc
        stats_dict['level'] = self.level

        if self.origin not in ["", None]:
            stats_dict['origin'] = self.origin

        if self.units not in ["", None]: 
            stats_dict['units'] = self.units

        if self.value not in ["", None]: 
            stats_dict['value'] = self.value

        if self.score not in ["", None]:
            stats_dict['score'] = self.score

        return stats_dict


def _generate_stats(context):
    # Set 1: values that can be gathered directly from the context
    ps1 = PipelineStatistics(
        name='project_id',
        value=list(context.observing_run.project_ids),
        longdesc='Proposal id number',
        level='MOUS')

    ps2 = PipelineStatistics(
        name='pipeline_version',
        value=environment.pipeline_revision,
        longdesc="pipeline version string",
        level='MOUS')

    ps3 = PipelineStatistics(
        name='pipeline_recipe',
        value=context.project_structure.recipe_name,
        longdesc="recipe name",
        level='MOUS')

    ps4 = PipelineStatistics(
        name='casa_version',
        value=environment.casa_version_string,
        longdesc="casa version string",
        level='MOUS')

    stats_collection = []
    stats_collection.append(ps1)
    stats_collection.append(ps2)
    stats_collection.append(ps3)
    stats_collection.append(ps4)

    # Set 2: results objects needed
    stats_from_results = stats_extractor.get_stats_from_results(context)
    for elt in stats_from_results:
        if elt not in [[], [[]]]:
            result = elt[0][0]
            stats_collection.append(result)

    LOG.info(stats_collection)
    # construct final dictionary
    final_dict = {}
    for stat in stats_collection:
        final_dict[stat.name] = stat.to_dict()
    return final_dict
