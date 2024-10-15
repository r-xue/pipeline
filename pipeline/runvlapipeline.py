# Note the assumption here is that the script is run in the following
# way because the argument count starts from casapy not the script name
#
# casapy --nogui --nologger -c runvlapipeline.py <pipeline_procesing_request>
#


# The system module
import sys

#sys.path.insert (0, os.path.expandvars("$SCIPIPE_HEURISTICS"))

# Import the module which executes the pipeline processing request.
import pipeline.infrastructure.executevlappr as eppr

# Need to use casashell to get the '-c' parameter
import casashell

# Execute the request
if __name__ == '__main__':
    eppr.executeppr(casashell.argv[casashell.argv.index('-c')+2], importonly=False)
