# Started from jmaster's create_docs.py.
# Modified by kberry to work with post-removal of the task interface.
from collections import namedtuple
from mako.template import Template

import inspect
import os
from typing import Tuple

import pipeline
import pipeline.h.cli  # Needed for some reason?
import pipeline.hif.cli
import pipeline.hifa.cli
import pipeline.hifas.cli
import pipeline.hifv.cli
import pipeline.hsd.cli
import pipeline.hsdn.cli

Task = namedtuple('Task', 'name short description parameters examples')

task_groups = {"h": "Generic",
               "hif": "Interferometry Generic",
               "hifa": "Interferometry ALMA",
               "hifas": "Interferometry ALMA SRDP",
               "hifv": "Interferometry VLA",
               "hsd": "Single Dish",
               "hsdn": "Nobeyama"}

pdict = {"h": [],
         "hif": [],
         "hifa": [],
         "hifas": [],
         "hifv": [],
         "hsd": [],
         "hsdn": []}


def write_out(pdict, rst_file="pipeline_new_tasks.rst"):
    """Creates reST file for the "landing page" for the tasks. 
    """
    script_path = os.path.dirname(os.path.realpath(__file__))
    task_template = Template(filename=os.path.join(script_path, 'pipeline_tasks.mako'))

    # Write the information from into a rst file that can be rendered by sphinx as html/pdf/etc.
    rst_file_full_path = os.path.join(script_path, rst_file)
    with open(rst_file_full_path, 'w') as fd:
        rst_text = task_template.render(plversion=2023, pdict=pdict, task_groups=task_groups)
        fd.writelines(rst_text)


def write_tasks_out(pdict):
    """Creates reST files for each task.
    """
    script_path = os.path.dirname(os.path.realpath(__file__))
    task_template = Template(filename=os.path.join(script_path, 'individual_task.mako'))
    for entry in pdict:
        for task in pdict[entry]:
            rst_file = "{}/{}_task.rst".format(entry, task.name)
            rst_file_full_path = os.path.join(script_path, rst_file)
            with open(rst_file_full_path, 'w') as fd:
                rst_text = task_template.render(category=entry, name=task.name, description=task.description, parameters=task.parameters, examples=task.examples)
                fd.writelines(rst_text)


# TODO: Pull out excess whitespace
def docstring_parse(docstring: str) -> Tuple[str, str, str, str, str]:
    """ Does its best to parse the docstring for each python task.
        Example of docstring-format that this will parse:
        #FIXME: add
    """
    parameter_delimiter = "--------- parameter descriptions ---------------------------------------------"
    examples_delimiter = "--------- examples -----------------------------------------------------------"

    short = ""
    long = ""
    default = ""
    output = ""
    examples = ""
    parameters = ""
    parameters_dict = {}

    try:
        beginning_half, end_half = docstring.split(parameter_delimiter)

        index = 0

        lines = beginning_half.split("\n")
        if len(lines) > 1:
            short = lines[1].split(" ---- ")
            if len(short) > 1:
                if short[1] != '':
                    short = short[1]
                    index = 2
                else:
                    # hifa_wvrgcal and hifa_wvrgcal flag have longer short descriptions that
                    # extend onto the next line
                    short = lines[2].strip() + "\n" + lines[3].strip()
                    index = 4

        long = beginning_half
        # Better format long description:
        long_split = long.split('\n')[index:]
        long_split_stripped = [line.strip() for line in long_split]
        long = "\n".join(long_split_stripped).strip("\n")

        second_split = end_half.split(examples_delimiter)

        parameters = second_split[0]
        # Better format parameters:

        # FIXME: This is still a "rough draft" that needs updating, verifying, and formatting.
        parms_split = parameters.split("\n")

        parameters_dict = {}  # format is {'param': 'description'}
        current_parm_desc = None
        parameter_name = ""
        for line in parms_split:
            if len(line) > 4:
                if not line[4].isspace():
                    if current_parm_desc is not None:
                        parameters_dict[parameter_name] = current_parm_desc
                    parameter_name = line.split()[0]
                    index = line.find(parameter_name)
                    description_line_one = line[index+len(parameter_name):].strip()
                    current_parm_desc = description_line_one
                else:
                    if current_parm_desc is not None:
                        new_line = line.strip()
                        # Don't add totally empty lines:
                        if not new_line.isspace():
                            current_parm_desc = current_parm_desc + " " + new_line + "\n"

        examples = second_split[1]
        # Better format examples:
        examples = "\n".join([line.strip() for line in examples.split("\n")]).strip("\n")

        return short, long, default, output, examples, parameters_dict
    
    except Exception as e: 
        print("FAILED to PARSE DOCSTRING. Error: {}".format(e))
        print("Failing docstring: {}".format(docstring))
        return short, long, default, output, examples, parameters_dict


def create_docs():
    """ Walks through the pipeline and creates documentation for each pipeline task.
    """
    for name, obj in inspect.getmembers(pipeline):
        if name in task_groups.keys():
            for name2, obj2 in inspect.getmembers(obj):
                if 'cli' in name2:
                    for name3, obj3 in inspect.getmembers(obj2):
                        if '__' not in name3 and name3 is not None and name3[0] == 'h':
                            docstring = obj3.__doc__
                            short, long, default, output, examples, parameters = docstring_parse(docstring)
                            pdict[name].append(Task(name3, short, long, parameters, examples))

    # Write out "landing page"
    write_out(pdict)

    # Write individual task pages
    write_tasks_out(pdict)


if __name__ == "__main__":
    create_docs()