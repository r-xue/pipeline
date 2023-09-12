# Started from jmaster's create_docs.py.
# Modified by kberry to work with post-removal of the task interface.

import argparse
import inspect
import os
import sys
from collections import namedtuple
from typing import Tuple

from mako.template import Template

Task = namedtuple('Task', 'name short description parameters examples')

task_groups = {"h": "Generic",
               "hif": "Interferometry Generic",
               "hifa": "Interferometry ALMA",
               "hifas": "Interferometry ALMA SRDP",
               "hifv": "Interferometry VLA",
               "hsd": "Single Dish",
               "hsdn": "Nobeyama"}

# Tasks to exclude from the reference manual
# hifv tasks confirmed by John Tobin via email 20230911
tasks_to_exclude = ['hifv_targetflag', 'hifv_gaincurves', 'hifv_opcal', 'hifv_rqcal', 'hifv_swpowcal', 'hifv_tecmaps']

pdict = {"h": [],
         "hif": [],
         "hifa": [],
         "hifas": [],
         "hifv": [],
         "hsd": [],
         "hsdn": []}


def check_dirs(filename):
    """pre-check/create the ancestry directories of a given file path."""
    filedir = os.path.dirname(filename)
    if not os.path.exists(filedir):
        os.makedirs(filedir)


def write_out(pdict, rst_file="pipeline_new_tasks.rst", outdir=None):
    """Creates reST file for the "landing page" for the tasks.
    """
    script_path = os.path.dirname(os.path.realpath(__file__))
    task_template = Template(filename=os.path.join(script_path, 'pipeline_tasks.mako'))

    # Write the information from into a rst file that can be rendered by sphinx as html/pdf/etc.

    output_dir = script_path if outdir is None else outdir
    rst_file_full_path = os.path.join(output_dir, rst_file)
    check_dirs(rst_file_full_path)
    with open(rst_file_full_path, 'w') as fd:
        rst_text = task_template.render(plversion=2023, pdict=pdict, task_groups=task_groups)
        fd.writelines(rst_text)


def write_tasks_out(pdict, outdir=None):
    """Creates reST files for each task.
    """
    script_path = os.path.dirname(os.path.realpath(__file__))
    task_template = Template(filename=os.path.join(script_path, 'individual_task.mako'))

    output_dir = script_path if outdir is None else outdir
    for entry in pdict:
        for task in pdict[entry]:
            rst_file = "{}/{}_task.rst".format(entry, task.name)
            rst_file_full_path = os.path.join(output_dir, rst_file)
            check_dirs(rst_file_full_path)
            with open(rst_file_full_path, 'w') as fd:
                rst_text = task_template.render(category=entry, name=task.name, description=task.description,
                                                parameters=task.parameters, examples=task.examples)
                fd.writelines(rst_text)


# TODO: Pull out excess whitespace
def docstring_parse(docstring: str) -> Tuple[str, str, str, str, str]:
    """ Does its best to parse the docstring for each python task.
        Example of docstring-format that this will parse:
        #FIXME: add
    """
    parameter_delimiter = "--------- parameter descriptions ---------------------------------------------"
    examples_delimiter = "--------- examples -----------------------------------------------------------"
    issues_delimiter = "--------- issues -----------------------------------------------------------"

    short = ""
    long = ""
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
        long_split_stripped = [line[4:] for line in long_split]
        long = "\n".join(long_split_stripped).strip("\n")

        second_split = end_half.split(examples_delimiter)

        parameters = second_split[0]
        # Better format parameters:

        # FIXME: This is still a "rough draft" that needs updating and verifying
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

        # Add the information for the last parameter
        if parameter_name != "" and current_parm_desc is not None:
            parameters_dict[parameter_name] = current_parm_desc

        # Remove "dryrun" from dict as it is not wanted in the Reference Manual
        if "dryrun" in parameters_dict:
            del parameters_dict["dryrun"]

        examples = second_split[1].strip("\n")

        if issues_delimiter in examples:
            temp_split = examples.split(issues_delimiter)
            examples = temp_split[0]

        examples = "\n".join([line[4:] for line in examples.split("\n")]).strip("\n")

        return short, long, examples, parameters_dict

    except Exception as e:
        print("FAILED to PARSE DOCSTRING. Error: {}".format(e))
        print("Failing docstring: {}".format(docstring))
        return short, long, examples, parameters_dict


def create_docs(missing_report=True, outdir=None, srcdir=None):
    """
    Walks through the pipeline and creates documentation for each pipeline task.

    Optionally generates and outputs lists of tasks with missing examples, parameters, and
    longer descriptions.
    """

    if srcdir is not None and os.path.exists(srcdir):
        sys.path.insert(0, srcdir)
        import pipeline

    # Lists of cli PL tasks that are missing various pieces:
    missing_example = []
    missing_description = []
    missing_parameters = []

    for name, obj in inspect.getmembers(pipeline):
        if name in task_groups.keys():
            for name2, obj2 in inspect.getmembers(obj):
                if 'cli' in name2:
                    for task_name, task_func in inspect.getmembers(obj2):
                        if '__' not in task_name and task_name is not None and task_name[0] == 'h':
                            docstring = task_func.__doc__
                            short, long, examples, parameters = docstring_parse(docstring)

                            if not examples:
                                missing_example.append(task_name)
                            if not long:
                                missing_description.append(task_name)
                            if not parameters:
                                missing_parameters.append(task_name)

                            if task_name not in tasks_to_exclude:
                                pdict[name].append(Task(task_name, short, long, parameters, examples))
                            else:
                                print("Excluding task: {}".format(task_name))

    if missing_report:
        print("The following tasks are missing examples:")
        for name in missing_example:
            print(name)
        print("\n")

        print("The following tasks are missing descriptons:")
        for name in missing_description:
            print(name)

        print("\n")

        print("The following tasks are missing parameters:")
        for name in missing_parameters:
            print(name)
            print("\n")

    # Write out "landing page"
    write_out(pdict, outdir=outdir)

    # Write individual task pages
    write_tasks_out(pdict, outdir=outdir)


def cli_command():
    """CLI interface of create_docs.py.
    
    try `python create_docs.py --help`
    """

    # insert the pipeline source directory in case it's not visual to the in-use interpreter.
    if os.getenv('pipeline_dir') is not None:
        # use the env variable "pipeline_dir" to look for the Pipeline source code.
        pipeline_src = os.path.abspath(pipeline_dir)
    else:
        # use the ancestry path if "pipeline_dir" is not set.
        pipeline_src = os.path.abspath('../../pipeline')

    parser = argparse.ArgumentParser(description='Generate Pipeline task .RST files')
    parser.add_argument('--outdir', '-o', type=str, default=None, help='Output path of the RST files/subdirectories')
    parser.add_argument('--srcdir', '-s', type=str, default=pipeline_src, help='Path of the Pipeline source code')

    args = parser.parse_args()

    create_docs(missing_report=True, outdir=args.outdir, srcdir=args.srcdir)


if __name__ == "__main__":
    cli_command()
