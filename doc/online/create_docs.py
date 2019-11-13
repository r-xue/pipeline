from mako.template import Template
import xml.etree.ElementTree as ET
import argparse
import glob
import os

class TaskParam:
    def __init__(self):
        self.name = ''
        self.type = ''
        self.description = ''
        self.defaultval = None
        self.subpar = False

task_template = Template(filename='task.mako')

parser = argparse.ArgumentParser(description='Process some integers.')
# TODO modify the taskname parameter to allow for a list of task names instead of only a single task
parser.add_argument('--taskname', '-t', type=str, default='', help='name of task to build documentation')
args = parser.parse_args()

# TODO add code to determine this script location and make all paths below relative to it

# create docs for a single task, or all of them
if args.taskname:
    task_name = args.taskname  # e.g. 'hifa_importdata'
    module_name = task_name.split('_')[0]
    taskfiles = ["../../pipeline/{mn}/cli/{tn}.xml".format(mn=module_name, tn=task_name)]
else:
    taskfiles = glob.glob('../../pipeline/h*/cli/h*.xml')

if not os.path.exists('tasks'):
    os.mkdir('tasks')
    print('Creating tasks/ subdirectory to populate with .rst files')

for taskfile in taskfiles:
    task_name = os.path.splitext(os.path.basename(taskfile))[0]

    rst_file = 'tasks/{}.rst'.format(task_name)

    print('\tCreating {f} from {t}'.format(f=rst_file, t=taskfile))
    pars = []
    # Read the casa XML interface file
    with open(taskfile) as xmlfile:
        tree = ET.parse(xmlfile)
        root = tree.getroot()

        for xx in root[0]:
            if 'shortdescription' in xx.tag:
                shortdescription = xx.text
            if 'example' in xx.tag:
                example = xx.text

        for xx in root[0]:
            if 'input' in xx.tag:
                inp_el = xx

                for param in inp_el:
                    if 'param' in param.tag:
                        pname = param.attrib.get('name')
                        ptype = param.attrib.get('type')
                        for yy in param:
                            if 'description' in yy.tag:
                                pdesc = yy.text
                            if 'value' in yy.tag:
                                pval = yy.text

                        tp = TaskParam()
                        tp.name = pname
                        tp.type = ptype
                        tp.description = pdesc
                        tp.defaultval = pval
                        pars.append(tp)

    # Write the information from the casa XML into a rst file that can be rendered by sphinx as html/pdf/etc.
    with open(rst_file, 'w') as fd:
        # print(pars)
        rst_text = task_template.render(taskname=task_name, shortdescription=shortdescription, example=example, pdict=pars)
        fd.writelines(rst_text)
