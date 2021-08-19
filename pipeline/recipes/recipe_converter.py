"""
recipe_conversion.py - conversion script from procedure xml file to Python code.

Please run the script with -h option to see complete usage.

Usage:

    python3 /path/to/recipe_converter.py [recipe] [script]

    or

    casa -c /path/to/recipe_converter.py [recipe] [script]

"""
import argparse
import os
import re
import string
from typing import List, Tuple, Union
import xml.dom.minidom as minidom

# type alias
DOM = Union[minidom.Document, minidom.Element]

# special strings for recipe conversion
INDENT = '        '
TEMPLATE_TEXT = '''# General imports
import traceback

# Pipeline imports
from pipeline.infrastructure import casa_tools

IMPORT_ONLY = 'Import only'


# Run the procedure
def ${func_name}(vislist, importonly=False, pipelinemode='automatic', interactive=True):
    echo_to_screen = interactive
    casa_tools.post_to_log("Beginning pipeline run ...")

    try:
        # Initialize the pipeline
        h_init()

${procedure}

    except Exception as e:
        if str(e) == IMPORT_ONLY:
            casa_tools.post_to_log("Exiting after import step ...", echo_to_screen=echo_to_screen)
        else:
            casa_tools.post_to_log("Error in procedure execution ...", echo_to_screen=echo_to_screen)
            errstr = traceback.format_exc()
            casa_tools.post_to_log(errstr, echo_to_screen=echo_to_screen)

    finally:
        # Save the results to the context
        h_save()

        casa_tools.post_to_log("Terminating procedure execution ...", echo_to_screen=echo_to_screen)
'''


def get_recipe_dir() -> str:
    """Return absolute path to recipe directory.

    Basically, this returns the directory where this file is located.

    Returns:
        Absolute path to recipe directory.
    """
    recipe_dir, _ = os.path.split(__file__)
    recipe_dir = os.path.abspath(recipe_dir)
    print(f'recipe directory is {recipe_dir}')
    return recipe_dir


def get_cli_dir(category: str) -> str:
    """Return path to cli directory for given category.

    Note that this function does not check the existence of
    cli directory.

    Args:
        category: Category name (h, hif, hifa, hifv, hsd, hsdn, etc.)

    Returns:
        Path to cli directory.
    """
    recipe_dir = get_recipe_dir().rstrip('/')
    pipeline_root_dir, _ = os.path.split(recipe_dir)
    cli_dir = os.path.join(pipeline_root_dir, f'{category}/cli')
    print(f'cli directory is {cli_dir}')
    return cli_dir


def get_element(node: DOM, tag_name: str, expect_unique: bool = False) -> Union[DOM, List[DOM]]:
    """Get (list of) DOM object whose name matches tag_name.

    Args:
        node: Parent DOM object.
        tag_name: Tag name.
        expect_unique: Return single DOM object if True.
                       Defaults to False.

    Returns:
        List of DOM objects whose name is tag_name, or the first DOM object
        in the list if expect_unique is True.
    """
    elements = node.getElementsByTagName(tag_name)

    if expect_unique:
        assert len(elements) == 1
        return_value = elements[0]
    else:
        return_value = elements

    return return_value


def get_data(node: DOM) -> str:
    """Extract data from DOM object.

    Return string 'data' if node represents <tag>data</tag>.

    Args:
        node: DOM object

    Returns:
        Data string.
    """
    return node.firstChild.data


def parse_parameter(node: DOM) -> Tuple[str, str]:
    """Parse DOM object corresponding to Parameter tag in procedure xml file.

    Args:
        node: DOM object corresponding to Parameter tag

    Returns:
        Parameter name and its value.
    """
    key_element = get_element(node, 'Keyword', expect_unique=True)
    value_element = get_element(node, 'Value', expect_unique=True)
    return get_data(key_element), get_data(value_element)


def get_short_description(tree: DOM) -> str:
    """Extract task short description.

    Args:
        tree: DOM object corresponding to task xml file (taskname.xml).

    Returns:
        Task short description.
    """
    node = filter(
        lambda x: x.parentNode.nodeName == 'task',
        tree.getElementsByTagName('shortdescription')
    )
    short_desc_node = next(node)
    short_desc = get_data(short_desc_node).strip('\n').strip()
    return short_desc


def get_param_types(tree: DOM) -> dict:
    """Extract list of parameters and value types.

    Args:
        tree: DOM object corresponding to task xml file (taskname.xml).

    Returns:
        Dictionary holding (param_name_str, param_type_str) pair.
    """
    node = filter(
        lambda x: x.parentNode.nodeName == 'input',
        tree.getElementsByTagName('param')
    )
    type_dict = dict(
        (x.getAttribute('name'), x.getAttribute('type')) for x in node
    )
    return type_dict


def get_task_property(task_name: str) -> dict:
    """Get task property from task xml file.

    Args:
        task_name: Pipeline task name.

    Returns:
        Pipeline task property, including the comment (taken from
        the shortdescription tag) and the parameter_types dictionary
        holding (param_name_str, param_type_str) pair.
    """
    if task_name == 'breakpoint':
        return {}

    task_category = task_name.split('_')[0]
    cli_dir = get_cli_dir(task_category)
    task_xml = os.path.join(cli_dir, f'{task_name}.xml')
    print(f'task_xml is {task_xml}')
    assert os.path.exists(task_xml)
    root_element = minidom.parse(task_xml)
    short_desc = get_short_description(root_element)
    type_dict = get_param_types(root_element)

    task_property = {
        'comment': short_desc,
        'parameter_types': type_dict
    }

    return task_property


def parse_command(node: DOM) -> dict:
    """Parse DOM object into dictionary.

    The node parameter must be the object corresponding to
    ProcessingCommand tag in the procedure xml file

    Args:
        node: DOM object to be parsed.

    Returns:
        Dictionary representation of DOM object.

        Return value has the follwoing structure:

            {task_name: {
                'comment': str,
                'parameter': dict,
                'parameter_types': dict,
            }}

        where comment is a string taken from pipeline task xml file
        (shortdescription tag), parameter is a dictionary of the pair
        of parameter name and value specified in procedure xml file,
        and parameter_types is a dictionary of the pair of parameter
        name and its type also taken from pipeline task xml file.

    """
    command_element = get_element(node, 'Command', expect_unique=True)
    command = get_data(command_element)

    parameter_set_element = get_element(node, 'ParameterSet', expect_unique=True)
    parameter_elements = get_element(parameter_set_element, 'Parameter')
    parameters = dict(parse_parameter(p) for p in parameter_elements)
    print(f'command is {command}')
    task_property = get_task_property(command)
    print(f'parameters are {parameters}')
    task_property['parameter'] = parameters
    return {command: task_property}


def parse(procedure_abs_path: str) -> Tuple[str, List[dict]]:
    """Parse procedure xml file.

    Args:
        procedure_abs_path: Absolute path to the procedure xml file.

    Returns:
        Pipeline recipe name with the list of commands.
    """
    root_element = minidom.parse(procedure_abs_path)

    # ProcessingProcedure
    procedure_element = get_element(root_element, 'ProcessingProcedure', expect_unique=True)

    # ProcedureTitle - will be function name
    title_element = get_element(procedure_element, 'ProcedureTitle', expect_unique=True)
    func_name = get_data(title_element)

    print(f'function name is {func_name}')

    # ProcessingCommand
    command_elements = get_element(procedure_element, 'ProcessingCommand')
    commands = [parse_command(e) for e in command_elements]
    print(f'command list is:')
    for command in commands:
        print(f'{command}')

    return func_name, commands


def get_comment(task_name: str, config: dict) -> str:
    """Generate comment for given task from the configuration.

    The function refers comment field of config. Multi-line comment
    is properly handled.

    Args:
        task_name: Pipeline task name.
        config: Pipeline task configuration.

    Returns:
        Comment for pipeline task. If task_name is 'breakpoint',
        the comment indicating breakpoint will be returned.
    """
    comment = config.get('comment', '')
    prefix = f'{INDENT}# '
    if task_name == 'breakpoint':
        comment = prefix + f' ---- {task_name} ----'
    elif comment:
        comment = comment.strip('\n')
        # handle multi-line comment
        comment = re.sub('\n +', f'\n{prefix}', comment)
        comment = prefix + comment + '\n'
    return comment


def get_execution_command(task_name: str, config: dict) -> str:
    """Generate execution command from given task name and configuration.

    The config parameter should have at least two fields: parameter and
    parameter_types. The parameter field should be the dictionary with
    (param_name_str, param_value_str) pair while the parameter_types field
    should be the dictionary with (param_name_str, param_type_str) pair.

    Args:
        task_name: Pipeline task name.
        config: Pipeline task configuration.

    Returns:
        Task execution command string. If task_name is 'breakpoint',
        empty string will be returned.
    """
    parameter = config.get('parameter', '')

    # breakpoint
    if task_name == 'breakpoint':
        return ''

    # param_types = get_parameter_types(task_name)
    param_types = config['parameter_types']

    if parameter:
        def construct_arg(key, value):
            value_type = param_types[key]
            # TODO: handle variant and any types properly
            if value_type in ('string', 'variant', 'any'):
                arg = f'{key}=\'{value}\''
            else:
                arg = f'{key}={value}'
            return arg

        custom_args = ', '.join([construct_arg(k, v) for k, v in parameter.items()])
        args = f'{custom_args}, pipelinemode=\'interactive\''
    else:
        args = 'pipelinemode=\'automatic\''

    # special handling for importdata task
    is_importdata = 'importdata' in task_name
    if is_importdata:
        args = f'vis=vislist, {args}'

    # construct function call
    command = f'{INDENT}{task_name}({args})'

    if is_importdata:
        command += '''

        if importonly:
            raise Exception(IMPORT_ONLY)'''

    return command


def c2p(command: dict) -> str:
    """Convert pipeline command dictionary into string.

    The command parameter should be the dictionary returned by
    parse_command.

    Args:
        command: Pipeline task and its custom parameters.

    Returns:
        Python code snippet invoking given pipeline task.
        The string will look like the following:

            # task shortdescription taken from the task xml file
            taskname(pipelinemode='automatic')

        or, if parameters are customized in the procedure xml file,

            # task shortdescription taken from the task xml file
            taskname(custom_param=custom_value, pipelinemode='interactive')

        Note that there will be some additional code for importdata stage.
    """
    print(f'c2p: command is {list(command.keys())[0]}')
    assert len(command) == 1
    task_name, config = list(command.items())[0]
    procedure = get_comment(task_name, config)

    command = get_execution_command(task_name, config)
    procedure += command

    return procedure


def to_procedure(commands: List[dict]) -> str:
    """Convert list of commands to string that represents Python code snippet.

    Args:
        commands: List of pipeline tasks and their custom parameters.

    Returns:
        Python code snippet that executes pipeline tasks sequentially.
    """
    return '\n\n'.join([c2p(command) for command in commands])


def export(func_name: str, commands: List[dict], script_name: str) -> None:
    """Export parsed information as a Python script.

    Args:
        func_name: Name of the function defined in the Python script.
        commands: List of pipeline tasks and their custom parameters.
        script_name: Output script name.
    """
    template = string.Template(TEMPLATE_TEXT)
    procedure = to_procedure(commands)
    with open(script_name, 'w') as f:
        f.write(template.safe_substitute(
            func_name=func_name,
            procedure=procedure
        ))


def main(recipe_name: str, script_name: str) -> None:
    """Generate Python script from procedure xml file.

    Args:
        recipe_name: Recipe name. Will be translated into xml file name,
                           procedure_<recipe_name>.xml.
        script_name: Output script name.
    Raises:
        FileNotFoundError: Procedure xml file does not exist.
    """
    procedure = f'procedure_{recipe_name}.xml'

    print(f'__file__ is {__file__}')
    print(f'recipe is {recipe_name}')
    print(f'procedure is {procedure}')

    recipe_dir = get_recipe_dir()

    procedure_abs_path = os.path.join(recipe_dir, procedure)
    if not os.path.exists(procedure_abs_path):
        raise FileNotFoundError(f'Procedure "{procedure}" not found.')

    # parse procedure xml file
    func_name, commands = parse(procedure_abs_path)

    # export commands as Python script
    export(func_name, commands, script_name)


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Convert procedure xml file into Python script')
    parser.add_argument('recipe', type=str, nargs='?', default='hsd_calimage',
                        help='recipe type. will be translated to xml file name, "procedure_<recipe>.xml"')
    parser.add_argument('script', type=str, nargs='?', default=None,
                        help='output scirpt name. defaults to <recipe>.py')
    args = parser.parse_args()

    recipe_name = args.recipe
    script_name = args.script
    if script_name is None:
        script_name = f'{recipe_name}.py'

    main(recipe_name, script_name)

