import collections
import distutils.cmd
import distutils.log
import os
import shutil
import subprocess

import setuptools
from setuptools.command.build_py import build_py

ENCODING = 'utf-8'  # locale.getpreferredencoding()
PIPELINE_PACKAGES = ['h', 'hif', 'hifa', 'hifv', 'hsd', 'hsdn']


def flatten(items):
    """Yield items from any nested iterable"""
    for x in items:
        if isinstance(x, collections.Iterable) and not isinstance(x, (str, bytes)):
            for sub_x in flatten(x):
                yield sub_x
        else:
            yield x


class MinifyJSCommand(distutils.cmd.Command):
    description = 'Minify the pipeline javascript'
    user_options = [('inplace', 'i', 'Generate minified JS in src directory')]
    boolean_options = ['inplace']

    def __init__(self, dist):
        distutils.cmd.Command.__init__(self, dist)

    def initialize_options(self):
        """Set default values for options."""
        self.inplace = None

    def finalize_options(self):
        # get the path to this file
        root_dir = os.path.dirname(os.path.realpath(__file__))
        build_py_cmd = self.get_finalized_command('build_py')

        if self.inplace:
            build_path = root_dir
        else:
            # get the path to the build directory
            build_path = os.path.join(root_dir, build_py_cmd.build_lib)

        js_path = build_py_cmd.get_package_dir('pipeline.infrastructure.renderer.templates.resources.js')
        self.js_input_dir = os.path.join(root_dir, js_path)
        self.js_output_dir = os.path.join(build_path, js_path)
        self.root_dir = root_dir

    def run(self):
        to_minify = {
            'pipeline_common.min.js': [
                'jquery-3.3.1.js',                          # dependency of pipeline.js and its plugins,
                'holder.js',                                # placeholders for missing images
                'lazyload.js',                              # only load images when they become visible
                'jquery.fancybox.js',                       # fancybox
                'plotcmd.js',                               # pipeline plugin for fancybox
                'purl.js',                                  # URL inspection
                'bootstrap.js',                             # bootstrap
                'pipeline.js',                              # pipeline
            ],
            'pipeline_plots.min.js': [
                'select2.js',                               # filters on detail plots pages
                'd3.v3.js',                                 # histograms
            ]
        }

        if not os.path.exists(self.js_output_dir):
            os.mkdir(self.js_output_dir)
            shutil.copymode(self.js_input_dir, self.js_output_dir)

        for output_file, input_files in to_minify.items():
            output_path = os.path.join(self.js_output_dir, output_file)
            input_paths = [os.path.join(self.js_input_dir, f) for f in input_files]
            self.minify(input_paths, output_path)

    def minify(self, inputs, output):
        self.announce('Creating minified JS: {}'.format(os.path.basename(output)),
                      level=distutils.log.INFO)

        command = ['java',
                   '-jar', 'closure-compiler.jar',
                   '--js_output_file', output,
                   '--language_in', 'ECMASCRIPT6',
                   '--language_out', 'ECMASCRIPT6',
                   '--generate_exports', 'false',
                   '--warning_level', 'verbose',
                   '--compilation_level', 'BUNDLE']

        input_files = [['--js', i] for i in inputs]

        input_args = flatten([command, input_files])

        subprocess.check_output([str(i) for i in input_args], cwd=self.root_dir)


class MinifyCSSCommand(distutils.cmd.Command):
    description = 'Minify the pipeline CSS'
    user_options = [('inplace', 'i', 'Generate minified CSS in src directory')]
    boolean_options = ['inplace']

    def __init__(self, dist):
        distutils.cmd.Command.__init__(self, dist)

    def initialize_options(self):
        """Set default values for options."""
        self.inplace = None

    def finalize_options(self):
        # get the path to this file
        root_dir = os.path.dirname(os.path.realpath(__file__))
        build_py_cmd = self.get_finalized_command('build_py')

        if self.inplace:
            build_path = root_dir
        else:
            # get the path to the build directory
            build_path = os.path.join(root_dir, build_py_cmd.build_lib)

        # get the path to the source CSS directory
        css_path = build_py_cmd.get_package_dir('pipeline.infrastructure.renderer.templates.resources.css')
        self.css_input_dir = os.path.join(root_dir, css_path)
        self.css_output_dir = os.path.join(build_path, css_path)

    def run(self):
        to_minify = {
            'all.min.css': [
                'font-awesome.css',
                'jquery.fancybox.css',
                'select2.css',
                'select2-bootstrap.css',
                'pipeline.css',
            ]
        }

        if not os.path.exists(self.css_output_dir):
            os.mkdir(self.css_output_dir)
            shutil.copymode(self.css_input_dir, self.css_output_dir)

        for output_file, input_files in to_minify.items():
            output_path = os.path.join(self.css_output_dir, output_file)
            input_paths = [os.path.join(self.css_input_dir, f) for f in input_files]
            self.minify(input_paths, output_path)

    def minify(self, inputs, output_path):
        self.announce('Creating minified CSS: {}'.format(os.path.basename(output_path)),
                      level=distutils.log.INFO)

        buffer = []
        for name in inputs:
            with open(name, 'r', encoding=ENCODING) as f:
                buffer.append(f.read())
        buffer = '\n\n'.join(buffer)

        import csscompressor
        output = csscompressor.compress(buffer)

        with open(output_path, 'w', encoding=ENCODING) as f:
            f.write(output)
            f.write('\n')


class BuildMyTasksCommand(distutils.cmd.Command):
    description = 'Generate the CASA CLI bindings'
    user_options = [('inplace', 'i', 'Generate CLI bindings in src directory')]
    boolean_options = ['inplace']

    def __init__(self, dist):
        distutils.cmd.Command.__init__(self, dist)

    def initialize_options(self):
        """Set default values for options."""
        self.inplace = None

    def finalize_options(self):
        # get the path to this file
        dir_path = os.path.dirname(os.path.realpath(__file__))

        if self.inplace:
            self.build_path = dir_path
        else:
            # get the path to the build directory
            build_py_cmd = self.get_finalized_command('build_py')
            self.build_path = os.path.join(dir_path, build_py_cmd.build_lib)

    def run(self):
        for d in PIPELINE_PACKAGES:
            cli_dir = os.path.join('pipeline', d, 'cli')
            cli_module = '.'.join(['pipeline', d, 'cli'])
            src_dir = os.path.join(self.build_path, cli_dir)
            if not os.path.exists(src_dir):
                continue

            cli_init_py = os.path.join(src_dir, '__init__.py')
            # Remove old init module to avoid incompatible code and duplication
            if os.path.exists(cli_init_py):
                os.remove(cli_init_py)

            gotasks_dir = os.path.join(src_dir, 'gotasks')
            gotasks_init_py = os.path.join(gotasks_dir, '__init__.py')
            # Remove old init module to avoid incompatible code and duplication
            if os.path.exists(gotasks_init_py):
                os.remove(gotasks_init_py)

            for xml_file in [f for f in os.listdir(src_dir) if f.endswith('.xml')]:
                self.announce('Building task from XML: {}'.format(xml_file), level=distutils.log.INFO)
                subprocess.check_output([
                    'buildmytasks',
                    '--module',
                    cli_module,
                    xml_file
                ], cwd=src_dir)

                root, _ = os.path.splitext(xml_file)
                import_statement = 'from .{} import {}'.format(root, root)
                with open(cli_init_py, 'a+', encoding=ENCODING) as init_file:
                    import_exists = any(import_statement in line for line in init_file)
                    if not import_exists:
                        init_file.seek(0, os.SEEK_END)
                        init_file.write('{}\n'.format(import_statement))

                with open(gotasks_init_py, 'a+', encoding=ENCODING) as init_file:
                    import_exists = any(import_statement in line for line in init_file)
                    if not import_exists:
                        init_file.seek(0, os.SEEK_END)
                        init_file.write('{}\n'.format(import_statement))


class VersionCommand(distutils.cmd.Command):
    description = 'Generate the version file'
    user_options = [('inplace', 'i', 'Generate the version file in src directory')]
    boolean_options = ['inplace']

    def __init__(self, dist):
        distutils.cmd.Command.__init__(self, dist)

    def initialize_options(self):
        """Set default values for options."""
        self.inplace = None

    def finalize_options(self):
        # get the path to this file
        dir_path = os.path.dirname(os.path.realpath(__file__))

        if self.inplace:
            self.build_path = dir_path
        else:
            # get the path to the build directory
            build_py_cmd = self.get_finalized_command('build_py')
            self.build_path = os.path.join(dir_path, build_py_cmd.build_lib)

    def run(self):
        version = _get_git_version()
        version_py = os.path.join(self.build_path, 'pipeline', '_version.py')
        self.announce('Creating version file: {}'.format(version_py), level=distutils.log.INFO)
        with open(version_py, 'w', encoding=ENCODING) as f:
            f.write("# File generated by setup.py\n# do not change, do not track in version control\n")
            f.write("version = '{}'\n".format(version))
            f.write('\n')


def _get_git_version():
    # Retrieve info about current commit.
    try:
        # Set version to latest tag, number of commits since tag, and latest
        # commit hash.
        commit_hash = subprocess.check_output(['git', 'describe', '--always', '--tags', '--long', '--dirty'],
                                              stderr=subprocess.DEVNULL).decode().strip()
    except (FileNotFoundError, subprocess.CalledProcessError):
        # FileNotFoundError: if git is not on PATH.
        # subprocess.CalledProcessError: if git command returns error.
        commit_hash = None

    # Retrieve info about current branch.
    try:
        git_branch = subprocess.check_output(['git', 'symbolic-ref', '--short', 'HEAD'],
                                             stderr=subprocess.DEVNULL).decode().strip()
    except (FileNotFoundError, subprocess.CalledProcessError):
        # FileNotFoundError: if git is not on PATH.
        # subprocess.CalledProcessError: if git command returns error; for example, current checkout
        #   may have a detached HEAD pointing at a specific tag (not pointing to a branch).
        git_branch = None

    # Consolidate into single version string.
    if commit_hash is None:
        version = "unknown"
    elif git_branch is None:
        version = commit_hash
    else:
        version = "{}-{}".format(git_branch, commit_hash)

    return version


class PipelineBuildPyCommand(build_py):
    def run(self):
        build_py.run(self)
        self.run_command('minify_css')
        self.run_command('minify_js')
        self.run_command('buildmytasks')
        self.run_command('version')


# The pipeline.XXX.cli packages are not recognised by find_packages as they do
# not contain an __init__.py file. The __init__.py files are autogenerated as
# part of the buildmytasks command, and as we don't want autogenerated files
# to be committed, the cli/__init__.py files are added to .gitignore and are
# not part of a pristine checkout. This leaves us with a problem: we either
# need __init__.py to be created ahead of the find_packages() call, we add the
# unrecognised packages to the package list manually, or we use a package
# finder class with more relaxed heuristics about identifying packages. Here
# we do the latter; this might need updating to
# setuptool.find_namespace_packages when CASA's setuptools is updated.
packages = setuptools.PEP420PackageFinder().find(exclude=['build*', 'doc*'])


setuptools.setup(
    name='Pipeline',
    version='8.0.0',
    description='CASA pipeline',
    cmdclass={
        'buildmytasks': BuildMyTasksCommand,
        'build_py': PipelineBuildPyCommand,
        'minify_js': MinifyJSCommand,
        'minify_css': MinifyCSSCommand,
        'version': VersionCommand,
    },
    # install_requires=[
    #     'cachetools',       # memo decorator to cache expensive calls
    #     'intervaltree',     # callibrary
    #     'Mako',             # web log
    #     'pyparsing',        # parse user input using CASA syntax
    # ],
    setup_requires=[
        'csscompressor'  # minify CSS
    ],
    options=dict(egg_info=dict(tag_build='_{}'.format(_get_git_version()))),
    packages=packages,
    package_data={'': ['*.css',
                       '*.egg',
                       '*.eot',
                       '*.gif',
                       '*.html',
                       '*.jpg',
                       '*.js',
                       '*.mak',
                       '*.mako',
                       '*.otf',
                       '*.png',
                       '*.svg',
                       '*.ttf',
                       '*.txt',
                       '*.woff',
                       '*.woff2',
                       '*.xml']},
    zip_safe=False
)
