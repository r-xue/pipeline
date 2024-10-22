#!/usr/bin/env python
#
# This file is execfile()d with the current directory set to its
# containing dir.
#
# Note that not all possible configuration values are present in this
# autogenerated file.
#
# All configuration values have a default; values that are commented out
# serve to show the default.

# If extensions (or modules to document with autodoc) are in another
# directory, add these directories to sys.path here. If the directory is
# relative to the documentation root, use os.path.abspath to make it
# absolute, like shown here.
#
#

from datetime import datetime
import os
import sys
import textwrap

pipeline_src = os.getenv('pipeline_src')
if pipeline_src is not None:
    # Use the env variable "pipeline_src" to look for Pipeline if it's specified.
    sys.path.insert(0, os.path.abspath(pipeline_src))
else:
    # Use the ancestry path if "pipeline_src" is not set.
    sys.path.insert(0, os.path.abspath('../../'))

try:
    import pipeline

    from pipeline.infrastructure.renderer.regression import get_all_subclasses
    from pipeline.infrastructure.api import Task
    from pipeline.infrastructure.api import Inputs
    from pipeline.infrastructure.api import Results
    from pipeline.h.tasks import ImportData

    taskclasses_str = [ret0.__module__+'.'+ret0.__name__ for ret0 in get_all_subclasses(Task)]
    inputsclasses_str = [ret0.__module__+'.'+ret0.__name__ for ret0 in get_all_subclasses(Inputs)]
    resultsclasses_str = [ret0.__module__+'.'+ret0.__name__ for ret0 in get_all_subclasses(Results)]
    importdataclasses_str = [ret0.__module__+'.'+ret0.__name__ for ret0 in get_all_subclasses(ImportData)]

    # create custom directives to create inheritance diagrams for all Task/Inputs Classes
    #   https://docutils.sourceforge.io/docs/ref/rst/restructuredtext.html#substitution-definitions
    #   https://www.sphinx-doc.org/en/master/usage/configuration.html#confval-rst_prolog

    rst_epilog = r"""
        .. |taskclasses_diagram| inheritance-diagram:: {}
            :parts: -1
        .. |inputsclasses_diagram| inheritance-diagram:: {}
            :parts: -1
        .. |resultsclasses_diagram| inheritance-diagram:: {}
            :parts: -1
        .. |importdataclasses_diagram| inheritance-diagram:: {}
            :parts: -1
        """.format(' '.join(taskclasses_str),
                   ' '.join(inputsclasses_str),
                   ' '.join(resultsclasses_str),
                   ' '.join(importdataclasses_str))
    rst_epilog = textwrap.dedent(rst_epilog)

except ImportError as error:
    print("Can't import Pipeline, but we will continue to build the docs.")
    print(error.__class__.__name__ + ": " + error.message)
    pass

# -- General configuration ---------------------------------------------

# If your documentation needs a minimal Sphinx version, state it here.
#
# needs_sphinx = '1.0'

# Add any Sphinx extension module names here, as strings. They can be
# extensions coming with Sphinx (named 'sphinx.ext.*') or your custom ones.
extensions = ['sphinx.ext.autodoc',
              # 'autoapi.extension',
              'sphinx.ext.autosectionlabel',
              'sphinx.ext.autosummary',
              'sphinx.ext.todo',
              'sphinx_markdown_tables',
              #              'sphinx.ext.coverage',
              'sphinx.ext.imgconverter',
              'sphinx.ext.mathjax',
              'sphinx.ext.napoleon',
              'sphinx.ext.coverage',
              'sphinx.ext.githubpages',
              'sphinx.ext.intersphinx',
              'sphinx.ext.inheritance_diagram',
              'sphinx_automodapi.automodapi',
              'sphinx_automodapi.smart_resolver',
              'sphinxcontrib.bibtex',
              'sphinx_astrorefs',
              'recommonmark',
              'sphinx.ext.graphviz',
              'sphinx.ext.viewcode',
              'nbsphinx',
              'IPython.sphinxext.ipython_console_highlighting',
              'IPython.sphinxext.ipython_directive']

add_module_names = False

# Add any paths that contain templates here, relative to this directory.
templates_path = ['_templates']

# The suffix(es) of source filenames.
# You can specify multiple suffix as a list of string:
#

# source_suffix = ['.rst', '.md']
source_suffix = '.rst'

# The master toctree document.
master_doc = 'index'

# General information about the project.
project = 'Pipeline'
author = "Pipeline Dev. Team"
copyright = u'2020–{0}, '.format(datetime.utcnow().year) + author

# The version info for the project you're documenting, acts as replacement
# for |version| and |release|, also used in various other places throughout
# the built documents.
#
# The short X.Y version.
# version = pipeline.__version__
# The full version, including alpha/beta/rc tags.
# release = pipeline.__version__

release = pipeline.environment.pipeline_revision
version = release.split('+')[0]

# The language for content autogenerated by Sphinx. Refer to documentation
# for a list of supported languages.
#
# This is also used if you do content translation via gettext catalogs.
# Usually you set "language" from the command line for these cases.
language = 'en'

# List of patterns, relative to source directory, that match files and
# directories to ignore when looking for source files.
# This patterns also effect to html_static_path and html_extra_path
exclude_patterns = [
    '_build',
    'Thumbs.db',
    '.DS_Store']

# The name of the Pygments (syntax highlighting) style to use.
# pygments_style = 'sphinx'
pygments_style = 'default'

# If true, `todo` and `todoList` produce output, else they produce nothing.
todo_include_todos = False


# -- Options for HTML output -------------------------------------------

# The theme to use for HTML and HTML Help pages.  See the documentation for
# a list of builtin themes.
#
html_theme = 'sphinx_rtd_theme'  # "furo"

# Theme options are theme-specific and customize the look and feel of a
# theme further.  For a list of options available for each theme, see the
# documentation.
#
# html_theme_options = {}

# Add any paths that contain custom static files (such as style sheets) here,
# relative to this directory. They are copied after the builtin static files,
# so a file named "default.css" will overwrite the builtin "default.css".
html_static_path = ['_static']


# -- Options for HTMLHelp output ---------------------------------------

# Output file base name for HTML help builder.
htmlhelp_basename = 'pipeline_doc'

html_baseurl = ''

# -- Options for LaTeX output ------------------------------------------

latex_elements = {
    # The paper size ('letterpaper' or 'a4paper').
    #
    'papersize': 'letterpaper',

    # The font size ('10pt', '11pt' or '12pt').
    #
    'pointsize': '10pt',

    # Additional stuff for the LaTeX preamble.
    #
    'preamble': r'''
  \usepackage{hyperref}
  \usepackage{longtable}
  \setcounter{tocdepth}{1}
  \protected\def\sphinxcode#1{\textcolor{red}{\texttt{#1}}}
  \makeatletter
  \renewcommand{\sphinxtableofcontents}{%
    \pagenumbering{roman}%
    \begingroup
      \parskip \z@skip
      \sphinxtableofcontentshook
      \tableofcontents
    \endgroup
    % before resetting page counter, let's do the right thing.
    \if@openright\cleardoublepage\else\clearpage\fi
    \pagenumbering{arabic}% 
    \makeatother
   }
''',

    # Latex figure (float) alignment
    #
    # 'figure_align': 'htbp',

    # remove blank pages (between the title page and the TOC, etc.)
    #
    'classoptions': ',openany,oneside',
    'babel': '\\usepackage[english]{babel}',
}

# If false, no module index is generated.
latex_use_modindex = True

# Grouping the document tree into LaTeX files. List of tuples
# (source start file, target name, title, author, documentclass
# [howto, manual, or own class]).
latex_documents = [
    ('_taskdocs/taskdocs', 'taskdocs.tex',
     'Pipeline Tasks Reference Manual',
     'pipeline team', 'manual'),
]


# -- Options for manual page output ------------------------------------

# One entry per manual page. List of tuples
# (source start file, name, description, authors, manual section).
man_pages = [
    (master_doc, 'pipeline',
     'Pipeline Documentation',
     [author], 1)
]


# -- Options for Texinfo output ----------------------------------------

# Grouping the document tree into Texinfo files. List of tuples
# (source start file, target name, title, author,
#  dir menu entry, description, category)
texinfo_documents = [
    (master_doc, 'pipeline',
     'Pipeline @ NRAO Documentation',
     author,
     'pipeline',
     'placeholder',
     'Miscellaneous'),
]

# -- Sidebars

html_sidebars = {
    '**': ['localtoc.html'],  # not allowed if using the 'furo' theme
    'search': [],
    'genindex': [],
    'py-modindex': [],
}

# -- napoleon

napoleon_google_docstring = True
apoleon_numpy_docstring = False
napoleon_use_param = False
napoleon_use_ivar = True


verbatimwrapslines = False
html_show_sourcelink = True
# Temporarily disable autosummary so that links to individual pipeline tasks works as expected for the
# reference manual
autosummary_generate = True
autosummary_generate_overwrite = True
autosummary_imported_members = True
autosummary_ignore_module_all = True
# autodoc_mock_imports = ["pipeline"]
# autodoc_default_options = ['members']

autodoc_default_options = {
    # other options
    'show-inheritance': True
}

# sphinx-automodapi: https://sphinx-automodapi.readthedocs.io
numpydoc_show_class_members = False
automodapi_toctreedirnm = '_automodapi'

# -- intersphinx

# intersphinx_mapping = {
#     'python': ('https://docs.python.org/3', None),
#     'astropy':  ('http://docs.astropy.org/en/stable', None),
# }


# intersphinx_mapping = {'python': ('https://docs.python.org/3', None)}
# intersphinx_mapping['astropy'] = ('http://docs.astropy.org/en/latest/', None)
# intersphinx_mapping['pyerfa'] = ('https://pyerfa.readthedocs.io/en/stable/', None)
# intersphinx_mapping['pytest'] = ('https://pytest.readthedocs.io/en/stable/', None)
# intersphinx_mapping['ipython'] = ('https://ipython.readthedocs.io/en/stable/', None)
# intersphinx_mapping['pandas'] = ('https://pandas.pydata.org/pandas-docs/stable/', None)
# intersphinx_mapping['sphinx_automodapi'] = ('https://sphinx-automodapi.readthedocs.io/en/stable/', None)
# intersphinx_mapping['packagetemplate'] = ('http://docs.astropy.org/projects/package-template/en/latest/', None)
# intersphinx_mapping['h5py'] = ('http://docs.h5py.org/en/stable/', None)


def setup(app):
    app.add_css_file('custom_theme.css')


# sphinx-astrorefs
bibtex_bibfiles = ['references/pipeline.bib']
bibtex_encoding = "utf-8"
astrorefs_resolve_aas_macros = True
astrorefs_resolve_aas_macros_infile = 'references/pipeline.bib'
astrorefs_resolve_aas_macros_outfile = 'references/pipeline-resolved.bib'


# inheritance_graph_attrs = dict(rankdir="TB", size='""')


autoapi_dirs = ['../../pipeline']

inheritance_graph_attrs = {
    'rankdir': 'LR',
    'size': '"25.0, 50.0"',
    'bgcolor': 'whitesmoke',
}

inheritance_node_attrs = {
    'shape': 'box',
    'fontsize': 12,
    'height': 0.25,
    'fontname': '"Vera Sans, DejaVu Sans, Liberation Sans, '
    'Arial, Helvetica, sans"',
    'style': '"setlinewidth(0.5),filled"',
    'fillcolor': 'white',
}

inheritance__edge_attrs = {
    'arrowsize': 0.5,
    'style': '"setlinewidth(0.5)"',
}
