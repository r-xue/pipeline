"""Generate autosummary stubs for CLI task functions.

Sphinx autosummary only generates stubs for items listed directly in
source ``.. autosummary:: :toctree:`` directives.  It does NOT process
nested ``:toctree:`` inside stubs it just generated, **and** it strips
directive options (like ``:toctree:`` / ``:nosignatures:``) and extra
blocks (like ``.. toctree:: :hidden:``) from template-generated stubs.

This extension works around both limitations:

1. Hook ``builder-inited`` with priority 900 (after autosummary at 500)
   so the module stubs already exist.
2. Introspect each CLI module to discover public functions.
3. Generate an individual ``.rst`` stub for every function.
4. Append a ``.. toctree:: :hidden:`` block to each module stub so that
   the sidebar shows functions nested under their parent module.
"""

import importlib
import inspect
import logging
from pathlib import Path

from sphinx.application import Sphinx

logger = logging.getLogger(__name__)

#: CLI modules whose public functions should get individual stubs.
CLI_MODULES = [
    'pipeline.h.cli',
    'pipeline.hif.cli',
    'pipeline.hifa.cli',
    'pipeline.hifv.cli',
    'pipeline.hsd.cli',
    'pipeline.hsdn.cli',
]

#: Directory (relative to source) where stubs are written.
AUTOSUMMARY_DIR = '_autosummary'

#: Template for individual function stubs.
STUB_TEMPLATE = """\
{title}
{underline}

.. currentmodule:: {module}

.. autofunction:: {name}
"""

#: Marker so we only patch a module stub once.
_TOCTREE_MARKER = '.. cli_function_stubs toctree'


def _discover_functions(mod_name: str) -> list[str]:
    """Return sorted list of public function names exported by *mod_name*."""
    try:
        mod = importlib.import_module(mod_name)
    except ImportError:
        logger.warning('cli_function_stubs: could not import %s', mod_name)
        return []

    names = []
    for name, obj in inspect.getmembers(mod, inspect.isfunction):
        if name.startswith('_'):
            continue
        func_module = getattr(obj, '__module__', '') or ''
        if func_module.startswith(mod_name):
            names.append(name)
    return sorted(names)


def _generate_cli_function_stubs(app: Sphinx) -> None:
    """Generate function stubs and patch module stubs with a hidden toctree."""
    srcdir = Path(app.srcdir)
    outdir = srcdir / AUTOSUMMARY_DIR
    outdir.mkdir(exist_ok=True)

    overwrite = getattr(app.config, 'autosummary_generate_overwrite', True)

    for mod_name in CLI_MODULES:
        func_names = _discover_functions(mod_name)
        if not func_names:
            continue

        # --- 1. Generate individual function stubs ---
        for name in func_names:
            stub_path = outdir / f'{mod_name}.{name}.rst'
            if stub_path.exists() and not overwrite:
                continue

            escaped = name.replace('_', r'\_')
            content = STUB_TEMPLATE.format(
                title=escaped,
                underline='=' * len(escaped),
                module=mod_name,
                name=name,
            )
            stub_path.write_text(content)

        # --- 2. Patch module stub with hidden toctree ---
        mod_stub = outdir / f'{mod_name}.rst'
        if not mod_stub.exists():
            continue

        text = mod_stub.read_text()
        if _TOCTREE_MARKER in text:
            continue  # already patched

        toctree_entries = '\n'.join(f'   {mod_name}.{n}' for n in func_names)
        patch = (
            f'\n.. {_TOCTREE_MARKER}\n'
            f'.. toctree::\n'
            f'   :hidden:\n\n'
            f'{toctree_entries}\n'
        )
        mod_stub.write_text(text + patch)


def setup(app: Sphinx) -> dict:
    app.connect('builder-inited', _generate_cli_function_stubs, priority=900)
    return {'version': '0.1', 'parallel_read_safe': True}
