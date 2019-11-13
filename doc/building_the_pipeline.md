# Building the pipeline
## Prerequisites
The pipeline build procedure depends on Python 3, CASA buildmytasks, and Java,
which is used by buildmytasks to process the task XML and to minify the
Pipeline Javascript.

It is recommended to put CASA `bin` directory first on your path for the
duration of the installation procedure. 

## Standard build
The pipeline can be built and installed like any standard Python module, with
```
$ python3 setup.py build
```
The build output will be found in the build/lib directory. This directory can
be put on the PYTHONPATH inside CASA to make the pipeline available, e.g.,
```python
import sys
sys.path.insert(0, '/path/to/pipeine/build/lib')
```
  

## Standard install
The pipeline can be built and installed like any standard Python module, with
```
$ python3 setup.py install
```
If a pipeline egg is already installed, this command will upgrade the 
pipeline with the new installation. 

## Temporary install
To build a pipeline .egg file without installing the egg and hence overwriting
the CASA default pipeline installation, execute 
```
$ python setup.py bdist_egg
```
The resulting egg file can be found in the dist directory and added to the
CASA sys.path in your CASA prelude, e.g.,
```python
import sys
sys.path.insert(0, '/path/to/workspace/dist/Pipeline.egg')
```

### Switching between pipeline versions
Developers often have multiple workspaces, each workspace containing a
different version of the pipeline. Below is an example prelude.py which
switches between workspaces based on the launch arguments given to CASA, e.g.,
`casa --trunk --egg` makes the most recent pipeline egg from the _trunk_ 
workspace available. Edit the workspaces dictionary definition to match your
environment. 
```python
#
#  CASA prelude to switch between development environments and eggs
# 
# casa --trunk         : puts the 'trunk' workspace directory first on the CASA
#                        path
# casa --trunk --egg   : put the most recent egg from the trunk workspace first  
#                        on the CASA path
import os.path
import sys

# edit workspaces to match your environment. The dictionary keys become the 
# recognised CASA command line arguments.
workspaces = {
    'trunk': '~/alma/pipeline/svn/pristine/pipeline',
    'sessions': '~/alma/pipeline/svn/pristine/pipeline-feature-sessions',
}

def find_most_recent_egg(directory):
    # list all the egg files in the directory..
    files = [f for f in os.listdir(directory) if f.endswith('.egg')]

    # .. and from these matches, create a dict mapping files to their
    # modification timestamps, ..
    name_n_timestamp = dict([(f, os.stat(os.path.join(directory,f)).st_mtime) for f in files])

    # .. then return the file with the most recent timestamp
    return max(name_n_timestamp, key=name_n_timestamp.get)


def get_egg(path):
    dist_dir = os.path.join(path, 'dist')
    try:
        egg = find_most_recent_egg(dist_dir)
    except OSError:
        msg = 'Error: no pipeline egg found in {!s}\n'.format(dist_dir)
        sys.stderr.writelines(msg)
        return None
    else:
        return os.path.join(dist_dir, egg)


for k, workspace_path in workspaces.items():
    full_path = os.path.expanduser(workspace_path)
    if '--' + k in sys.argv:
        if '--egg' in sys.argv:
            entry_to_add = get_egg(full_path)
            entry_type = 'egg'
        else:
            entry_to_add = full_path
            entry_type = 'directory'
        if entry_to_add:
            msg = 'Adding {!s} to CASA PYTHONPATH: {!s}\n'.format(entry_type, entry_to_add)
            sys.stdout.writelines(msg)
            sys.path.insert(0, entry_to_add)

```

## Developer install
As a developer, you will quickly grow weary of creating an egg every time you
wish to exercise new code. The pipeline supports developer installations. In
this mode, a pseudo installation is made which adds your source directory to
the CASA site-packages. Hence the working version you are editing will become
the pipeline version available to CASA.
```
$ python3 setup.py develop
```
To uninstall the developer installation, execute
```
$ python3 setup.py develop -u
```

### Optional: CASA CLI bindings
The CASA CLI bindings are always generated and included in a standard install.
To make the CASA CLI bindings available for a developer install, the CLI 
bindings need to be written to the src directory. This can be done using the
`buildmytasks` command, using the _-i_ option to generate the bindings 
in-place, i.e., 
```
$ python3 setup.py buildmytasks -i
```
The bindings should be rebuilt whenever you change the interface XML definitions.

__Take care not to commit the code-generated files to SVN!__

### Optional: removing legacy pipeline installation from CASA
To prevent any possible conflict between legacy pipeline installation and new
pipeline code, the legacy pipeline installation should be removed from CASA. 
Execute:
```
casa-config --sh-exec rm '$PYTHONHOME/pipeline'
``` 
