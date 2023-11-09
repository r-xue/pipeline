# CASA API dependency

The markdown file [casa_api.md](casa_api.md) provides the list of CASA APIs used by Pipeline in the production monolithic CASA setup.

# requirements.txt

In [requirements.txt](requirements.txt), we specify the 3rd-party Python packages/modules required by Pipeline in the CASA/Pipeline monolithic releases. This list excludes the dependencies already install in the latest monolith CASA release.

# Conda environment

The environment file ([pipeline.yaml](pipeline.yaml)) is provided to specify a minimal Conda environment that is suitable for running the modular CASA and Pipeline from a standard Python interpreter. *Note: this method is not officially supported and tested, and should only be used for local development purposes.*

# PIPE-938

[PIPE938.md](PIPE938.md) provides an update-to-date full list of Pipeline dependencies, including CASA APIs, 3rd-party Python packages/modules, etc.
