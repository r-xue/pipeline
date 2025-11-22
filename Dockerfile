# syntax=docker/dockerfile:1
FROM ubuntu:24.04

# 1. System Setup
# Define all environment variables in one place
ENV DEBIAN_FRONTEND=noninteractive \
    LANG=C.UTF-8 \
    LC_ALL=C.UTF-8 \
    CONDA_DIR=/opt/conda \
    PIP_NO_CACHE_DIR=off \
    DISPLAY=:0

# Install only the bare minimum to download and run the Miniforge installer.
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
        bash wget ca-certificates git xvfb nano less rsync \
        libfuse2 fuse libxcb-cursor0 libglib2.0-0t64 libfontconfig1 libfreetype6 \
        libsm6 libxext6 libxml2-utils libxrender1 && \
    apt-get autoremove -y && \ 
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

# 2. Install Miniforge (Mamba)

RUN wget -qO miniforge.sh "https://github.com/conda-forge/miniforge/releases/latest/download/Miniforge3-Linux-x86_64.sh" && \
    bash miniforge.sh -b -p "${CONDA_DIR}" && \
    rm miniforge.sh && \
    "${CONDA_DIR}/bin/conda" clean -afy

# Add conda to path temporarily for the build
ENV PATH="${CONDA_DIR}/bin:${PATH}"

# 3. Create Python Environment and install the CASA6 Package Stack
COPY environment.yml requirements.txt ./
RUN --mount=type=cache,target=/opt/conda/pkgs \
    --mount=type=cache,target=/root/.cache/pip \
    mamba env create -f environment.yml --yes && \
    mamba clean --all --yes

# Set the PATH to automatically "activate" the pipeline environment
ENV PATH="${CONDA_DIR}/envs/pipeline/bin:${PATH}" \
    CONDA_DEFAULT_ENV=pipeline

# 4. Final Configuration
# Create a non-root user
RUN useradd -m -s /bin/bash pipeline && \
    mkdir -p /home/pipeline/.casa/data && \
    chown -R pipeline:pipeline /home/pipeline

USER pipeline
WORKDIR /home/pipeline

CMD ["bash"]