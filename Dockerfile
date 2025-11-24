# syntax=docker/dockerfile:1
FROM ubuntu:24.04

# 1. System Setup
# Define environment variables
ENV DEBIAN_FRONTEND=noninteractive \
    LANG=C.UTF-8 \
    LC_ALL=C.UTF-8 \
    CONDA_DIR=/opt/conda \
    PIP_NO_CACHE_DIR=off \
    DISPLAY=:0 \
    # Add conda bin to path immediately
    PATH="/opt/conda/envs/pipeline/bin:$PATH" \
    CONDA_DEFAULT_ENV=pipeline

# Create the user EARLY so we can assign permissions before filling directories
RUN useradd -m -s /bin/bash pipeline && \
    mkdir -p "${CONDA_DIR}" && \
    chown -R pipeline:pipeline "${CONDA_DIR}"

# Install system dependencies and Micromamba
# Added 'SHELL' pipefail protection to catch curl|tar errors
SHELL ["/bin/bash", "-o", "pipefail", "-c"]
RUN --mount=type=cache,target=/var/cache/apt,sharing=locked \
    --mount=type=cache,target=/var/lib/apt/lists,sharing=locked \
    apt-get update && \
    apt-get install -y --no-install-recommends \
    bash curl bzip2 ca-certificates git xvfb nano less rsync \
    libfuse2 fuse libxcb-cursor0 libglib2.0-0t64 libfontconfig1 libfreetype6 \
    libsm6 libxext6 libxml2-utils libxrender1 && \
    # Install micromamba to global path
    curl -Ls https://micro.mamba.pm/api/micromamba/linux-64/latest | tar -xj -C /bin/ --strip-components=1 bin/micromamba && \
    apt-get autoremove -y && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

# 2. Create Python Environment
# Switch to the user BEFORE installing the environment to avoid 'chown -R' later
USER pipeline
WORKDIR /home/pipeline

# Use --chown to prevent files being owned by root during COPY
COPY --chown=pipeline:pipeline environment.yml requirements.txt ./

RUN --mount=type=cache,target=${CONDA_DIR}/pkgs,sharing=locked,uid=1000 \
    --mount=type=cache,target=/home/pipeline/.cache/pip,sharing=locked,uid=1000 \
    micromamba env create -f environment.yml --yes && \
    micromamba clean --all --yes && \
    # Clean up artifacts to reduce size
    find ${CONDA_DIR} -follow -type f -name '*.a' -delete && \
    find ${CONDA_DIR} -follow -type f -name '*.pyc' -delete && \
    find ${CONDA_DIR} -follow -type f -name '*.js.map' -delete && \
    find ${CONDA_DIR} -follow -type f -name "*.lock" -delete && \
    rm environment.yml requirements.txt

# 3. Final Configuration
# Create CASA data directory (as user)
RUN mkdir -p /home/pipeline/.casa/data

CMD ["bash"]