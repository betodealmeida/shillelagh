#!/bin/bash
# This script is meant to be called by the "install" step defined in
# .travis.yml. See http://docs.travis-ci.com/ for more details.
# The behavior of the script is controlled by environment variabled defined
# in the .travis.yml in the top level folder of the project.
#
# This script is inspired by Scikit-Learn (http://scikit-learn.org/)
#
# THIS SCRIPT IS SUPPOSED TO BE AN EXAMPLE. MODIFY IT ACCORDING TO YOUR NEEDS!

set -e

if [[ "$DISTRIB" == "conda" ]]; then
    # Deactivate the travis-provided virtual environment and setup a
    # conda-based environment instead
    deactivate

    if [[ -f "$HOME/miniconda/bin/conda" ]]; then
        echo "** Skipping install conda [cached]"
    else
        echo "** Installing conda ..."
        # By default, travis caching mechanism creates an empty dir in the
        # beginning of the build, but conda installer aborts if it finds an
        # existing folder, so let's just remove it:
        rm -rf "$HOME/miniconda"

        # Use the miniconda installer for faster download / install of conda
        # itself
        wget https://repo.anaconda.com/miniconda/Miniconda3-latest-Linux-x86_64.sh -O miniconda.sh
        chmod +x miniconda.sh && ./miniconda.sh -b -p "$HOME/miniconda"
    fi
    export PATH=$HOME/miniconda/bin:$PATH

    CONDA_INIT="$(conda info --root)/etc/profile.d/conda.sh"
    if [[ -f "$CONDA_INIT" ]]; then
        source "$(conda info --root)/etc/profile.d/conda.sh"
        # ^  In an iterative shell we would use `conda init bash`, this is an
        #    workaround described in:
        #    https://docs.conda.io/projects/conda/en/latest/user-guide/tasks/use-conda-with-travis-ci.html
    else
        echo "File $CONDA_INIT not found."
        echo "It is likely that the current version of conda changed its 'conda init' procedure."
        echo "Please check conda's docs and update this script accordingly"
    fi

    echo "** Making sure to use the most updated version ..."
    conda update --yes conda

    # (prefer local venv, since the miniconda folder is cached)
    echo "** Creating conda environment ..."
    conda create -p ./.venv  -c conda-forge --yes python=${PYTHON_VERSION} pip setuptools virtualenv tox
    conda activate ./.venv
else
    pip install -U pip setuptools virtualenv tox
fi

if [[ "$COVERAGE" == "true" ]]; then
    pip install -U pytest-cov pytest-virtualenv coverage coveralls codecov flake8 pre-commit
fi

travis-cleanup() {
    printf "** Cleaning up environments ... "  # printf avoids new lines
    if [[ "$DISTRIB" == "conda" ]]; then
        # Force the env to be recreated next time, for build consistency
        conda deactivate
        conda remove -p ./.venv --all --yes
        rm -rf ./.venv
    fi
    echo "DONE"
}
