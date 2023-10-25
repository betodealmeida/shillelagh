#!/bin/bash
set -e  # If any command fails, stop the script

# create virtualenv
cd
python3 -m venv venv
source venv/bin/activate
pip3 install 'setuptools>=61' wheel

# install shillelagh
cd /src
pip3 install -v -e '.[all]'

# install multicorn2
rm -rf multicorn2
git clone https://github.com/pgsql-io/multicorn2.git
cd multicorn2
git checkout v2.5
pip3 install .

# call the original entrypoint
exec docker-entrypoint.sh postgres
