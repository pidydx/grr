#!/bin/bash

# This script will install GRR and python dependencies in a venv.
# It is called by docker and vagrant for installing.

set -x
set -e


INSTALL_PREFIX="";
SRC_DIR=".";
NODEENV_INSTALL=false
EDITABLE=false
SDIST=false
PYPI=false

function header()
{
  echo ""
  echo "##########################################################################################"
  echo "     ${*}";
  echo "##########################################################################################"
}

OPTIND=1
while getopts "h?espr:i:" opt; do
    case "$opt" in
    h|\?)
        echo "Usage: ./install_server_from_src.sh [OPTIONS]"
        echo " -r Path to GRR repository files"
        echo " -i Install prefix path"
        echo " -e Install server as pip editable"
        echo " -p Install server from pypi"
        echo " -s Build and install server sdists"
        exit 0
        ;;
    r)  SRC_DIR=$OPTARG;
        ;;
    i)  INSTALL_PREFIX=$OPTARG;
        ;;
    e)  NODEENV_INSTALL=true;
        EDITABLE=true;
        ;;
    s)  NODEENV_INSTALL=true;
        SDIST=true;
        ;;
    p)  PYPI=true;
        ;;
    esac
done

shift $((OPTIND-1))
[ "$1" = "--" ] && shift

# Create virtualenv
virtualenv $INSTALL_PREFIX
source "$INSTALL_PREFIX/bin/activate"

# Turn these into absolute paths.
cd "$INSTALL_PREFIX"
INSTALL_PREFIX=$PWD
cd -

cd "$SRC_DIR"
SRC_DIR=$PWD
cd -

# Create wheelhouse to speed up builds
mkdir -p /wheelhouse
pip wheel --find-links=/wheelhouse --wheel-dir=/wheelhouse --pre grr-response-server
pip wheel --find-links=/wheelhouse --wheel-dir=/wheelhouse -f https://storage.googleapis.com/releases.grr-response.com/index.html grr-response-templates

# This sets up NodeJS inside the virtualenv. NodeJS is needed to compile AdminUI JS/CSS files.
# The virtualenv must be reactivated after install.
if [ "$NODEENV_INSTALL" = true ]; then
  echo '{ "allow_root": true }' > /root/.bowerrc
  pip install nodeenv
  nodeenv -p --prebuilt
  source "$INSTALL_PREFIX/bin/activate"
fi

if [ "$SDIST" = true ]; then
  cd $SRC_DIR
  python $SRC_DIR/api_client/python/setup.py sdist --dist-dir="/sdists/api_client"
  python $SRC_DIR/setup.py sdist --dist-dir="/sdists/core" --no-make-docs
  python $SRC_DIR/grr/config/grr-response-client/setup.py sdist --dist-dir="/sdists/client"
  python $SRC_DIR/grr/config/grr-response-server/setup.py sdist --dist-dir="/sdists/server"
  # Make the grr-response-test sdist but do not install it by default
  python $SRC_DIR/grr/config/grr-response-test/setup.py sdist --dist-dir="/sdists/test"
  pip install --find-links=/wheelhouse /sdists/api_client/*.tar.gz
  pip install --no-binary protobuf --find-links=/wheelhouse /sdists/core/*.tar.gz
  pip install --find-links=/wheelhouse /sdists/client/*.tar.gz
  pip install --find-links=/wheelhouse /sdists/server/*.tar.gz
  pip install --find-links=/wheelhouse grr_response_templates
  exit $?
fi

if [ "$EDITABLE" = true ]; then
  cd $SRC_DIR
  pip install --find-links=/wheelhouse --editable api_client/python
  pip install --no-binary protobuf --find-links=/wheelhouse --editable .
  pip install --find-links=/wheelhouse --editable grr/config/grr-response-client
  pip install --find-links=/wheelhouse --editable grr/config/grr-response-server
  pip install --find-links=/wheelhouse --editable grr/config/grr-response-test
  pip install --find-links=/wheelhouse grr_response_templates
  exit $?
fi

if [ "$PYPI" = true ]; then
  pip install --no-binary protobuf --find-links=/wheelhouse --pre grr-response-server
  pip install --find-links=/wheelhouse -f https://storage.googleapis.com/releases.grr-response.com/index.html grr-response-templates
  exit $?
fi





