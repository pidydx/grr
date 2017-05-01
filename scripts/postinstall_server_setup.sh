#!/bin/bash

# This script will setup GRR server configuration files, launchers, and logs.
# It is called by debian/rules when building the deb package.

set -x
set -e

INSTALL_PREFIX="";
INSTALL_BIN="install";
INSTALL_OPTS="-p -m644";
SRC_DIR=".";
BUILDING_DEB=false
INSTALL_CMD="$INSTALL_BIN $INSTALL_OPTS"

function header()
{
  echo ""
  echo "##########################################################################################"
  echo "     ${*}";
  echo "##########################################################################################"
}

OPTIND=1
while getopts "h?dr:i:" opt; do
    case "$opt" in
    h|\?)
        echo "Usage: ./install_server_from_src.sh [OPTIONS]"
        echo " -r Path to GRR repository files"
        echo " -i Install prefix path"
        echo " -d Used f"
        exit 0
        ;;
    r)  SRC_DIR=$OPTARG;
        ;;
    i)  INSTALL_PREFIX=$OPTARG;
        ;;
    d)  BUILDING_DEB=true;
        ;;
    esac
done

shift $((OPTIND-1))
[ "$1" = "--" ] && shift

# Turn these into absolute paths.
cd "$INSTALL_PREFIX"
INSTALL_PREFIX=$PWD
cd -

cd "$SRC_DIR"
SRC_DIR=$PWD
cd -

SRC_DIR_BASE=$(basename "$SRC_DIR")
if [[ "$SRC_DIR_BASE" != "grr" ]]; then
  echo "Please run from the grr source directory or provide a valid path to the source directory with -r"
  exit 2
fi

LAUNCHER="$SRC_DIR/scripts/venv_launcher"

header "Install Configuration Files"
if [ "$BUILDING_DEB" = false ]; then
  # When running as a regular install $INSTALL_PREFIX must be cleared to install remaining files into the correct
  # target directories.
  INSTALL_PREFIX=""
  $INSTALL_CMD "$SRC_DIR/install_data/systemd/server/grr-server.service" "/lib/systemd/system/"
  $INSTALL_CMD "$SRC_DIR/install_data/etc/default/grr-server.default" "/etc/default/grr-server"
fi

# dh_installinit doesn't cater for systemd template files. The
# service target is installed by dh_installinit we just need to copy over the
# template.
$INSTALL_CMD "$SRC_DIR/install_data/systemd/server/grr-server@.service" "$INSTALL_PREFIX/lib/systemd/system/"

# Set up default configuration
mkdir -p "$INSTALL_PREFIX/etc/grr"

# When installed globally the config files are copied to the global
# configuration directory, except grr-server.yaml, which is effectively part of
# the code.
for f in $SRC_DIR/install_data/etc/*.yaml; do
  if [ "$f" != "$SRC_DIR/install_data/etc/grr-server.yaml" ]; then
    $INSTALL_CMD "$f" "$INSTALL_PREFIX/etc/grr/"
  fi
done

header "Install Launcher Files"
INSTALL_OPTS="-p -m755";
INSTALL_CMD="$INSTALL_BIN $INSTALL_OPTS"
mkdir -p "$INSTALL_PREFIX/usr/bin/"

$INSTALL_CMD $LAUNCHER "$INSTALL_PREFIX/usr/bin/grr_config_updater"
$INSTALL_CMD $LAUNCHER "$INSTALL_PREFIX/usr/bin/grr_console"
$INSTALL_CMD $LAUNCHER "$INSTALL_PREFIX/usr/bin/grr_server"
$INSTALL_CMD $LAUNCHER "$INSTALL_PREFIX/usr/bin/grr_export"
$INSTALL_CMD $LAUNCHER "$INSTALL_PREFIX/usr/bin/grr_fuse"

# Set up log directory
mkdir -p "$INSTALL_PREFIX"/var/log/grr

header "Install Complete"
