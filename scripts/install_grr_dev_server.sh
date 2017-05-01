#!/bin/bash
#
# Script to set up a GRR editable install in vagrant for development purposes
#

INSTALL_PREFIX="";
SRC_DIR=".";

OPTIND=1
while getopts "h?r:i:" opt; do
    case "$opt" in
    h|\?)
        echo "Usage: ./install_server_from_src.sh [OPTIONS]"
        echo " -r Path to GRR repository files"
        echo " -i Install prefix path"
        exit 0
        ;;
    r)  SRC_DIR=$OPTARG;
        ;;
    i)  INSTALL_PREFIX=$OPTARG;
        ;;
    esac
done

shift $((OPTIND-1))
[ "$1" = "--" ] && shift

INSTALL_CMD="$INSTALL_BIN $INSTALL_OPTS"

$SRC_DIR/scripts/install_dependencies.sh -c -p

$SRC_DIR/scripts/install_grr_server_venv.sh -e -i $INSTALL_PREFIX -r $SRC_DIR

$SRC_DIR/scripts/postinstall_server_setup.sh -r $SRC_DIR -i $INSTALL_PREFIX

