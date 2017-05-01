#!/bin/bash

# This script will install GRR dependencies.
# It is called by docker and vagrant for preparing installs

set -x
set -e

PROTOC_PKG="protoc-3.2.0-linux-x86_64.zip"
PROTOC_URL="https://github.com/google/protobuf/releases/download/v3.2.0/${PROTOC_PKG}"
PROTOC=false

# The 32bit libs listed here are required for PyInstaller, which isn't used on
# the server but picked up by the deb library checker as missing. PyInstaller is
# a client python package dependency and we need that package for the
# ClientBuild entry point for repacking. Disabling the library check might
# result in missing something important. Adding the build deps here seems like
# the least bad option.
# The packaging tools here are used to repack the linux client installers.

BASE_PKGS="debhelper dpkg-dev prelink python2.7-dev python-pip rpm wget zip"
BUILD_PIP_PKGS="default-jre git libffi-dev libssl-dev"
BUILD_DEB_PKGS="dh-systemd dh-virtualenv lib32z1 libc6-i386"

function header()
{
  echo ""
  echo "##########################################################################################"
  echo "     ${*}";
  echo "##########################################################################################"
}

INSTALL_PKGS=$BASE_PKGS

OPTIND=1
while getopts "h?cpd" opt; do
    case "$opt" in
    h|\?)
        echo "Usage: ./install_server_from_src.sh [OPTIONS]"
        echo "Installs only the base requirements to install from deb when no options provided."
        echo " -c Install protobuf compiler in /usr/local"
        echo " -p Install dependencies to build pip packages"
        echo " -d Install dependencies build deb packages"
        exit 0
        ;;
    c)  PROTOC=true;
        ;;
    p)  INSTALL_PKGS="$INSTALL_PKGS $BUILD_PIP_PKGS";
        ;;
    d)  INSTALL_PKGS="$INSTALL_PKGS $BUILD_PIP_PKGS $BUILD_DEB_PKGS";
        ;;
    esac
done

shift $((OPTIND-1))
[ "$1" = "--" ] && shift

# Install dependencies
header "Install Dependencies"
apt-get --yes update
apt-get install -y $INSTALL_PKGS
pip install --upgrade pip
pip install --upgrade setuptools
pip install virtualenv
pip install wheel

if [ "$PROTOC" = true ]; then
  # Install required protobuf compiler
  header "Install protobuf compiler"
  cd /usr/local
  wget -N --quiet "$PROTOC_URL"
  unzip -o $PROTOC_PKG
  rm $PROTOC_PKG readme.txt
fi