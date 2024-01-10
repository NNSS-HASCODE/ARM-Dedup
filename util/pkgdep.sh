#!/bin/bash
# Please run this script as root.

SYSTEM=`uname -s`

if [[ -f /etc/debian_version ]]; then
    apt-get update
    apt-get install -y gcc pkg-config git libglib2.0-dev libfdt-dev libpixman-1-dev zlib1g-dev
    apt-get install -y libaio-dev python exuberant-ctags

    apt-get install -y libncurses5-dev libelf-dev
    apt-get install -y libnuma-dev
    apt-get install -y libcap-ng-dev libcap-dev libcap-ng-utils
    apt-get install -y libattr1 libattr1-dev

    apt-get install -y build-essential bison flex libssl-dev libelf-dev libcap-dev libattr1-dev libncurses-dev

else
    echo "pkgdep: unsupported system type ($SYSTEM), please install QEMU and Linux depencies manually"
	exit 1
fi
