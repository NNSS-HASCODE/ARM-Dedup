#!/bin/bash
#
# This script builds FEMU and Linux for ARM-Dedup
# Usage: ./build.sh
#
# Note: Please cd into ARM-Dedup/ first, and then run "./build.sh"
#

ARM_Dedup_TOPDIR=$(pwd)

red=`tput setaf 1`
green=`tput setaf 2`
blue=`tput setaf 4`
reset=`tput sgr0`

BUILD_LOG="arm-dedup-build.log"

sudo swapoff -a

echo -e "\n====> Start building ARM-Dedup ... should take ${green}<30min${reset} to finish with 32 cores on the server\n"

# First, install dependencies
echo ""
echo "====> ${green}[1/3]${reset} Installing dependencies ..."
echo ""
sudo ./util/pkgdep.sh >${BUILD_LOG} 2>&1


# Second, build FEMU
echo ""
echo "====> ${green}[2/3]${reset} Building FEMU ..."
echo ""
cd ${ARM_Dedup_TOPDIR}/src/femu
mkdir -p build-femu
cd build-femu
cp ../femu-scripts/femu-compile.sh .
make clean >/dev/null 2>&1
./femu-compile.sh >>${BUILD_LOG} 2>&1

# Third, build Linux
echo ""
echo "====> ${green}[3/3]${reset} Building Linux ..."
echo ""
cd ${ARM_Dedup_TOPDIR}/src/linux
make clean >/dev/null 2>&1
yes "" | make oldconfig >>${BUILD_LOG} 2>&1
sed -i "s/# CONFIG_DM_DEDUP is not set/CONFIG_DM_DEDUP=m/g" .config
make -j32 >>${BUILD_LOG} 2>&1
cd ../../


FEMU_BIN="src/femu/build-femu/x86_64-softmmu/qemu-system-x86_64"
LINUX_BIN="src/linux/arch/x86/boot/bzImage"

if [[ -e ${FEMU_BIN} && -e ${LINUX_BIN} ]]; then
    echo ""
    echo "===> ${green}Congrats${reset}, IODA is successfully built!"
    echo ""
    echo "Please check the compiled binaries:"
    echo "  - FEMU at [${blue}${FEMU_BIN}${reset}]"
    echo "  - Linux at [${blue}${LINUX_BIN}${reset}]"
    echo ""

else

    echo ""
    echo "===> ${red}ERROR:${reset} Failed to build, please check [${BUILD_LOG}]."
    echo ""

fi
