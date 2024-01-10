# ARM-Dedup

This repository is the code for the paper "Eliminating Storage Management Overhead of Deduplication over SSD Arrays Through a Hardware/Software Co-Design".

## Overview

ARM-Dedup include the following components:

- `femu`: Implemented a prototype of ARM-SSD on the basis of the original version
- `linux`: Enahnced Linux kernel (based on Linux v4.19) by modifying the RAID array controllers and a block layer deduplication engine.

## Usage

### 1. Environmental requirement

We recommend using a server with more than 128GB DRAM, and equip with `Ubuntu 20.04, GCC: 7.5.0`. Otherwise, you need to follow below requirements:

1. Ubuntu system version should be 18.04 or 20.04, later version maybe can't compile 4.x kernel
2. DRAM should at least more than the simulated ssd's size
3. GCC Version is 7.x needed to compile linux 4.x kernel, later version(8.x/9.x) can't compile 4.x kernel

> One way to setup the default version of gcc is listed below:
> 
> sudo update-alternatives --install /usr/bin/gcc gcc /usr/bin/gcc-7 100
>
> sudo update-alternatives --install /usr/bin/gcc gcc /usr/bin/gcc-9 80

### 2. Compile 

run `build.sh` in the root dir of the project.

```bash
sudo ./build.sh
```

> if the compile scripts show a `fail` result, see `arm-dedup-build.log` to get more debug info. 

### 3. Run VM

Before run, you need to prepare a `qcow2` format image by yourself. The image is too large to upload to this reposity. The image's requriments is same with the host server above. We recommend using the official image provided by [femu](https://github.com/vtess/FEMU).

If you have one prepared now, you can start it by the script `run-vm.sh`

```bash
./run-vm.sh
```

> make sure you have changed the image's file path and share dir path to yours in the script.

After the vm start, you can ssh to get access as below in another terminal.

```bash
ssh femu@localhost -p 10101
```

### 4. Install Kernel

If all the previous steps success, you can share the host's dir in vm.

First, create a mount point in vm.

```bash
mkdir -p ~/share
```

Then, you need to mount share dir on it.

```bash
sudo mount -t 9p -o trans=virtio hostshare ~/share/ -oversion=9p2000.L
```

Last, you should be able to access the linux dir in the host. So, you can install the compiled kernel into vm.

```bash
cd ~/share/linux/

sudo make INSTALL_MOD_STRIP=1 modules_install
sudo make install

sudo update-grub2
```

After that, you need to `reboot` the vm to switch to the new installed version kernel.

### 5. Start Dmdedup

Below is an example of how to run Dmdeudup on a raid array.

```bash
#!/bin/bash

device_count_in_raid=$1
raid_device_name=$2
meta_dev_size_per=$3
dedup_name=$4
dedup_system=$5
transaction_size=$6
GC_threshould=$7
R_MapTable_ratio=$8
cache_size=$9
GC_block_limit=${10}
raid_level=${11}

echo -e "$# $* \n Begin create dmdedup (device_count_in_raid=${device_count_in_raid}, raid_device_name=${raid_device_name}, \
    meta_dev_size_per=${meta_dev_size_per}, dedup_name=${dedup_name}, dedup_system=${dedup_system}, \
    transaction_size=${transaction_size}, GC_threshould=${GC_threshould}, R_MapTable_ratio=${R_MapTable_ratio}, cache_size=${cache_size}, \
    GC_block_limit=${GC_block_limit}, raid_level=${raid_level})"

Sector=512
let KB=2*${Sector}
let MB=1024*${KB}
let GB=1024*${MB}

if [ ${raid_level} -eq 0 ]; then
let meta_dev_size=${meta_dev_size_per}*${device_count_in_raid}/${MB}
sudo fdisk ${raid_device_name} 1>/dev/null 2>&1 <<EOF
n
p
1

+${meta_dev_size}M
n
p
2


w
EOF
echo "Split RAID0 into two parts..."

else
raid_size=`sudo blockdev --getsz ${raid_device_name}`
let meta_dev_size=${meta_dev_size_per}*${device_count_in_raid}/${MB}
let tt_device_count=${device_count_in_raid}+1
let data_dev_size=${raid_size}/${tt_device_count}*${device_count_in_raid}-${meta_dev_size}
sudo fdisk ${raid_device_name} 1>/dev/null 2>&1 <<EOF
n
p
1

+${meta_dev_size}M
n
p
2

+${data_dev_size}M
n
p
3


w
EOF
echo "Split RAID5 into two parts..."
fi

META_DEV=${raid_device_name}p1
DATA_DEV=${raid_device_name}p2
DATA_DEV_SIZE=`sudo blockdev --getsz ${DATA_DEV}`
TARGET_SIZE=${DATA_DEV_SIZE}
sudo dd if=/dev/zero of=${META_DEV} bs=4096 count=1 1>/dev/null 2>&1
sudo depmod -a
sudo modprobe dm-bufio
echo "${cache_size}" | sudo tee /sys/module/dm_bufio/parameters/max_cache_size_bytes >/dev/null
sudo modprobe dm-dedup
echo "0 ${TARGET_SIZE} dedup ${META_DEV} ${DATA_DEV} 4096 md5 \
    ${dedup_system} ${transaction_size} 0 ${GC_threshould} ${device_count_in_raid} ${R_MapTable_ratio} \
    ${GC_block_limit} ${raid_level}"
echo "0 ${TARGET_SIZE} dedup ${META_DEV} ${DATA_DEV} 4096 md5 \
    ${dedup_system} ${transaction_size} 0 ${GC_threshould} ${device_count_in_raid} ${R_MapTable_ratio} \
    ${GC_block_limit} ${raid_level}" | sudo dmsetup create ${dedup_name}

ret=`lsblk | grep ${dedup_name} -c`
if [ ${ret} -gt 0 ]; then
    echo "Create deduplication system success..."
else
    echo "Create deduplication system failed..."
fi
```

### 6. Test

TBD

## Reference

We appreciate the help of the following open-source repositories for our project:

- https://github.com/vtess/FEMU
- https://github.com/vtess/IODA-SOSP21-AE
- https://github.com/dmdedup/dmdedup4.19
- https://github.com/torvalds/linux.git