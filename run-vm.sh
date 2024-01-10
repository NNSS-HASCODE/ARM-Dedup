#!/bin/bash

sudo rm ./SSDInfo_*.log

IMGDIR="./images"
KERNEL="src/linux/arch/x86/boot/bzImage"
FEMU="src/femu/build-femu/x86_64-softmmu/qemu-system-x86_64"
Share_Dir_Path="./share"

sudo sh -c 'echo 1 > /proc/sys/vm/drop_caches'
echo 2 | sudo tee /sys/kernel/mm/ksm/run >/dev/null 2>&1

echo "===> Booting the Virtual Machine..."
sleep 3

    #-kernel "${IODA_KERNEL}" \
    #-append "root=/dev/sda1 console=ttyS0,115200n8 console=tty0 biosdevname=0 net.ifnames=0 nokaslr log_buf_len=128M loglevel=4" \

MB=1
let GB=1024*${MB}
device_count=5
let array_size_MB=64*${GB}
let device_size_MB_per=${array_size_MB}/${device_count}

device_command=""
for((i=1;i<=${device_count};i++));
do device_command+="-device femu,devsz_mb=${device_size_MB_per},femu_mode=1 "; done

sudo ${IODA_FEMU} \
    -name "iodaVM" \
    -cpu host \
    -smp 24 \
    -m 40G \
    -enable-kvm \
    -boot menu=on \
    -fsdev local,security_model=passthrough,id=fsdev0,path=${Share_Dir_Path} \
    -device virtio-9p-pci,id=fs0,fsdev=fsdev0,mount_tag=hostshare \
    -drive file=${IODA_IMGDIR}/u20s.qcow2,if=virtio,cache=none,aio=native,format=qcow2 \
    ${device_command} \
    -netdev user,id=user0,hostfwd=tcp::10101-:22 \
    -device virtio-net-pci,netdev=user0 \
    -nographic | tee ./ioda-femu.log 2>&1 \

echo "femu finish!"
