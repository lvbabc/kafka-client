#!/bin/bash

files=`ls -l | awk '$7 == 28 && $8 > "09:30" && $8 < "23:00" {print $9}' | grep "192\.168\.4\.162" `
for i in $files
do
        echo $i
        scp $i zxsoft@192.168.4.162:/home/zxsoft/2016-04-15
done


# ls -l | awk '$7 == 15 && $8 > "09:30" && $8 < "13:00" {print $9}' | xargs md5sum > md5_${HOSTNAME}.txt
