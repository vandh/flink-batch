#!/bin/bash
cd /data/day
dir=/data/day/D_*
dwfiles=$(ls ${dir})
for sfile in ${dwfiles}
do
    sfile=${sfile##*/}
    newDir=${sfile:2:8}
    if [[ ${newDir} -ge "20200101" && ${newDir} -le "20310715" && ! -d ${newDir} ]]; then
          #echo "make dir - ${newDir}"
          mkdir ${newDir}
		  mv D_${newDir}_* ${newDir}
    fi
done
echo "no.1 task GL 1,2"
flink run -c com.jw.plat.Main2 /data/flinktool/data-flink-1.0-SNAPSHOT.jar  /data/day/20210730 1-2 GBK - GL 2 FILE /data/day/result_add/$(date -d last-day +%Y%m%d)
echo "no.2 task GL 5,6"
flink run -c com.jw.plat.Main2 /data/flinktool/data-flink-1.0-SNAPSHOT.jar  /data/day/20210730 5-6 GBK - GL 2 FILE /data/day/result_add/$(date -d last-day +%Y%m%d)
echo "no.3 task GL 3,4"
flink run -c com.jw.plat.Main2 /data/flinktool/data-flink-1.0-SNAPSHOT.jar  /data/day/20210730 3-4 GBK - GL 2 FILE /data/day/result_add/$(date -d last-day +%Y%m%d)
echo "no.4 task GL 7,8"
flink run -c com.jw.plat.Main2 /data/flinktool/data-flink-1.0-SNAPSHOT.jar  /data/day/20210730 7-8 GBK - GL 2 FILE /data/day/result_add/$(date -d last-day +%Y%m%d)
echo "no.5 task APPRE"
flink run -c com.jw.plat.Main2 /data/flinktool/data-flink-1.0-SNAPSHOT.jar  /data/day/20210730 - GBK - APPRE 2 FILE /data/day/result_add/$(date -d last-day +%Y%m%d)
echo "no.6 task APSTD"
flink run -c com.jw.plat.Main2 /data/flinktool/data-flink-1.0-SNAPSHOT.jar  /data/day/20210730 - GBK - APSTD 2 FILE /data/day/result_add/$(date -d last-day +%Y%m%d)
echo "no.7 data trans"
flink run -c com.jw.plat.Main2 /data/flinktool/data-flink-1.0-SNAPSHOT.jar   DB xx.238.25.109 10072 rtp_dw_db root evBKIA27vUap /data/day/result_add/$(date -d last-day +%Y%m%d) ADD_AP_STD_VOUCHER_INFO_1 /data/day/conf/  TRANS  $(date -d last-day +%Y-%m-%d)


