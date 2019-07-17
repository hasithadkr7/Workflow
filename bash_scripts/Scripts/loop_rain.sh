#!/bin/bash
for i in {1..73}
do
   #echo "Welcome $i times"
#done
   echo $(python3 plot_rain_XXXX.py ${i})
done
