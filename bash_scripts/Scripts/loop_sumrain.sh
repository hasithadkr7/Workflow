#!/bin/bash
for i in {1..20}
do
   #echo "Welcome $i times"
#done
   echo $(python3 plot_sumrain_XXXX.py ${i})
done

