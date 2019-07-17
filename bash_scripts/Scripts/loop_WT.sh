#ls -d WT_2018* > list_folder.txt
#for filename in Data/*.txt; do
for f in WT_*; do
    if [[ -d $f ]]; then
     #printf  ${f}
        #printf $f >> Date_WT.csv 
        printf '%s\n' "$f" >> Date_WT.csv
	#cat ${f}/WT_00_* >> 00.csv
        #cat ${f}/WT_24_* >> 24.csv
        #cat ${f}/WT_48_* >> 48.csv
        #cat ${f}/WT_72_* >> 72.csv
    fi
done
