#! /bin/bash

# $1: log file dir
# cd $1

files=`ls *INFO*log`
# process each log file
for file_name in $files
do
# file_name="NextgenBenchmarkTest.INFO.20221221-071202.log"

# replace * with \*
sed -i 's/*/\*/g' $file_name

# total benchmark case count
cnt=`grep -n "BENCH SQL" $file_name |wc -l`
benchmark_file=`echo $file_name| cut -d "." -f 1,3`
result_csv=$benchmark_file".csv"

# csv header
echo "duckdb_runtime(ms),bdtk_prepare_time(ms),bdtk_run_time(ms),bdtk_total_time(ms)">>$result_csv

# process each benchmark case
for ((idx=1; idx<=cnt; idx++))
do
# get current case log range
cur_line_number=`grep -n "BENCH SQL" $file_name | head -$idx | tail -1 | cut -d ":" -f 1`
next_idx=`expr $idx + 1`
next_line_number=`grep -n "BENCH SQL" $file_name | head -$next_idx | tail -1 | cut -d ":" -f 1`

# for last case
if [ $cur_line_number -eq $next_line_number ]
then
next_line_number=`cat $file_name| wc -l `
fi

#cur_block=`sed -n "${cur_line_number},${next_line_number}p" $file_name`
# echo $cur_block
#bench_sql=`echo $cur_block|grep "BENCH SQL:" | cut -d ":" -f 5`
# echo $bench_sql

# extract SQL
bench_sql=`sed -n "${cur_line_number},${next_line_number}p" $file_name|grep "BENCH SQL:" | head -1 | cut -d ":" -f 5`

# extract time
duck_time=`sed -n "${cur_line_number},${next_line_number}p" $file_name|grep "Timer end * DuckDb" | rev | cut -d " " -f 2 | rev`  
bdtk_prepare_time=`sed -n "${cur_line_number},${next_line_number}p" $file_name|grep "Timer end * create" | rev | cut -d " " -f 2 | rev`
bdtk_run_time=`sed -n "${cur_line_number},${next_line_number}p" $file_name|grep "Timer end * run" | rev | cut -d " " -f 2 | rev`  
bdtk_total_time=`sed -n "${cur_line_number},${next_line_number}p" $file_name|grep "Timer end * Nextgen" | rev | cut -d " " -f 2 | rev`  

# to csv
echo "${bench_sql}" >> ${result_csv}
echo "${duck_time},${bdtk_prepare_time},${bdtk_run_time},${bdtk_total_time}" >> ${result_csv}
done

done