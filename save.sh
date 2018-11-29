#!/bin/bash

function usage {
    echo Usage: ./$0 "[-d] [-x <path>] [-c <path>] <%Y-%m-%d>" 
}

function get_xml {
    if [ -n "$2" -a -n "$3" ]; then 
       ./getxml $2 $3 -d $1   
    else
       ./getxml -d $1   
    fi
}

function save_pitches {
    awards_dir="pitch_awards_$YEAR"
    stats_dir="pitcher_stats_$YEAR"
    python pipeline.py -d $1 $2 $3 $4 $5 $6 -a $awards_dir -s $stats_dir
}


args=`getopt dc:x: $*`
errcode=$?
set -- $args

if [ $? != 0 ]; then usage; exit 1; fi;  

for i
do
  case "$i"
  in
      -d)
          DB_FLAG="-p"
          shift;; 
      -x)
          XML_FLAG_AND_DIR="$1 $2"
          shift; shift;;
      -c)
          CSV_FLAG_AND_DIR="$1 $2"
          shift; shift;;
      -- ) 
          shift;
          break;;
  esac
done

DATE=$1
YEAR=`python scripts/getyear.py $DATE`
echo year $YEAR
if [ -z $DATE ]; then 
    usage
else
    get_xml $DATE $XML_FLAG_AND_DIR &&
    save_pitches $DATE $XML_FLAG_AND_DIR $CSV_FLAG_AND_DIR $DB_FLAG
fi
