#!/bin/bash

mkdir -p data

cd data

wget https://www.dropbox.com/s/3uo4gznau7fn6kg/Archive.zip
wget https://www.dropbox.com/s/yuw9m5dbg03sad8/plate_type.csv

unzip Archive.zip

rm -r Archive.zip && rm -r __MACOSX