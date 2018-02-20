#!/bin/sh
echo "Running"

`mvn clean package`

`hadoop dfs -rm -r ${4}/*`

`hadoop $1 $2 $3 $4`
