#!/bin/bash

export GOPATH="/home/fukun/Desktop/Courses/DistributionSystem/MIT6.284/MIT-6.824/src/"
export PATH="$PATH:/usr/lib/go-1.9/bin"

rm res -rf
mkdir res
for ((i = 0; i < 1000; i++))
do
echo $i
(go test -race) > ./res/$i
grep -nr "FAIL.*" res
done