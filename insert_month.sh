#!/bin/bash
# 5.1 ~ 5.31
for i in $(seq 20200501 20200601);
do
  echo $i "번째 파일 실행중"
  python server_month.py $i
done
