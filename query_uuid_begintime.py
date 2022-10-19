'''
작성일 : 2020. 12. 28.
작성자 : 정성모 
코드개요 : 위드라이브 mysql 접속하여 하루치 데이터(uuid, time_begin) 가져오기
'''
import json
import pymysql
import time

result = []
try:
    traj_db = pymysql.connect(
        user='',
        passwd='',
        host='',
        db='wedrive',
        port=3306,
        charset='utf8')
    cursor = traj_db.cursor(pymysql.cursors.DictCursor)
    #20201124
    sql = "SELECT uuid, time_begin FROM TB_TRACKING2_DATA_20200925 limit 100000"
    cursor.execute(sql)
    result = cursor.fetchall()
    traj_db.close()
except Exception as e:
    print("비정상 연결 끊김")
    print(e)

li = []
for data in result:
    tb = data["time_begin"].strftime('%Y-%m-%d %H:%M:%S')
    data["time_begin"] = tb
    jsondata = json.dumps(data)
    li.append(jsondata)

with open("uuid_tb.txt","w") as f:
    f.write('\n'.join(li))
    f.close()

with open("uuid_tb.txt","r") as fr:
    
    data = fr.read()
    fr.close()
    data = data.split('\n')
