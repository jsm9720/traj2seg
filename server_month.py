'''
작성일 : 2020. 12. 28.
작성자 : 정성모
코드개요 : hdfs에서 전처리한 파일을 읽어 멀티프로세싱 환경에서 매핑한 후 accmulo or mysql의 매핑하여 삽입( batch )
'''
from read_segment import read_seg
from mapping import trajectory2segment

from concurrent.futures import ProcessPoolExecutor
from multiprocessing import Process, Pool
from hdfs import InsecureClient
from queue import Queue
from datetime import datetime

import time
import json
import sys
import pysharkbite
import pymysql
import os
import gc
import csv

def main():
    global cell
    global link_id
    global F_NODE

    cell, link_id, F_NODE = read_seg()
    print("start server...")

    count = 0
    # i = 1
    # sw = True

    client_hdfs = InsecureClient("http://192.168.1.16:50070")
    csv.field_size_limit(1000000000)

    print("main")
    i = sys.argv[1]
    load_data = "/wedrive_data/proc_01_parsed_line/data_%s.csv" % i
    with client_hdfs.read(load_data,encoding="utf-8") as fr:
        csv_data = csv.reader(fr)
#        i = int(i)
        q = time.time()
#        next_q = time.time()
        while True:
            # if sw == False:
            #     fr = f_open(i)
            #     sw = True
            pool = Pool(20)
            while True:
                if count == 10000:
                    pool.close()
                    pool.join()
                    count = 0
                    break
                try:
                    next_csv_data = next(csv_data)[2:]
                    r_data = ','.join(next_csv_data)
                except StopIteration:
                    print("file read end")
                    pool.close()
                    pool.join()
                    fr.close()
    
    #                if i == 1:
    #                    print("precess time", time.time()-q)
    #                else:
    #                    print("precess time", time.time()-next_q)
                    
    #                i += 1
    #                fr = f_open(i)
    #                next_q = time.time()
    #                break
                    print("process time",time.time()-q)
                    sys.exit(0)
                    # sw = False
                    # i += 1
                    # break
                else:
                    count += 1
                    pool.apply_async(mapping, (r_data,))
                    if count % 5000 == 0:
                        print(count)
    
    
def mapping(value):
    '''
    함수개요 : sql.gz 파일에서 읽은 data를 포이트추출, 매핑, 스피드데이터 추출, accumulo insert하는 함수
    매개변수 : value = 궤적 info
    ''' 
    try:
#        start = time.time()
        value1 = json.loads(value)
        traj_point = extract_point(value1)

        mapping_data = trajectory2segment(traj_point, cell, link_id, F_NODE)

        speed_data = convert2speed(value1, mapping_data)
        write_to_accumulo(speed_data)
        #update_db(speed_data)
        #insert_db(speed_data)


        return 0
    except Exception as e:
        print("프로세스 전사")
        print(os.getpid(),"error code :",e)

def extract_point(data):
    '''
    함수개요 : 궤적 데이터에서 포인트 정보(lat,lng) 추출함수
    매개변수 : data = json형태로 파싱된 궤적 info
    '''
    try:
        traj = []
        for i in data:
            gps = []
            lat = i["lat"]
            lng = i["lng"]
            gps = [lat,lng]
            traj.append(gps)
        return traj
    except Exception as e:
        print("포인트 추출 사망")
        print(e)

def convert2speed(traj_data, mapping_data):
    '''
    함수개요 : 매핑된 데이터와 궤적 정보 데이터를 통해 링크별 speed data 추출
    매개변수 :
        traj_data = json형태로 파싱된 궤적 info
        mapping_data = mapping 된 데이터 ( link_id : 1, 24, linkid : 25 ~ 40, ... )
    '''
    speed = []
    min5_speed = []
    point_data = traj_data
    for md in mapping_data:
        traj_index = mapping_data[md]

        # wedrive 현재속도 없데이트를 위한 링크의 마지막점 시간 추출
        if "time" in point_data[traj_index[1]].keys():
            link_last_time_value = point_data[traj_index[1]]['time']/1000
            link_last_time = time.strftime('%Y-%m-%d %H:%M:%S',time.localtime(link_last_time_value))
        else:
            link_last_time = point_data[traj_index[1]]['date']

        # Accumulo 저장을 위한 링크 첫번쨰 포인트 기준으로 5분단위대 시간
        if "time" in point_data[traj_index[0]].keys():
            second = point_data[traj_index[0]]['time']/1000
            date_time = time.localtime(second)
            m_remainder_value = 300-(date_time.tm_min%5)*60
            s_remainder_value = date_time.tm_sec
            min5_date_time = time.strftime(' %Y-%m-%d %H:%M:%S',time.localtime(second+(m_remainder_value - s_remainder_value)))

        else:
            data_datatime = datetime.strptime(point_data[traj_index[0]]['date'], '%Y-%m-%d %H:%M:%S')
            second = data_datatime.timestamp()
            date_time = time.localtime(second)
            m_remainder_value = 300-(date_time.tm_min%5)*60
            s_remainder_value = date_time.tm_sec
            min5_date_time = time.strftime(' %Y-%m-%d %H:%M:%S',time.localtime(second+(m_remainder_value - s_remainder_value)))        

        temp = 0 

        #링크에 매핑되는 점이 하나 일때 mapping = {seg : (0,0)} 처리 필요
        for i in range(traj_index[0],traj_index[1]+1):
            temp += point_data[i]['speed']*3.6 # m/s * 3.6 하면 km/h
        avg_speed = temp / (traj_index[1]-traj_index[0]+1)
        #speed.append([avg_speed, link_last_time, md])
        min5_speed.append([md+str(min5_date_time),int(second),avg_speed])

    return min5_speed


def update_db(speed):
    '''
    함수개요 : 링크별 속도 데이터를 mysql의 업데이트
    매개변수 : speed = 링크별 평균 속도(평균속도, point_time, link_id)
    '''
    try:
        speed_db = pymysql.connect(
            user='root', 
            passwd='1', 
            host='192.168.1.16', 
            db='updatedb', 
            port=3306,
            charset='utf8')
        with speed_db.cursor() as cursor:
        #sql = "insert into LINK_SPEED_DATA(SPEED, LINK_ID) values (%s,%s)"
            sql = "update test set SPEED = %s, UPDATE_TIME = %s where LINK_ID = %s"
            cursor.executemany(sql, speed)
        speed_db.commit()
    except Exception as e:
        print("update db 비정상 연결 끊김")
        print(e)
        if e.args[0] == 1213:
            while True:
                try:
                    with speed_db.cursor() as cursor:
                        sql = "update test set SPEED = %s, UPDATE_TIME = %s where LINK_ID = %s"
                        cursor.executemany(sql, speed)
                    speed_db.commit()
                    print("commit")
                    break
                except Exception as e:
                    if e.args[0] != 1213:
                        print(e)
                        break
    finally:
        speed_db.close()

def insert_db(speed):
    '''
    함수개요 : 링크별 속도 데이터를 mysql의 insert
    매개변수 : speed = 링크별 평균속도(평균속도, point_time, link_id)
    '''
    try:
        speed_db = pymysql.connect(
            user='root', 
            passwd='1', 
            host='192.168.1.16', 
            db='updatedb', 
            port=3306,
            charset='utf8')
        with speed_db.cursor() as cursor:
        #sql = "insert into LINK_SPEED_DATA(SPEED, LINK_ID) values (%s,%s)"
            sql = "update 5_MIN set avg_speed = %s where time = %s and link_id = %s"
            cursor.executemany(sql, speed)
        speed_db.commit()
    except Exception as e:
        print("update db 비정상 연결 끊김")
        print(e)
        if e.args[0] == 1213:
            while True:
                try:
                    with speed_db.cursor() as cursor:
                        sql = "update 5_MIN set avg_speed = %s where link_id = %s and time = %s"
                        cursor.executemany(sql, speed)
                    speed_db.commit()
                    print("commit")
                    break
                except Exception as e:
                    if e.args[0] != 1213:
                        print(e)
                        break
    finally:
        speed_db.close()

def write_to_accumulo(rows):
    '''
    함수개요 : 링크별 평균속도 데이터를 accmulo의 append
    매개변수 : rows = 링크별 평균속도 (link_id+5분단위로 전처리한 time, timestemp, 평균속도)
    '''
    try:
        zoo_instance = "dblab"
        zookeepers = "dblab-node-01:2182,dblab-server-01:2182,dblab-server-02:2182,dblab-server-03:2182"
        username = "root"
        password = "1" 
        
        configuration = pysharkbite.Configuration()
        
        zk = pysharkbite.ZookeeperInstance(zoo_instance, zookeepers, 1000, configuration)
        
        user = pysharkbite.AuthInfo(username, password, zk.getInstanceId())

        connector = pysharkbite.AccumuloConnector(user, zk) 
        table_operations = connector.tableOps("Link_5_Min_Data")
        auths = pysharkbite.Authorizations()
        
        writer = table_operations.createWriter(auths, 10)
        for row in rows:
            mutation = pysharkbite.Mutation(row[0])
            mutation.put("","","", row[1],str(row[2])+", 0, 0.0, 0.0")
            writer.addMutation(mutation)
        writer.close()
    except Exception as e:
        print("disconnect")
        print(e)
    return 0

main()
