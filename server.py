'''
작성일 : 2020. 12. 28.
작성자 : 정성모
코드개요 : main과 thread로 2가지 작업을 동시에 하며, main에서 궤적데이터에서
           링크별 평균속도를 추출하여 데이터베이스에 삽입, thread에서는
           socket 통신을 위한 작업

           python 3.8.5 ver
           mysql 8.0.22-0ubuntu0.20.04.3 ver
           pymysql 0.10.1 ver
           sharkbite 0.7.4 ver
'''
from read_segment import read_seg
from mapping import trajectory2segment
from convertToSpeed import convert2speed

from concurrent.futures import ProcessPoolExecutor
from multiprocessing import Process, Pool
from queue import Queue

import threading
import socket
import time
import json
import sys
import datetime
import pymysql
import pysharkbite
import os
import gc

def main():
    global cell
    global link_id
    global F_NODE
    global que
    que = Queue()

    cell, link_id, F_NODE = read_seg()
    print("start server...")
    s_th = threading.Thread(target=server)
    s_th.daemon = True
    s_th.start()
    
    q = time.time()
    count = 0
    print("main")
    pool = Pool(20, maxtasksperchild=250)
    while True:
        value = que.get() # queue를 이용하여 queue에 데이터가 없을 경우 sleep한 상태로 대기
        count += 1
        pool.apply_async(mapping, (value,)) # 비동기

        if count % 1000 == 0:
            print(count, que.qsize())

def mapping(value):
    '''
    함수개요 : wedrive로부터 data(UUID,time_begin)를 받아 궤적 포인트들 추출, 매핑, 속도데이터 변환, accumulo 및 mysql 삽입 처리를 위한 함수
    매개변수 : value = socket통신으로 받은 data(UUID, time_begin)
    '''
    try:
        start = time.time()
        #print("value:",sys.getsizeof(value))
        data = [i.split(",")[2:4]for i in value.split("\n")]
        traj_point = data[1:-1]
        print("preprocessing success")
        mapping_data = trajectory2segment(traj_point, cell, link_id, F_NODE)
        print(mapping_data)
        '''
        data = traj_info_db(value)
        #print("data:",sys.getsizeof(data))
        traj_point = extract_point(data)
        #print(os.getpid(),"traj_point")
        #print("traj_point:",sys.getsizeof(traj_point))
        mapping_data = trajectory2segment(traj_point, cell, link_id, F_NODE)
        #print(os.getpid(),"mapping_data")
        #print("mapping_data:",sys.getsizeof(mapping_data))
        speed_data, min5_speed = convert2speed(data, mapping_data)
        #print("speed_data:",sys.getsizeof(speed_data))
        #print(os.getpid(),"speed_data")
        update_db(speed_data)

        write_to_accumulo(min5_speed)
        #print(os.getpid(),"end",time.time()-start)        
        #print(os.getpid())
        '''
        return 0
        
    except Exception as e:
        print("프로세스 전사")
        print(os.getpid(),"error code :",e)

def extract_point(data):
    '''
    함수개요 : 궤적 데이터에서 포인트 정보(lat,lng)들을 추출
    매개변수 : data = 궤적 데이터
    '''
    try:
        traj = []
        if not data[0]["jsondata"]:
            sys.exit()
        data = json.loads(data[0]["jsondata"])
        for i in data[0]["items"]:
            gps = []
            lat = i["lat"]
            lng = i["lng"]
            gps = [lat,lng]
            traj.append(gps)
        return traj
    except Exception as e:
        print("포인트 추출 사망")
        print(e)

def traj_info_db(value):
    '''
    함수개요 : UUID, time_begin을 통해 궤적 정보를 가져오는 함수
    매개변수 : value = data(UUId, time_begin)
    '''
    try:
        traj_db = pymysql.connect(
            user='root', 
            passwd='1', 
            host='192.168.1.16', 
            db='testdb', 
            port=3306,
            charset='utf8')
        uid_bt = json.loads(value)
        cursor = traj_db.cursor(pymysql.cursors.DictCursor)
        sql = "SELECT * FROM TB_TRACKING2_DATA_" + uid_bt["time_begin"][:10].replace("-","") + " WHERE uuid = %s and time_begin = %s"
        cursor.execute(sql, (uid_bt["uuid"], uid_bt["time_begin"]))
        result = cursor.fetchall()

        return result
    except Exception as e:
        print("traj_db 비정상 연결 끊김")
        print(e)
    finally:
        traj_db.close()


def update_db(speed):
    '''
    함수개요 : 링크별 평균속도 데이터를 mysql의 업데이트
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
#    try:
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
            mutation.put("","","",row[1],str(row[2])+", 0, 0.0, 0.0")
            writer.addMutation(mutation)
        writer.close()
    except Exception as e:
        print("disconnect")
        print(e)
    return 0

def accept(client_socket, addr):
    '''
    함수개요 : socket 서버가 client와 연결되면 데이터를 받아 queue에 저장하는 함수
    매개변수 : 
        clinent_socket = socket 서버와 연결된 클라이언트
        addr = socket 서버와 연결된 클라이언트 주소
    '''
    try:
        result = ""
        # 무한루프를 돌면서
        while True:
            
            # 클라이언트가 보낸 메시지를 수신하기 위해 대기합니다. 
            data = client_socket.recv(1024)
            
            # 빈 문자열을 수신하면 루프를 중지합니다. 
            if not data:
                # 수신받은 문자열을 출력합니다.
    #            print('Received from', addr, result)
                que.put(result)
    #            print("que size:",que.qsize())

                break
        
            # 받은 문자열을 다시 클라이언트로 전송해줍니다.(에코) 
            client_socket.sendall(data)

            result += data.decode()

#        # 소켓을 닫습니다.
#        client_socket.close()
    except Exception as e:
        print("socket error")
        print(e)
    finally:
        client_socket.close()

def server():
    '''
    함수개요 : socket 서버로 socket 설정 및 HOST, PORT 설정하는 함수
    '''
    
    # 접속할 서버 주소입니다. 여기에서는 루프백(loopback) 인터페이스 주소 즉 localhost를 사용합니다. 
    HOST = '192.168.1.16'
    
    # 클라이언트 접속을 대기하는 포트 번호입니다.   
    PORT = 9990
    
    # 소켓 객체를 생성합니다. 
    # 주소 체계(address family)로 IPv4, 소켓 타입으로 TCP 사용합니다.  
    server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    
    # 포트 사용중이라 연결할 수 없다는 
    # WinError 10048 에러 해결를 위해 필요합니다. 
    server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    
    # bind 함수는 소켓을 특정 네트워크 인터페이스와 포트 번호에 연결하는데 사용됩니다.
    # HOST는 hostname, ip address, 빈 문자열 ""이 될 수 있습니다.
    # 빈 문자열이면 모든 네트워크 인터페이스로부터의 접속을 허용합니다. 
    # PORT는 1-65535 사이의 숫자를 사용할 수 있습니다.  
    server_socket.bind((HOST, PORT))
    # 서버가 클라이언트의 접속을 허용하도록 합니다. 
    server_socket.listen()
    
    while True:
        client_socket, addr = server_socket.accept()
        # 접속한 클라이언트의 주소입니다.
        #print('Connected by', addr)
        accept(client_socket, addr)
        #th = threading.Thread(target=accept, args=(client_socket, addr))
        #th.daemon = True
        #th.start()

main()
