from read_segment import read_seg
from mapping import trajectory2segment
from concurrent.futures import ProcessPoolExecutor
import socket
from queue import Queue
import threading
import time
import pymysql
import json
import sys
import datetime


def main():
    global traj_db
    global cell
    global link_id
    global F_NODE
    global que
    que = Queue()
    traj_db = pymysql.connect(
        user='root', 
        passwd='1', 
        host='192.168.1.16', 
        db='testdb', 
        port=3306,
        charset='utf8')
    cell, link_id, F_NODE = read_seg()
    print("start server")
    s_th = threading.Thread(target=server)
    s_th.daemon = True
    s_th.start()

    while True:
        if que.qsize() != 0:
            q = time.time()
            value = que.get()
            print(que.qsize())
            with ProcessPoolExecutor(max_workers=20) as pool:
                pool.submit(mapping, value)
            print("q",time.time()-q)
            

def mapping(value):
    start = time.time()
    data = traj_info_db(value)
    traj = extract_point(data)
    mapping_data = trajectory2segment(traj, cell, link_id, F_NODE)
    #print(mapping_data)
    print("end",time.time()-start)

def extract_point(data):
    traj = []
    data = json.loads(data[0]["jsondata"])
    for i in data[0]["items"]:
        gps = []
        lat = i["lat"]
        lng = i["lng"]
        gps = [lat,lng]
        traj.append(gps)
    return traj

def traj_info_db(value):
    uid_bt = json.loads(value)
    uid = uid_bt["uuid"]
    bt = uid_bt["begin_time"]
    day = bt.split()[0].replace('-','')
    cursor = traj_db.cursor(pymysql.cursors.DictCursor)
    #cursor = traj_db.cursor()
    sql = "SELECT * FROM TB_TRACKING2_DATA_"+day+" where uuid = "+'\''+uid+'\''+" and time_begin = "+'\''+bt+'\''
    #sql = "SELECT * FROM TB_TRACKING2_DATA_20200925 limit 1"
    cursor.execute(sql)
    result = cursor.fetchall()
    return result

def accept(client_socket, addr):
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

    '''
    # 클라이언트가 보낸 메시지를 수신하기 위해 대기합니다. 
    data = client_socket.recv(1024)
    
    # 수신받은 문자열을 출력합니다.
    print('Received from', addr, data.decode())
    
    # 받은 문자열을 다시 클라이언트로 전송해줍니다.(에코) 
    client_socket.sendall(data)
    '''

    # 소켓을 닫습니다.
    client_socket.close()
    #server_socket.close()

def server():
    
    # 접속할 서버 주소입니다. 여기에서는 루프백(loopback) 인터페이스 주소 즉 localhost를 사용합니다. 
    HOST = '127.0.0.1'
    
    # 클라이언트 접속을 대기하는 포트 번호입니다.   
    PORT = 9999
    
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
    
    # while True:
    #     with ProcessPoolExecutor(max_workers=20) as pool:
    #         pool.submit(accept, server_socket)
    
    while True:
        client_socket, addr = server_socket.accept()
        # 접속한 클라이언트의 주소입니다.
#        print('Connected by', addr)

        th = threading.Thread(target=accept, args=(client_socket, addr))
        th.daemon = True
        th.start()

main()
