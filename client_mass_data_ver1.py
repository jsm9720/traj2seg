'''
작성일 : 2020. 12. 26.
작성자 : 정성모
코드개요 : 소켓서버에 연속적으로 소켓을 보내기 위한 client
           (100,000개의 데이터를 소켓 서버로 보냄)
'''
import socket
import time
from datetime import datetime

t = time.time()
count = 0
with open("uuid_tb.txt","r") as fr:
    while True:
        uid_tb = fr.readline()
        if not uid_tb: break
        HOST = '192.168.1.16'  
        PORT = 9990 

        client_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        try:
            client_socket.connect((HOST, PORT))
    
            data = uid_tb
            client_socket.sendall(data.encode())

            test = client_socket.recv(1024)
#            client_socket.close()
            count += 1
        except Exception as e:
            print(client_socket)
        finally:
            client_socket.close()

print(time.time() - t)
