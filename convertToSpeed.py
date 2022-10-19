'''
작성일 : 2020. 12. 26.
작성자 : 정성모
코드개요 : 궤적을 링크별로 매핑한 데이터의 정보를 통해 궤적 포인트가 가진 시간를 사용하여 링크별 속도 추출
'''
import json
import time
from datetime import datetime

def convert2speed(traj_data, mapping_data):
    '''
    함수개요: 매핑데이터( link_id : 1~N까지,link_id : ... )를 통해 링큽별 해당되는 궤적 포인트들의 속도들을 평균하여 해당 링크의 속도로 지정
    매개변수:
        traj_data : 궤적의 원 데이터
        mapping_data : 궤적을 링크별로 매핑한 데이터
    '''
    speed = []
    min5_speed = []
    min5_date_time = ""
    jsondata = json.loads(traj_data[0]['jsondata'])
    point_data = jsondata[0]['items']
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
        speed.append([avg_speed, link_last_time, md])
        min5_speed.append([md+str(min5_date_time),int(second),avg_speed])

    return speed, min5_speed
