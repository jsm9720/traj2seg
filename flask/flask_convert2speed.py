import time

'''
함수개요 : 매핑데이터를 통해 링크별 해당되는 궤적 포인트들의 속도들을 평균하여 해당 링크의 속도로 지정
data = [[point],[]] : 궤적의 포인트 데이터
mapping_data = {seq=(traj_start_index, traj_end_index),seq=(,)} : 궤적을 링크 별로 매핑한 데이터
'''
def convert2speed(data, mapping_data):
    speed = {}
    for md in mapping_data:
        traj_index = mapping_data[md]
        point_time = float(data[traj_index[0]][-2])/1000
        link_first_time = time.strftime('%Y-%m-%d %H:%M:%S',time.localtime(point_time))

        temp = 0
        for i  in range(traj_index[0],traj_index[1]+1):
            temp += float(data[i][-3])*3.6
        avg_speed = temp / (traj_index[1]-traj_index[0]+1)
        speed[md]=[avg_speed,link_first_time]
    return speed

#data = convert2speed(pre, mapping_data)
#print(data)
