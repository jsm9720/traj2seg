#-*- coding: utf-8 -*-

from haversine import haversine
from math import radians, sin, cos, degrees, atan2

import sys
import json
import time

x_min = 32.950424
y_min = 124.773835
x_max = 38.763189
y_max = 131.563393
x_d = x_max - x_min
y_d = y_max - y_min

h_x_d = 589*2 # 634은 우리나라 전체의 위도 거리 634km
h_y_d = 647*2

per_x_cell = x_d/h_x_d
per_y_cell = y_d/h_y_d

traj = []
with open("POINT_20200925.txt","r") as f:
    for i in range(25):
        line = f.readline()
    line = line.split(',')
    line = line[2:]
    line = ",".join(line)
    data = json.loads(line)
    data = data["items"]

    for i in data:
        gps = []
        lat = i["lat"]
        lng = i["lng"]
        gps = [lat,lng]
        traj.append(gps)

# def trajectory2segment(traj, param_segment_index=0):
def trajectory2segment(param_segment_index=0, param_segments=[], param_start=0, param_start_next=0):

    start = param_start
    start_next = param_start_next
    segment_index = param_segment_index # 궤적이 지나는 세그먼트들 중에서 GPS 한점과 가장 가까운 세그먼트를 찾기위한 인덱스
    segments = param_segments # 궤적이 지나는 세그먼트들
    end_segment_index = 0 # 매핑할 세그먼트에 마지막 포인트의 인덱스
    error_weight = 0 # 매핑 추정 과정에서 반복적으로 오류가 발견될 때 확인하는 변수
    count = 0 # 매핑 추정 과정의 리셋을 위한 카운터
    gps_index = 0 # 현재 GPS 인덱스

    segments_index = 0 # 궤적이 지나는 세그먼트들의 인덱스
    continue_index = 0
    continue_n_index = -1

    mapping = {} # 매핑된 데이터
    segment = [] # 궤적이 지나는 세그먼트들 중에 하나의 segment
 
    if param_segment_index == 0:

        segment, start, start_next = find_segment(0, False)
        segments.append(segment)
        
    print(segments)
    start_segment_index = start # 매핑할 세그먼트에 첫번재 포인트의 인덱스@@@@@@@@@@@@@@@ 0 값을 첫번째 GPS 값으로 바꿔줘야함
    s_gps = traj[start] # 점과 점사이의 거리가 임계값을 넘어갔을 때, 이전 GPSㅡ
    p2s_distance = 999999999
    heading = False
    next_seg_none = False
    p2p_value = 10
    p2s_value = 60 # 톨게이트와 라인과 최대 먼 거리가 50 후반 정도
    b_value = 10 # segment 범위를 벗어나지 않은 상태에서 다음 segment로 이동 할 경우를 처리하기 위해 boundary 사용
    s_number = 0

    m_count = 0
    l_count = 0

    for gps in traj[start_next:]: # 수정 : index로 변경 해야함
        
        segment = []
        temp_segments = []
        gps_index = traj.index(gps) # 현재 GPS index
        print(gps_index)
        if gps_index < continue_n_index-1:
            continue
            
        print(traj.index(s_gps), traj.index(gps))
        p2p_distance = min_distance(s_gps, gps)
        if p2p_distance > p2p_value:  # GPS들의 거리를 먼저구해 임계값의 포함되면 아래와 같은 계산을 하지 않고 매핑
            segment_range = impute_segment_range(gps,
                                                 segments[segments_index][segment_index]['geometry']['coordinates'][0][0],
                                                 segments[segments_index][segment_index]['geometry']['coordinates'][0][-1])
            
            print("range",segment_range, gps_index)
            print("segments_index", segments_index)
            print(gps)
            boundary = min_distance(gps, segments[segments_index][segment_index]['geometry']['coordinates'][0][-1][::-1])

            if segment_range & (boundary > b_value):
                print("test", boundary)
                for idx in range(s_number ,len(segments[segments_index][segment_index]['geometry']['coordinates'][0])-1):
                    within_range = impute_segment_range(gps,
                                                        segments[segments_index][segment_index]['geometry']['coordinates'][0][idx],
                                                        segments[segments_index][segment_index]['geometry']['coordinates'][0][idx+1])
                    if within_range:
                    
                        p2s_distance = min_distance(gps, gps,
                                                    segments[segments_index][segment_index]['geometry']['coordinates'][0][idx],
                                                    segments[segments_index][segment_index]['geometry']['coordinates'][0][idx+1])

                        print("point to segemint distance : ",p2s_distance)
                        heading = bearing(s_gps, gps,
                                          segments[segments_index][segment_index]['geometry']['coordinates'][0][idx],
                                          segments[segments_index][segment_index]['geometry']['coordinates'][0][idx+1])
                        
                        s_number = idx
                        break
                    else:
                        p2s_distance = 999999
                    
            elif (not segment_range) | (boundary < b_value):
                print(segment_range)
#                segment, contiune_index, pre_index = next_segments(segments[segments_index][segment_index][link_id], gps, gps_index, count) # 다음 새그먼트를 현재 새그먼트로 변환, count : 임계 값
                print("범위를 넘음")
                print("p2p_distance", p2p_distance)
                if not next_seg_none:
                    segment, continue_index, continue_n_index = next_segment(str(segments[segments_index][segment_index]["properties"]["T_NODE"]),gps_index)
                else:
                    segment = []
                print(segment)
                #다음 세그먼트가 링크가 없는 경로로 이동시 처리
                if len(segment) == 0:
#                    p2s_distance = min_distance(traj[continue_index], segments[segments_index][segment_index]['geometry']['coordinates'][0][-1][::-1])
                    p2s_distance = 999999
                    next_seg_none = True
                else:
                    temp_segments.append(segment)
                    s_number = 0
                    temp_segment_index = 0
                    temp_segments_index = 0

                    if segment_index != 0:
                        temp_segment_index = segment_index
                        segment_index = 0

                    if segments_index != 0:
                        temp_segments_index = segments_index
                        segments_index = 0

                    for idx in range(s_number ,len(temp_segments[segments_index][segment_index]['geometry']['coordinates'][0])-1):
                    
                        within_range = impute_segment_range(traj[continue_index],
                                                            temp_segments[segments_index][segment_index]['geometry']['coordinates'][0][idx],
                                                            temp_segments[segments_index][segment_index]['geometry']['coordinates'][0][idx+1])
                        print(idx)
                        print(traj[continue_index])

                        if within_range:
                        
                            p2s_distance = min_distance(traj[continue_index], traj[continue_index],
                                                        temp_segments[segments_index][segment_index]['geometry']['coordinates'][0][idx],
                                                        temp_segments[segments_index][segment_index]['geometry']['coordinates'][0][idx+1])
                            print("point to segemint distance : ",p2s_distance)

                            heading = bearing(traj[continue_index], traj[continue_n_index],
                                            temp_segments[segments_index][segment_index]['geometry']['coordinates'][0][idx],
                                            temp_segments[segments_index][segment_index]['geometry']['coordinates'][0][idx+1])
                            segment_index = temp_segment_index
                            segments_index = temp_segments_index
                            break
                        else:
                            p2s_distance = 999999


            # 유지
            if segment_range & (p2s_distance < p2s_value) & heading & (boundary > b_value): # (heading 오류의 관한 처리 필요)
                print("유지")
                print("-----------------------------------------------------")
                s_gps = traj[gps_index] # s_gps을 현재 gps로 변환
                # 마지막 GPS 까지 확인 한 경우
                if gps_index == len(traj)-1:
                
                    end_segment_index = traj.index(s_gps)
                    segment_id = segments[segments_index][segment_index]['properties']['LINK_ID']
                    mapping[segment_id] = start_segment_index, end_segment_index
                    print("mapping1",mapping)


            
            # 링크 이동
            elif ((not segment_range) & (p2s_distance < p2s_value))|((boundary < b_value) & (p2s_distance < p2s_value)):
            #elif (not segment_range) & (p2s_distance < p2s_value):
                print("boundary",boundary < b_value)
                print("p2s_distance < p2s_value",p2s_distance < p2s_value)
                print("링크 이동")
                segments.append(segment)
                segments_index = segments_index + 1
                segment_index = 0
#                if start_segment_index == end_segment_index:
                    
#                end_segment_index = traj.index(gps)-1
                end_segment_index = traj.index(s_gps)
                segment_id = segments[segments_index-1][segment_index]['properties']['LINK_ID']
                mapping[segment_id] = start_segment_index, end_segment_index
#                start_segment_index = end_segment_index+1 # start_segment_index을 현재 gps index로 변환
                start_segment_index = gps_index # start_segment_index을 현재 gps index로 변환
                print("mapping2", mapping)
                
                s_gps = traj[continue_index] # s_gps을 현재 gps로 변환
                continue

            
            #링크 유실, 매핑 오류
            elif segment_range & (p2s_distance > p2s_value) | (not segment_range) & (p2s_distance > p2s_value) :
                print("링크 유실, 매핑 오류")
                # 매핑 오류 및 링크 유실로 확정날 때, 오류가 발견된 처음 GPS 위치
                if error_weight == 0:
                    weight_s_gps = traj.index(s_gps)

                # 가중치 값을 이용하여 매핑 오류 추정
                if error_weight == 1:
                    # find_segment를 하여 링크 유실인지, 매핑 오류인지 확인
                    segment_or_index, pre, continue_n_index = find_segment(gps_index, True)
                    print("segemnt_or_index", segment_or_index)
                    if type(segment_or_index) == tuple:
                        break
                    
                    # 매핑 오류
                    elif not type(segment_or_index) == int:
                        next_seg_none = False
                        error_weight = 0
                        print("매핑 오류") 
                        segments.append(segment_or_index)
                        s_gps = traj[pre]
                        start_segment_index = pre
                        segments_index = segments_index + 1
                        s_number = 0
                        continue
#                        segment_index = segment_index + 1
#                        mapping = trajectory2segment(segment_index, segments, start, start_next)
#                        print("mapping3",mapping)
#                        break
                        
                    # 링크 유실
                    elif type(segment_or_index) == int:
                        print("type seg",type(segment_or_index))
                        print("링크 유실")
                        l_count + 1
                        print("링크 유실로 스킵 l_count",l_count)
                        # if next_seg_none:
                        #     print("next segment none")
                        #     end_segment_index = weight_s_gps-1
                        #     segment_id = segments[segments_index][segment_index]['properties']['LINK_ID']
                        #     mapping[segment_id] = start_segment_index, end_segment_index
                        #     next_seg_none = False
                        #     print("mapping4",mapping)

                        segment, pre, continue_n_index = find_segment(segment_or_index)
                        error_weight = 0
                        print(segment)
                        print("segment, pre, now ",segment, pre, continue_n_index)
                        print(error_weight)
                        if segment == 0:
                            break
                        else:
                            segments.append(segment)
                            s_gps = traj[pre]
                            start_segment_index = pre
                            segments_index = segments_index + 1
                            s_number = 0
                            next_seg_none = False
                            continue

                error_weight = error_weight + 1
            

            if next_seg_none:
                s_gps = traj[continue_index]
            else:
                s_gps = gps

            # 잠깐의 오류로 임계값을 넘은경우를 위한 리셋 기능
            count = count + 1
            if count == 3:
            
                error_weight = 0

        else:
            print("범위안", gps_index )
    return mapping

def find_segment(gps_index, mode=False):
    # GPS에 가장 가까운 세그먼트 k 개를 찾음
    # segments = [{link_id:"123",F_node, T_node, Lenth, geometry:[gps1, ... gpsn]}, ...]
    # return segments, -1
    next_gps = 0
    count = 0
    segments = []
    temp = []
    s_temp = set()
    for i in range(gps_index, len(traj)-1):
        p2p_d = min_distance(traj[gps_index],traj[i+1])

        if p2p_d > 10:

            next_gps = i + 1
            gps = traj[gps_index]
            grid_x = int((gps[0]-x_min)/per_x_cell)
            grid_y = int((gps[1]-y_min)/per_y_cell)
            selected_cell = (grid_x*h_x_d)+grid_y
            cell_range = [selected_cell - h_x_d - 1, selected_cell - h_x_d, selected_cell - h_x_d + 1,
                          selected_cell - 1, selected_cell, selected_cell + 1,
                          selected_cell + h_x_d - 1, selected_cell + h_x_d, selected_cell + h_x_d + 1]

            li = []
            for cell_num in cell_range:
                if not cell.get(cell_num):
                    continue
                li = li + cell[cell_num]
            segs_in_cells = set(li)
            segs_in_cells = list(segs_in_cells)
            

            for seg in segs_in_cells:
                s_seg = link_id[seg]["geometry"]["coordinates"][0][0]
                e_seg = link_id[seg]["geometry"]["coordinates"][0][-1]
                s_seg_next = link_id[seg]["geometry"]["coordinates"][0][1]

                boundary = min_distance(gps, link_id[seg]["geometry"]["coordinates"][0][-1][::-1])
                seg_range = impute_segment_range(gps, s_seg, e_seg)
                if seg_range:

                    temp_seg = link_id[seg]['geometry']['coordinates'][0]
                    for j in range(len(temp_seg)-1):

                        within_range = impute_segment_range(gps, temp_seg[j], temp_seg[j+1])
                        
                        if within_range:
                            heading = bearing(gps, traj[next_gps], temp_seg[j], temp_seg[j+1])

                            if not heading:
                                #print("heading cut")
                                #print(gps_index,gps, traj[next_gps], temp_seg[j], temp_seg[j+1])
#                                del_list.append(seg)
                                continue

                            s2p_d = min_distance(gps, gps, temp_seg[j], temp_seg[j+1])
                            if s2p_d > 20:
                                #print("distance cut")
                                #print(gps_index,gps, traj[next_gps], temp_seg[j], temp_seg[j+1])
#                                del_list.append(seg)
                                continue
                            # 두 segment 중에 올바른 segment가 길이가 짧아 범위밖을 나가 잘못된 segment로 판단하는 경우 처리
                            if boundary < 20:
                                segments = []
                                segments.append(link_id[seg])
                                print("boundary return")
                                if mode:
                                    return segments, gps_index, next_gps
                                else:
                                    return segments, gps_index, next_gps 
                            
                            temp.append(seg)
                            s_temp.add(seg)
                            segments.append(link_id[seg])
                            break

#                 #수정 필요
#                 seg_range = impute_segment_range(gps, s_seg, e_seg)
#                 if not seg_range:
# #                    print("fs_range cut")
#                     continue
        
#                 heading = bearing(gps, traj[next_gps], s_seg, s_seg_next)
#                 if not heading:
# #                    print("fs_heading cut")
#                     continue
        
#                 s2p_d = min_distance(gps, gps, s_seg, e_seg)
#                 if s2p_d > 60:
# #                    print("fs_distance cut")
#                     continue
#                 temp.append(seg)
#                 s_temp.add(seg)

            
            if mode:
                if count == 3:
                    if len(segments) != 0:
                        l_temp=list(s_temp)
                        a = []
                        for num in l_temp:
                            a.append(temp.count(num)) 
                        segments = []
                        segments.append(link_id[l_temp[a.index(max(a))]])
                        return segments, gps_index, next_gps
                    else:
                        return next_gps, gps_index, next_gps
            elif len(segments) != 0:
                l_temp=list(s_temp)
                a = []
                for num in l_temp:
                    a.append(temp.count(num))
                segments = []
                segments.append(link_id[l_temp[a.index(max(a))]])
                return segments, gps_index, next_gps
            gps_index = next_gps
            count += 1

    return 0, 0, 0

def read_seg():
    cell = {}
    link_id = {}
    with open("2019_09_20.geojson", "r") as f:
        count = 0
        while True :
            temp = set()
            line = f.readline()
            if not line: break
            if "LINK_ID" in line:
                try:
                    j = json.loads(line[:-2])
#                    print(j)
                except:
                    j = json.loads(line)
#                    print(j)
                link = j["properties"]["LINK_ID"]
                coordinates = j["geometry"]["coordinates"]
                for gps in coordinates[0]:
                    grid_x = int((gps[1]-x_min)/per_x_cell)
                    grid_y = int((gps[0]-y_min)/per_y_cell)
                    temp.add((grid_x*h_x_d)+grid_y)
                temp = list(temp)
                
                for cell_num in temp:
                    li = []
                    if not cell.get(cell_num):
                        cell[cell_num] = []
                    li = cell.get(cell_num)
                    li.append(link)
                    cell[cell_num] = li
                link_id[link] = j
    return cell, link_id

def impute_segment_range(gps, segment_s_gps, segment_e_gps, T_F=False):

    # paul Bourke의 Minimum Distance between a Point and a Line의 세그먼트와 점이 직교하는 위치에 있는지 확인해주는 공식 이용
    # gps가 segment의 양 끝 점을 기준으로 포함되었는지 확인
    # return True or False

    x1 = segment_s_gps[1]
    y1 = segment_s_gps[0]
    x2 = segment_e_gps[1]
    y2 = segment_e_gps[0]
    x3 = gps[0] # 데이터 형태에 따라 바꿔줘야함
    y3 = gps[1]

    result = False

    value = ((x3-x1)*(x2-x1)+(y3-y1)*(y2-y1))/((x2-x1)**2+(y2-y1)**2)
#    print("range : ",value)

    if (value > 0) & (value <= 1):
#    if value <= 1:
        result = True
    else:
        result = False
    if T_F:
        return result, value
    else:
        return result

def min_distance(s_gps, e_gps=[], s_segment=[], e_segment=[]):

    # 2개의 GPS의 거리를 계산 or GPS와 segment의 거리를 계산
    # return int
    value = 0
    if len(s_segment) > 0:
        x, y = point_in_seg(s_segment[1], s_segment[0], e_segment[1], e_segment[0], s_gps[0], s_gps[1])
        pre_point = (x, y)
        point = (s_gps[0], s_gps[1])
        value = haversine(pre_point, point, unit='m')
    else:
        pre_point = (s_gps[0], s_gps[1])
        point = (e_gps[0], e_gps[1])
        value = haversine(pre_point, point, unit='m')
    return value

def point_in_seg(x1, y1, x2, y2, x3, y3):

    '''
    함수 개요 : segment와 궤적의 직교하는 점을 구하기 위한 함수
    매개 변수 : x1, y1 = s_segment GPS
                x2, y2 = e_segment GPS
                x3, y3 = trajectory GPS
    함수 결과 : segment내에서 궤적의 GPS와 직교하는 위치 정보(위도, 경도)
    '''

    f_a = ((y2-y1)/(x2-x1))
    f_b = ((y2-y1)/(x2-x1))*-x1+y1
    
    a = -f_a**-1
    b = f_a**-1*x3+y3

    x = (b-f_b)/(f_a-a)
    y = a*x+b
    return x, y

def bearing(s_gps, e_gps, seg_point1, seg_point2):

    # 두 GPS의 heading value와 segment heading value를 비교하여 유사하면 True, 아니면 Flase
    # return True or False
    gps_heading = bearing_calculation(s_gps[0], s_gps[1], e_gps[0], e_gps[1])
    seg_heading = bearing_calculation(seg_point1[1], seg_point1[0], seg_point2[1], seg_point2[0])
#    print("gps heading",gps_heading,"seg_heading", seg_heading)
    value = False
    if gps_heading <= 45:
        if (0 <= seg_heading <= gps_heading + 45) | ((360 - (45 - gps_heading)) <= seg_heading <= 360):
            value = True
            return value
    elif 315 <= gps_heading:
        if ((gps_heading-45) <= seg_heading <= 360) | (0 <= seg_heading <= ((gps_heading + 45) - 360)):
            value = True
            return value
    else:
        if ((gps_heading-45) <= seg_heading <= (gps_heading+45)):
            value = True
            return value
    return value

def bearing_calculation(s_lat, s_lng, e_lat, e_lng):
    lat1 = radians(s_lat)
    lat2 = radians(e_lat)
    diffLong = radians(e_lng - s_lng)

    b_x = sin(diffLong) * cos(lat2)
    b_y = cos(lat1) * sin(lat2) - (sin(lat1) * cos(lat2) * cos(diffLong))
    initial_bearing = atan2(b_x, b_y)
    initial_bearing = degrees(initial_bearing)
    compass_bearing = (initial_bearing + 360) % 360 
    return int(compass_bearing)

# def next_segments(segments, gps, gps_index, count):
def next_segment(target, gps_index):
    # segment의 F_node, T_node를 통해서 연결되는 segment를 찾고,
    # gps = traj[gps_index]부터 min_distance(gps, next_gps)을 계산해 임계값을 벗어나는 포인트 2개를 찾고,
    # 포인트 2개와 가장 가까운 segment를 리턴
    # segments = [{link_id:"123",F_node, T_node, Lenth, geometry:[gps1, ... gpsn]}, ...]
    # return segments, index, pre_index (수정필요 : gps_index or error(ex -1) 다음 gps계산을 미리 하기 때문에 - 재귀함수로 해결 할 예정)
    count = 0
    error_count = 0
    next_seg = []
    temp_segment = {}
    for link in link_id:
        f_node = link_id[link]["properties"]["F_NODE"]
        if f_node == target:
            next_seg.append(link_id[link])

    for i in range(len(next_seg)):
        temp_segment[i] = 0
    
    for i in range(gps_index,len(traj)-1):
        p2p_d = min_distance(traj[gps_index],traj[i+1])
        if p2p_d > 10:
            next_gps = i + 1
            del_list = []
            segment = []
            for seg in next_seg:
                print("seg",seg)
                print("segment len : ",len(next_seg))
                gps = traj[gps_index]
                print(gps)
                s_seg = seg["geometry"]["coordinates"][0][0]
                e_seg = seg["geometry"]["coordinates"][0][-1]
                s_seg_next = seg["geometry"]["coordinates"][0][1]
                
                boundary = min_distance(gps, seg["geometry"]["coordinates"][0][-1][::-1])

                # # 터널을 위한 처리
                # print("p2p_d",p2p_d)
                # if (p2p_d > 150) | (p2p_distance > 150):
                #     f_range = impute_segment_range(gps, s_seg,e_seg)
                #     e_range = impute_segment_range(traj[next_gps], s_seg, e_seg)
                #     heading = bearing(gps, traj[next_gps], s_seg, e_seg)
                #     if f_range | e_range:
                #         if heading:
                #             segment = []
                #             segment.append(seg)
                #             print("tunnel return")
                #             return segment, gps_index, next_gps
                #     else:
                #         if heading:
                #             target = str(seg["properties"]["T_NODE"])                            
                #             segment, gps_index, next_gps = next_segment(target, next_gps)
                #             return segment, gps_index, next_gps

                seg_range = impute_segment_range(gps, s_seg, e_seg)
                if seg_range:

                    temp_seg = seg['geometry']['coordinates'][0]
                    for j in range(len(temp_seg)-1):

                        within_range = impute_segment_range(gps, temp_seg[j], temp_seg[j+1])
                        
                        if within_range:
                            heading = bearing(gps, traj[next_gps], temp_seg[j], temp_seg[j+1])

                            if not heading:
                                print("heading cut")
                                print(gps_index,gps, traj[next_gps], temp_seg[j], temp_seg[j+1])
#                                del_list.append(seg)
                                continue

                            s2p_d = min_distance(gps, gps, temp_seg[j], temp_seg[j+1])
                            if s2p_d > 60:
                                print("distance cut")
                                print(gps_index,gps, traj[next_gps], temp_seg[j], temp_seg[j+1])
#                                del_list.append(seg)
                                continue
                            # 두 segment 중에 올바른 segment가 길이가 짧아 범위밖을 나가 잘못된 segment로 판단하는 경우 처리
                            if boundary < 30:
                                segment = []
                                segment.append(seg)
                                print("boundary return")
                                return segment, gps_index, next_gps

                            segment.append(seg)
                            value = temp_segment.get(next_seg.index(seg))
                            temp_segment[next_seg.index(seg)] = value+1
                            print(temp_segment)
                            break
                            
                else:
                    print("range cut")

            if len(next_seg) == 1:
                return segment, gps_index, next_gps

            
            result = sorted(temp_segment.items(), key=lambda item: item[1])
            print("result", result)
            k1, v1 = result[-1]
            k2, v2 = result[-2]
            count += 1
            if count == 3:
                if v1 != v2:
                    segment = []
                    segment.append(next_seg[k1])
                    print("sorted")
                    return segment, gps_index, next_gps

            if not len(segment) == 1:
                #print("temp_segment : ",temp_segment)
                # 다음 세그먼트 찾는 과정 중 링크가 없는 지역으로 이동한 경우
                # 임계값을 6을 준 이유는 세그먼트를 벗어났다고 판단하는 기준이 60m 이상
                # 포인트 끼리의 거리 계산 임계값이 10m 이상으로 거리 계산 진행 중
                if len(segment) == 0:
                    error_count += 1
                    if error_count == 3:
                        return segment, gps_index, next_gps
                print("temp_segment",temp_segment)
                print(gps)
                print("a_check\n",segment, len(segment))
                gps_index = next_gps
                
            else:
                if v1 != v2:
                    return segment, gps_index, next_gps
                else:
                    gps_index = next_gps

# 추가 사항
# 모든 링크는 연결 되어 있으므로 다음 세그먼트가 없는 경우는 없다.  

start = time.time()
cell, link_id = read_seg()

mapping = trajectory2segment()
print(mapping)
print(time.time()-start)
print("mapping len ",len(mapping))
print("traj len ",len(traj))
print("traj ",traj[:50])

