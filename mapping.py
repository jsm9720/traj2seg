'''
작성일 : 2020. 12. 26
작성자 : 정성모
코드개요 : 궤적 데이터를 표준노드링크 데이터를 이용하여 궤적이 어떤 링크에 있는 지 매핑하는 코드
'''
from concurrent.futures import ProcessPoolExecutor
from haversine import haversine
from math import radians, sin, cos, degrees, atan2

import sys
import json
import time
import os


def trajectory2segment(traj, cell, link_id, F_NODE):
    '''
    함수 개요 :
        궤적 link_id 매핑
    매개 변수 :
        traj (list) = 궤적([[위도,경도],[위도,경도],...])
        cell (dict) = key : cell_id, value : link_ids (list)
        link_id (dict) = key : link_id, value : link_id_info(from.표준노드링크)
        F_NODE (dict) = key : from_node, value : 연결된 link_id (list)
    '''
    # print("Executing our Task on Process : {}".format(os.getpid()))

    segment_index = 0 # 궤적이 지나는 세그먼트들 중에서 GPS 한점과 가장 가까운 세그먼트를 찾기위한 인덱스
    end_segment_index = 0 # 매핑할 세그먼트에 마지막 포인트의 인덱스
    error_weight = 0 # 매핑 추정 과정에서 반복적으로 오류가 발견될 때 확인하는 변수
    count = 0 # 매핑 추정 과정의 리셋을 위한 카운터
    gps_index = 0 # 현재 GPS 인덱스

    segments_index = 0 # 궤적이 지나는 세그먼트들의 인덱스
    continue_index = 0
    continue_n_index = -1

    mapping = {} # 매핑된 데이터
    segments = [] # 궤적이 지나는 세그먼트들
    segment = [] # 궤적이 지나는 세그먼트들 중에 하나의 segment

    segment, start, start_next = find_segment(traj, 0, False, cell, link_id, F_NODE)
    segments.append(segment)

    start_segment_index = start
    s_gps = traj[start] # 점과 점사이의 거리가 임계값을 넘어갔을 때, 이전 GPSㅡ
    p2s_distance = 999999999
    heading = False
    next_seg_none = False
    p2p_value = 10
    p2s_value = 60 # 톨게이트와 라인과 최대 먼 거리가 50 후반 정도
    b_value = 10 # segment 범위를 벗어나지 않은 상태에서 다음 segment로 이동 할 경우를 처리하기 위해 boundary 사용
    s_number = 0

    for gps in traj[start_next:]:
    
        segment = []
        temp_segments = []
        gps_index = traj.index(gps) # 현재 GPS index
        

        if gps_index < continue_n_index-1:
            continue
            

        p2p_distance = min_distance(s_gps, gps)
        if p2p_distance > p2p_value:  # GPS들의 거리를 먼저구해 임계값의 포함되면 아래와 같은 계산을 하지 않고 매핑
            segment_range = impute_segment_range(gps,
                                                 segments[segments_index][segment_index]['geometry']['coordinates'][0][0],
                                                 segments[segments_index][segment_index]['geometry']['coordinates'][0][-1])
            
            boundary = min_distance(gps, segments[segments_index][segment_index]['geometry']['coordinates'][0][-1][::-1])

            if segment_range & (boundary > b_value):
                for idx in range(s_number ,len(segments[segments_index][segment_index]['geometry']['coordinates'][0])-1):
                    within_range = impute_segment_range(gps,
                                                        segments[segments_index][segment_index]['geometry']['coordinates'][0][idx],
                                                        segments[segments_index][segment_index]['geometry']['coordinates'][0][idx+1])
                    if within_range:
                    
                        p2s_distance = min_distance(gps, gps,
                                                    segments[segments_index][segment_index]['geometry']['coordinates'][0][idx],
                                                    segments[segments_index][segment_index]['geometry']['coordinates'][0][idx+1])

                        heading = bearing(s_gps, gps,
                                          segments[segments_index][segment_index]['geometry']['coordinates'][0][idx],
                                          segments[segments_index][segment_index]['geometry']['coordinates'][0][idx+1])
                        
                        s_number = idx
                        break
                    else:
                        p2s_distance = 999999
                    
            elif (not segment_range) | (boundary < b_value):

                if not next_seg_none:
                    segment, continue_index, continue_n_index = next_segment(str(segments[segments_index][segment_index]["properties"]["T_NODE"]),gps_index, traj, cell, link_id, F_NODE)
                    # 검토 사항
                    if segment == 0:
                        return mapping
                else:
                    segment = []

                #다음 세그먼트가 링크가 없는 경로로 이동시 처리
                if len(segment) == 0:
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

                        if within_range:
                        
                            p2s_distance = min_distance(traj[continue_index], traj[continue_index],
                                                        temp_segments[segments_index][segment_index]['geometry']['coordinates'][0][idx],
                                                        temp_segments[segments_index][segment_index]['geometry']['coordinates'][0][idx+1])

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

                s_gps = traj[gps_index] # s_gps을 현재 gps로 변환
                # 마지막 GPS 까지 확인 한 경우
                if gps_index == len(traj)-1:
                
                    end_segment_index = traj.index(s_gps)
                    segment_id = segments[segments_index][segment_index]['properties']['LINK_ID']
                    mapping[segment_id] = start_segment_index, end_segment_index

            # 링크 이동
            elif ((not segment_range) & (p2s_distance < p2s_value))|((boundary < b_value) & (p2s_distance < p2s_value)):

                segments.append(segment)
                segments_index = segments_index + 1
                segment_index = 0

                end_segment_index = traj.index(s_gps)
                segment_id = segments[segments_index-1][segment_index]['properties']['LINK_ID']
                mapping[segment_id] = start_segment_index, end_segment_index
                start_segment_index = gps_index # start_segment_index을 현재 gps index로 변환
                
                s_gps = traj[continue_index] # s_gps을 현재 gps로 변환
                continue
            
            #링크 유실, 매핑 오류
            elif segment_range & (p2s_distance > p2s_value) | (not segment_range) & (p2s_distance > p2s_value) :
                # 매핑 오류 및 링크 유실로 확정날 때, 오류가 발견된 처음 GPS 위치
                if error_weight == 0:
                    weight_s_gps = traj.index(s_gps) ### 확인 필요

                # 가중치 값을 이용하여 매핑 오류 추정
                if error_weight == 1:
                    # find_segment를 하여 링크 유실인지, 매핑 오류인지 확인
                    segment_or_index, pre, continue_n_index = find_segment(traj, gps_index, True, cell, link_id, F_NODE)
                    if type(segment_or_index) == tuple:
                        break
                    # 매핑 오류
                    elif not type(segment_or_index) == int:
                        if len(segment_or_index) == 0:
                            break
                        next_seg_none = False
                        error_weight = 0
                        segments.append(segment_or_index)
                        s_gps = traj[pre]
                        start_segment_index = pre
                        segments_index = segments_index + 1
                        s_number = 0
                        continue
                        
                    # 링크 유실
                    elif type(segment_or_index) == int:

                        segment, pre, continue_n_index = find_segment(traj, segment_or_index, False, cell, link_id, F_NODE)
                        error_weight = 0

                        if (segment == 0)|(len(segment)==0):
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

    return mapping

def find_segment(traj, gps_index, mode, cell, link_id, F_NODE):
    '''
    함수개요 : 궤적의 포인트와 가장 가까운 link들을 찾음
    매개변수 :
        traj = 궤적
        gps_index = 현재 검사중인 궤적포인트의 인덱스
        mode = boolean 타입으로 True, 현재 포인트에서 다음 3번 포인트까지 검사 후 리턴 False, 가장 가까운 link가 나올떄까지 포인트들을 검사
        cell (dict) = key : cell_id, value : link_ids (list)
        link_id (dict) = key : link_id, value : link_id_info(from.표준노드링크)
        F_NODE (dict) = key : from_node, value : 연결된 link_id (list)
    '''

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
                                continue

                            s2p_d = min_distance(gps, gps, temp_seg[j], temp_seg[j+1])
                            if s2p_d > 20:
#                                del_list.append(seg)
                                continue
                            # 두 segment 중에 올바른 segment가 길이가 짧아 범위밖을 나가 잘못된 segment로 판단하는 경우 처리
                            if boundary < 20:
                                segments = []
                                segments.append(link_id[seg])
                                if mode:
                                    return segments, gps_index, next_gps
                                else:
                                    return segments, gps_index, next_gps 
                            
                            temp.append(seg)
                            s_temp.add(seg)
                            segments.append(link_id[seg])
                            break
            
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

    return segments, gps_index, next_gps

def impute_segment_range(gps, segment_s_gps, segment_e_gps, T_F=False):
    '''
    함수개요 : paul Bourke의 Minimum Distance between a Point and a Line의 세그먼트와 점이 직교하는 위치 확인
              -1 < x < 1 일때 특정 점이 세그먼트 범위 안에 포함 
    매개변수 :
        gps = 현재 기준인 궤적의 포인트
        segment_s_gps = 세그먼트의 시작 포인트
        segment_e_gps = 세그먼트의 끝 포인트
        T_F = True) True or False와범위에 포함 여부 공식 계산 값, False) 포함 여부 True or False
    '''

    x1 = segment_s_gps[1]
    y1 = segment_s_gps[0]
    x2 = segment_e_gps[1]
    y2 = segment_e_gps[0]
    x3 = gps[0] # 데이터 형태에 따라 바꿔줘야함
    y3 = gps[1]

    result = False
    if ((y2-y1) == 0) & ((x2-x1) == 0):
        return result

    value = ((x3-x1)*(x2-x1)+(y3-y1)*(y2-y1))/((x2-x1)**2+(y2-y1)**2)
#    print("range : ",value)

    if (value > 0) & (value <= 1):
        result = True
    else:
        result = False
    if T_F:
        return result, value
    else:
        return result

def min_distance(s_gps, e_gps=None, s_segment=None, e_segment=None):
    '''
    함수개요 : 2개의 포인트의 거리를 계산 or GPS와 segment의 거리 계산
    매개변수 :
        s_gps = 포인트1
        e_gps = 포인트2
        s_segment = 세그먼트 처음 부분 포인트
        e_segment = 세그먼트 끝 부분 포인트
    '''
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
    함수개요 : segment와 주어진 포인트가 직교하는 포인트를 찾는 함수
    매개변수 : 
        x1, y1 = s_segment GPS
        x2, y2 = e_segment GPS
        x3, y3 = trajectory GPS
    '''

    f_a = ((y2-y1)/(x2-x1))
    f_b = ((y2-y1)/(x2-x1))*-x1+y1
    
    a = -f_a**-1
    b = f_a**-1*x3+y3

    x = (b-f_b)/(f_a-a)
    y = a*x+b
    return x, y

def bearing(s_gps, e_gps, seg_point1, seg_point2):
    '''
    함수개요 : 2 GPS의 heading value와 segment heading value를 비교하여 유사하면 True, 아니면 False
               (오차 +45도 or -45도까지 유사로 판정)
    매개변수 :
        s_gps = 포인트1
        e_gps = 포인트2
        seg_point1 = segment start point
        seg_point2 = segment end point
    '''

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
    '''
    함수개요 : 북쪽을 기준으로 360도 방향을 수치화
    매개변수 :
        s_lat = 시작부분 위도
        e_lng = 시작부분 경도
        s_lat = 끝부분 위도
        e_lng = 끝부분 위도
    '''
    
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
def next_segment(target, gps_index, traj, cell, link_id, F_NODE):
    '''
    함수개요 : 기존 segment의 F_node, T_node를 통해서 연결되는 segemtn를 찾는 함수 
    매개변수 :
        target = 기존 segemtn에서 T_node로 끝나는 node 번호
        gps_index = 현재 궤적 포인트의 index
        traj = 궤적
        cell (dict) = key : cell_id, value : link_ids (list)
        link_id (dict) = key : link_id, value : link_id_info(from.표준노드링크)
        F_NODE (dict) = key : from_node, value : 연결된 link_id (list)
    '''
    count = 0
    error_count = 0
    next_seg = []
    temp_segment = {}
    
    if not F_NODE.get(target):
        return [], gps_index, gps_index
    else:
        next_seg = F_NODE[target]

    for i in range(len(next_seg)):
        temp_segment[i] = 0
    
    for i in range(gps_index,len(traj)-1):
        p2p_d = min_distance(traj[gps_index],traj[i+1])
        if p2p_d > 10:
            next_gps = i + 1
            del_list = []
            segment = []
            for seg in next_seg:
                gps = traj[gps_index]
                seg_info = link_id[seg]
                s_seg = seg_info["geometry"]["coordinates"][0][0]
                e_seg = seg_info["geometry"]["coordinates"][0][-1]
                s_seg_next = seg_info["geometry"]["coordinates"][0][1]
                boundary = min_distance(gps, seg_info["geometry"]["coordinates"][0][-1][::-1])

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

                    temp_seg = seg_info['geometry']['coordinates'][0]
                    for j in range(len(temp_seg)-1):

                        within_range = impute_segment_range(gps, temp_seg[j], temp_seg[j+1])
                        
                        if within_range:
                            heading = bearing(gps, traj[next_gps], temp_seg[j], temp_seg[j+1])

                            if not heading:
#                                del_list.append(seg)
                                continue
                            s2p_d = min_distance(gps, gps, temp_seg[j], temp_seg[j+1])
                            if s2p_d > 60:
#                                del_list.append(seg)
                                continue
                            # 두 segment 중에 올바른 segment가 길이가 짧아 범위밖을 나가 잘못된 segment로 판단하는 경우 처리
                            if boundary < 30:
                                segment = []
                                segment.append(seg_info)
                                return segment, gps_index, next_gps

                            segment.append(seg_info)
                            value = temp_segment.get(next_seg.index(seg))
                            temp_segment[next_seg.index(seg)] = value+1
                            break

            if (len(next_seg) == 1)|(len(next_seg) == 0):
                return segment, gps_index, next_gps
            
            result = sorted(temp_segment.items(), key=lambda item: item[1])
            k1, v1 = result[-1]
            k2, v2 = result[-2]
            count += 1
            if count == 3:
                if v1 != v2:
                    segment = []
                    segment.append(link_id[next_seg[k1]])
                    return segment, gps_index, next_gps

            if not len(segment) == 1:
                # 다음 세그먼트 찾는 과정 중 링크가 없는 지역으로 이동한 경우
                if len(segment) == 0:
                    error_count += 1
                    if error_count == 3:
                        return segment, gps_index, next_gps
                gps_index = next_gps
                
            else:
                if v1 != v2:
                    return segment, gps_index, next_gps
                else:
                    gps_index = next_gps
    return 0, 0, 0
