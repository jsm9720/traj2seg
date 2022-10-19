'''
작성일 : 2020. 12. 28.
작성자 : 정성모
코드개요 : 표준노드링크에서 제공하는 표준노드링크 데이터(`2019_09_20.geojson`)  파싱
'''
import json

def read_seg():
    '''
    함수개요 : 우리나라의 좌표를 이용하여 cell을 나누어 cell의 포함된 링크 파싱 및 링크별 정보 파싱 및 F_NODE 정보 파싱
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

    cell = {}
    link_id = {}
    F_NODE = {}
    T_NODE = {}
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
                f_node = j["properties"]["F_NODE"]
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

                if not F_NODE.get(f_node):
                    F_NODE[f_node] = []
                f_value = F_NODE.get(f_node)
                f_value.append(link)
                F_NODE[f_node] = f_value
    
    return cell, link_id, F_NODE
