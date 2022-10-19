import os
import sys
sys.path.append(os.path.dirname(os.path.abspath(os.path.dirname(__file__))))

from read_segment import read_seg
from mapping import trajectory2segment
from flask_convert2speed import convert2speed

from flask import Flask, request

app = Flask(__name__)

@app.route('/', methods=['GET',])
def index():
    return "Hello World!"

@app.route('/mapping', methods=['POST'])
def mapping():
    data = request.files['file'].read().decode('utf-8')
    preprocessing = [i.split(",") for i in data.split("\n")[1:-1]]
    traj_point = [list(map(float,i[2:4])) for i in preprocessing]
    mapping_data = mapping_data = trajectory2segment(traj_point, cell, link_id, F_NODE)
    speed_data = convert2speed(preprocessing, mapping_data)
    print(speed_data)
    return speed_data

if __name__ == "__main__":
    cell, link_id, F_NODE = read_seg()
    print("start server")
    app.run(host='192.168.1.16', port='9990')

