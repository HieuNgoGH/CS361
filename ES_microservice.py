import zmq
import json
import time
import random
import pandas as pd

print("Starting ES_microservice server...\n")
context = zmq.Context()
socket = context.socket(zmq.REP)
socket.bind("tcp://*:5555")

Exoplanet_dict = {}
df = pd.read_csv('planetary_systems.csv')
col_list = ['pl_name', 'hostname', 'sy_snum', 'sy_pnum', 'sy_dist', 'disc_year', 'disc_facility',
            'pl_controv_flag', 'pl_orbper', 'pl_rade', 'pl_bmasse', 'st_spectype', 'st_teff', 'st_rad',
            'st_mass', 'pl_pubdate', 'releasedate', 'pl_refname']


def generate_rand():
    num = random.randint(0, len(df))
    get_data(num)


def get_data(request):
    for col in range(len(col_list)):
        value = str(df.at[request, col_list[col]])
        if col_list[col] == 'pl_refname':
            value = value.partition('href=')[2].partition(' target=')[0]
        Exoplanet_dict[f'{col_list[col]}'] = value
    print(Exoplanet_dict, '\n')
    socket.send(json.dumps(Exoplanet_dict).encode("utf-8"))


while True:
    #  Wait for  next request from client
    message = json.loads(socket.recv().decode('utf-8'))
    print(f"Received request: {message}")

    time.sleep(1)
    # The request is for a random data row
    if message == 'Random':
        print('random request')
        generate_rand()
    # No name exist in the file from the request
    elif message not in df.pl_name.values:
        print('invalid request')
        socket.send(json.dumps('invalid').encode("utf-8"))
    # The request is a specific exoplanet name
    else:
        print('name request exists')
        row_num = df[df['pl_name'] == message].index.to_numpy()[0]
        get_data(row_num)
