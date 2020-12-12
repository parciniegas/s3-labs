from datetime import datetime, timedelta, time, timezone
from joblib import Parallel, delayed
import json
import random
from numpy.core.fromnumeric import put
import pandas as pd
import uuid
import boto3
from reading import Reading, ReadingEncoder

def generate_sockets(num_sockets: int):
    sockets = [f"SOCKET_{x}" for x in range(num_sockets)]
    return sockets


def generate_readings(start_date: datetime, end_date: datetime, interval: int, client: str, sockets: int, source: str, bucket: str, plain: bool):
    count= 0
    dates = pd.date_range(start_date, end_date, freq=f'{interval}min')
    
    #number_of_cpu = joblib.cpu_count()
    delayed_funcs = [delayed(gen_readings_for_date)(date, sockets, client, source, bucket, plain) for date in dates]
    parallel_pool = Parallel(n_jobs=4)
    parallel_pool(delayed_funcs)

    #for date in dates:
    #    gen_readings_for_date(date, sockets, client, source, plain)
    #    count=+1
    #print(f"{count} readings generated")


def gen_readings_for_date(date: datetime, sockets: int, client: str, source: str, bucket: str, plain: bool):
    variables = ["Var-01", "Var-02", "Var-03", "Var-04"]
    versions = ["Original", "Usage"]
    sockets = [f"SOCKET-{x}" for x in range(sockets)]
    for socket in sockets:
        for variable in variables:
            for version in versions:
                reading = get_reading(client, socket, variable, date, source, version)
                put_reading(reading, bucket, plain)


def put_reading(reading: Reading, bucket: str, plain: bool):
    client = boto3.client('s3')
    json_reading = json.dumps(reading, cls=ReadingEncoder)
    date = reading.utc_datetime[0:10]
    key = f"{reading.id}"
    if not plain:
        key = f"dt={date}/src={reading.source}/med={reading.id_socket}/var={reading.channel}/{reading.datetime}"
    client.put_object(Bucket=bucket, Body=json_reading, Key=key)
    print(f"File {key} putted to s3")


def get_reading(client: str, socket: str, variable: str, date: datetime, source: str, version: str) -> Reading:
    reading = Reading(raw_data= f"{random.random() * 10}"[0:5],
                      ke=f"{random.random() * 10}"[0:5],
                      pt = f"{random.random() * 10}"[0:5],
                      ct = f"{random.random() * 10}"[0:5],
                      noins = f"MED {random.randint(1, 10)}",
                      id_socket= socket,
                      id_client= client,
                      account_no= f"Account {client}",
                      kva_number= f"Kva {random.randint(1, 10)}",
                      custom_name= variable,
                      channel= variable,
                      uom= "Kw",
                      date = date.isoformat()[0:10]+"T00:00:00",
                      datetime= (date - timedelta(hours=5)).isoformat(),
                      utc_datetime=date.isoformat(),
                      dst_flag= False,
                      no_intervals= random.randint(1, 50),
                      sf = random.randint(1, 1000),
                      is_backup=False,
                      num_log= random.randint(1, 50),
                      received_date= datetime.now().isoformat(),
                      origin_version= "Usage",
                      validation_status= "Approved",
                      register_date= datetime.now().isoformat(),
                      source= source,
                      version = version,
                      version_number= 1,
                      id = f"{uuid.uuid4()}",
                      value= f"{random.random() * 1000}"[0:6])
    return reading

start = datetime.now()
generate_readings(start_date= datetime(2020,11,6,0,0,0,0), 
                  end_date= datetime(2020,11,30,23,59,59,9999), 
                  interval= 15, client= "BPA", sockets= 4, source= "Primeread", 
                  bucket= "pad-datalake", plain= True)
stop = datetime.now()
print(f"Elapsed time: {start - stop}")
#print(reading)