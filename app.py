import pandas as pd
from placekey.api import PlacekeyAPI
import math
import threading
import time

GLOBAL_RES = list()

COUNTRY = "small-US"
ISO_CODE = "us"

PATH_IN= "./res"
PATH_OUT= "./out"
FILE_IN = '{0}/{1}.csv'.format(PATH_IN, COUNTRY)
FILE_OUT = '{0}/{1}_out.csv'.format(PATH_OUT, COUNTRY)

ROWS = 1000
BATCH_SIZE = 100
THREADS = 10
CHUNK_SIZE = math.ceil(ROWS / THREADS)

API  = PlacekeyAPI("gzOGnw0x8SsiJ4cM4TzI4F9yesp1Oul4")

class ExecutionThread():

    def __init__(self, id, input):
        self.id = id
        self.input = input
        self.thread = threading.Thread(target=ExecutionThread.process, args=(self,), name=f'Thread - {id}')
        self.output = list()
        self.finished = False

    def start(self):
        self.thread.start()

    def on_stop(self):
        for set in self.output:
            for _, entry in enumerate(set):
                GLOBAL_RES.append(entry)

        self.finished = True
    
    def get_id(self):
        return self.id

    def get_input(self):
        return self.input

    def get_input_subsection(self, lower, higher):
        return self.input[lower:higher]

    def get_output(self):
        return self.output

    def is_finished(self):
        return self.finished
    
    def size(self):
        return len(self.input)

    def append(self, array):
        self.output.append(array)

    @classmethod
    def process(clz, target):
        
        iterations = math.ceil(target.size() / BATCH_SIZE)

        for i in range(iterations):
            print(f"{target.get_id()} > Batch #{i + 1}/{iterations}")
            target.append(API.lookup_placekeys(target.get_input_subsection(i * BATCH_SIZE, min((i+1) * BATCH_SIZE, target.size()))))
        
        target.on_stop()

        return

    @classmethod
    def get_chunks(clz, input, sub_size):
        for i in range(0, len(input), sub_size):
            yield input[i:i + sub_size]

    @classmethod
    def finished(clz, thread_lst):
        for thread in thread_lst:
            if not thread.is_finished():
                return False
        return True


df = pd.read_table(FILE_IN, sep=',', encoding="utf_8")

data = list()

for index, entry in df.iterrows():
    data.append(dict({
        "query_id": str(index),
        "street_address": str(entry[5]),
        "postal_code": str(entry[6]),
        "city": str(entry[7]),
        "region": str(entry[8]),
        "iso_country_code": ISO_CODE
    }))

threads = list()
chunks = list(ExecutionThread.get_chunks(data, CHUNK_SIZE))

for i in range(THREADS):
    threads.append(ExecutionThread(f"Thread - {i}", chunks[i]))
    threads[i].start()
    time.sleep(0.2)

while not ExecutionThread.finished(threads):
    pass

values = list()
for i, e in enumerate(sorted(GLOBAL_RES, key=lambda x: x['query_id'])):
    try:
        values.append(e['placekey'])
    except KeyError:
        try:
            values.append(['error'])
        except KeyError:
            values.append('')

df["placekey"] = values

df.to_csv(FILE_OUT)