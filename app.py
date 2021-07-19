import pandas as pd
from placekey.api import PlacekeyAPI
import math
import threading
import time
import enlighten

GLOBAL_RES = list()

COUNTRY = "small-US"
ISO_CODE = "us"

PATH_IN= "./res"
PATH_OUT= "./out"
FILE_IN = '{0}/{1}.csv'.format(PATH_IN, COUNTRY)
FILE_OUT = '{0}/{1}_out.csv'.format(PATH_OUT, COUNTRY)

ROWS = 1000
BATCH_SIZE = 10
THREADS = 10
CHUNK_SIZE = math.ceil(ROWS / THREADS)

PROGRESS_BARS = enlighten.get_manager()


API  = PlacekeyAPI("gzOGnw0x8SsiJ4cM4TzI4F9yesp1Oul4")

class ExecutionThread():

    def __init__(self, id, input):
        self.id = id
        self.input = input
        self.output = list()
        self.thread = threading.Thread(target=ExecutionThread.process, args=(self,), name=f'{id}')
        self.progress_bar = PROGRESS_BARS.counter(total=self.get_iterations(), desc=f"Processing > {self.get_name()}", unit="batches")
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

    def get_name(self):
        return self.thread.name

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

    def get_iterations(self):
        return math.ceil(self.size() / BATCH_SIZE)

    def append(self, array):
        self.output.append(array)

    def get_progress_bar(self):
        return self.progress_bar

    @classmethod
    def process(clz, target):
        
        for i in range(target.get_iterations()):
            section = target.get_input_subsection(i * BATCH_SIZE, min((i+1) * BATCH_SIZE, target.size()))
            try:
                target.append(API.lookup_placekeys(section))
            except:
                lst = list()

                for entry in section:
                    lst.append({'query_id' : entry['query_id'], 'error': 'Unknown error'})

                target.append(lst)
            target.get_progress_bar().update()
        
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
extract_progress_bar = PROGRESS_BARS.counter(total = ROWS, desc="Extraction", unit="entries", color="red")

data = list()

for index, entry in df.iterrows():
    extract_progress_bar.update()
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

print('\r\n')

while not ExecutionThread.finished(threads):
    time.sleep(1)

PROGRESS_BARS.stop()
print('\r\n')


values = list()
for i, e in enumerate(sorted(GLOBAL_RES, key=lambda x: x['query_id'])):
    try:
        values.append(e['placekey'])
    except KeyError:
        try:
            values.append(e['error'])
        except KeyError:
            values.append('')

df["placekey"] = values

df.to_csv(FILE_OUT)