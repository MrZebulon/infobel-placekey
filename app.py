import pandas as pd
from placekey.api import PlacekeyAPI
import enlighten
import math, threading, time, os, pathlib, json

GLOBAL_RES = list()

COL_OFFSET = 4

COUNTRY = "small-US"
ISO_CODE = "US"

PATH_IN= "./res"
PATH_OUT= "./out"
PATH_TEMP= "./temp"

FILE_IN = f'{PATH_IN}/{COUNTRY}.csv'
FILE_OUT = f'{PATH_OUT}/{COUNTRY}_out.csv'

ROWS = 1000
BATCH_SIZE = 250
THREADS = 2
CHUNK_SIZE = math.ceil(ROWS / THREADS)

PROGRESS_BARS = enlighten.get_manager()

API  = PlacekeyAPI("gzOGnw0x8SsiJ4cM4TzI4F9yesp1Oul4")

class TempWrapper():
    
    def __init__(self, file_name):
        self.path = f'{PATH_TEMP}/{file_name}.json'
        
        if not os.path.isfile(self.path):
            open(self.path, 'a').close()

        if os.stat(self.path).st_size == 0:
            with open(self.path, 'w') as f:
                json.dump({}, f)

        with open(self.path, 'r') as f:
            self.blocks = json.load(f)
    
    def get_block(self, id):
        try:
            return self.blocks[str(id)]
        except:
            return None

    def new_block(self, block):
        id = block[0]['query_id']
        self.blocks[id] = block

    def save(self):
        with open(self.path, 'w') as f:
            json.dump(self.blocks, f)

class ExecutionThread():

    def __init__(self, id, input):
        self.id = id
        self.input = input
        self.output = list()
        self.thread = threading.Thread(target=ExecutionThread.process, args=(self,), name=f'{id}')
        self.progress_bar = PROGRESS_BARS.counter(total=self.get_iterations(), desc=f"Processing > {self.get_name()}", unit="batches")
        self.finished = False
        self.temp = TempWrapper(id)

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

    def get_section(self, lower, higher):
        return self.input[lower:higher]

    def get_output(self):
        return self.output

    def get_temp(self):
        return self.temp

    def get_progress_bar(self):
        return self.progress_bar

    def is_finished(self):
        return self.finished
    
    def size(self):
        return len(self.input)

    def get_iterations(self):
        return math.ceil(self.size() / BATCH_SIZE)

    def append(self, array):
        self.output.append(array)

    @staticmethod
    def process(target):
        
        for i in range(target.get_iterations()):

            target.get_progress_bar().update()

            section = target.get_section(i * BATCH_SIZE, min((i+1) * BATCH_SIZE, target.size()))
            block = target.get_temp().get_block(section[0]['query_id'])

            if block is not None:
                target.append(block)
                continue
        
            try:
                lookup = API.lookup_placekeys(section)
                target.append(lookup)

                target.get_temp().new_block(lookup)
                target.get_temp().save()
            except:

                lst = list()

                for entry in section:
                    lst.append({'query_id' : entry['query_id'], 'error': 'Unknown error'})

                target.append(lst)
       
        target.on_stop()

        return

    @staticmethod
    def get_chunks(input, sub_size):
        for i in range(0, len(input), sub_size):
            yield input[i:i + sub_size]

    @staticmethod
    def finished(thread_lst):
        for thread in thread_lst:
            if not thread.is_finished():
                return False
        return True

# Execution

df = pd.read_table(FILE_IN, sep=',', encoding="utf_8")
extract_progress_bar = PROGRESS_BARS.counter(total = ROWS, desc="Extraction", unit="entries", color="red")

data = list()

for index, entry in df.iterrows():
    extract_progress_bar.update()
    data.append(dict({
        "query_id": str(index),
        "street_address": str(entry[1 + COL_OFFSET]),
        "postal_code": str(entry[2 + COL_OFFSET]),
        "city": str(entry[3 + COL_OFFSET]),
        "region": str(entry[4 + COL_OFFSET]),
        "iso_country_code": ISO_CODE
    }))

threads = list()
chunks = list(ExecutionThread.get_chunks(data, CHUNK_SIZE))

for i in range(THREADS):
    threads.append(ExecutionThread(f"Thread - {i}", chunks[i]))
    threads[i].start()

print('\r\n')

while not ExecutionThread.finished(threads):
    pass

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

[f.unlink() for f in pathlib.Path(PATH_TEMP).glob("*") if f.is_file()] 