import pandas as pd
from placekey.api import PlacekeyAPI
import enlighten
import math, pathlib, os, json
from concurrent.futures.thread import ThreadPoolExecutor
from concurrent.futures import as_completed
#PARAMS

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

#CONSTS

API  = PlacekeyAPI("gzOGnw0x8SsiJ4cM4TzI4F9yesp1Oul4")

PROGRESS_BARS = enlighten.get_manager()

#Functions & Classes

class TempWrapper():
    
    def __init__(self, id):
        self.path = f'{PATH_TEMP}/Thread - {id}.json'
        
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

def process(chunk, index):
    
    res = list()

    wrapper = TempWrapper(index)
    iterations = math.ceil(len(chunk) / BATCH_SIZE)

    progress_bar = PROGRESS_BARS.counter(total=iterations, desc=f"Processing > Worker {index}", unit="batches")
    
    for i in range(iterations):

        progress_bar.update()

        section = chunk[i * BATCH_SIZE:(i+1) * BATCH_SIZE]
        block = wrapper.get_block(section[0]['query_id'])

        lookup = list()

        if block is not None:
            lookup = block
            res.append(lookup)
            continue

        try:
            lookup = API.lookup_placekeys(section)

        except:
            for entry in section:
                lookup.append({'query_id' : entry['query_id'], 'error': 'Unknown error'})

        res.append(lookup)
        wrapper.new_block(lookup)
        wrapper.save()

    return res 

def get_chunks(input, sub_size):
    for i in range(0, len(input), sub_size):
        yield input[i:i + sub_size]

#Execution

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



chunks = list(get_chunks(data, CHUNK_SIZE))

futures = []

with ThreadPoolExecutor(max_workers=THREADS) as executor:

    for i, chunk in enumerate(chunks):
        futures.append(executor.submit(process, chunk, i))
        

res = list()
for future in as_completed(futures):
    for iteration in future.result():
        for elem in iteration:
            res.append(elem)

values = list()

for i, e in enumerate(res):
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

PROGRESS_BARS.stop()
