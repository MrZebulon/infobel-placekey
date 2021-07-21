import pandas as pd
from placekey.api import PlacekeyAPI
import enlighten
import math, pathlib, os, json, time
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
MAX_THREADS = 2

BATCHES = math.ceil(ROWS / BATCH_SIZE)

#CONSTS

API  = PlacekeyAPI("gzOGnw0x8SsiJ4cM4TzI4F9yesp1Oul4")

PROGRESS_BARS = enlighten.get_manager()

#Functions & Classes

class TempWrapper():
    
    def __init__(self):
        self.path = f'{PATH_TEMP}/computed.json'

        with open(self.path, 'w+') as f:

            if os.stat(self.path).st_size == 0:
                json.dump({}, f)
                self.blocks = dict()
            else:
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

def process(batch, wrapper, progression):
        
    block = wrapper.get_block(batch[0]['query_id'])

    if block is not None:
        return block

    lookup = list()

    try:
        lookup = API.lookup_placekeys(batch)

    except:
        for _ in len(batch):
            lookup.append({'query_id' : entry['query_id'], 'error': 'Unknown error'})
    
    progression.update()

    return lookup 

def get_batch(input, index):
    return input[index * BATCH_SIZE:(i+1) *BATCH_SIZE]

#Execution

df = pd.read_table(FILE_IN, sep=',', encoding="utf_8")
extraction_progress_bar = PROGRESS_BARS.counter(total = ROWS, desc="Extraction", unit="entries", color="red")

data = list()

for index, entry in df.iterrows():
    extraction_progress_bar.update()
    data.append(dict({
        "query_id": str(index),
        "street_address": str(entry[1 + COL_OFFSET]),
        "postal_code": str(entry[2 + COL_OFFSET]),
        "city": str(entry[3 + COL_OFFSET]),
        "region": str(entry[4 + COL_OFFSET]),
        "iso_country_code": ISO_CODE
    }))

futures = []

processing_progression_bar = PROGRESS_BARS.counter(total = ROWS, desc="Processing", unit="entries", color="white")

with ThreadPoolExecutor(max_workers=MAX_THREADS) as executor:

    wrapper = TempWrapper()
    for i in range(BATCHES):
        futures.append(executor.submit(process, get_batch(data, i), wrapper, processing_progression_bar))
        

res = list()
for future in as_completed(futures):
    for iteration in future.result():
        res.append(iteration)

values = list()
for i, e in enumerate(sorted(res, key=lambda x: x['query_id'])):
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
