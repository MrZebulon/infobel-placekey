from re import L
import pandas as pd
from placekey.api import PlacekeyAPI
import enlighten
import math
from concurrent.futures.thread import ThreadPoolExecutor

pd.options.mode.chained_assignment = None

#PARAMS

COL_OFFSET = 4

COUNTRY = "small-US"
ISO_CODE = "US"

PATH_IN= "./res"
PATH_OUT= "./out"
PATH_TEMP= "./temp"

FILE_IN = f'{PATH_IN}/{COUNTRY}.csv'
FILE_OUT = f'{PATH_OUT}/{COUNTRY}_' + '{0}' + '_out.csv'

ROWS = 800
ROWS_OFFSET = 100
BATCH_SIZE = 250
MAX_THREADS = 2

#CONSTS

API  = PlacekeyAPI("gzOGnw0x8SsiJ4cM4TzI4F9yesp1Oul4")

PROGRESS_BARS = enlighten.get_manager()

#Functions & Classes

def process(index, batch, data, progression):
    
    lookup = list()

    try:
        lookup = API.lookup_placekeys(batch)

    except:
        for entry in len(batch):
            lookup.append({'query_id' : entry['query_id'], 'placeholder': ''})
    
    for entry in lookup:
        try:
            data["placekey"] = entry['placekey']
        except:
            data["placekey"] = 'Error'

    data.to_csv(FILE_OUT.format(index), sep="|")
    progression.update()


def get_batch(input, index):
    return input[index * BATCH_SIZE: (index+1) *BATCH_SIZE]


def get_sub_dataframe(dataframe, lower, upper):
    return dataframe.iloc[lower:upper, : ]

#Execution

df = pd.read_table(FILE_IN, sep=',', encoding="utf_8")
extraction_progress_bar = PROGRESS_BARS.counter(total = ROWS, desc="Extraction", unit="entries", color="red")

data = list()

for index, entry in get_sub_dataframe(df, ROWS_OFFSET, ROWS_OFFSET + ROWS).iterrows():
    extraction_progress_bar.update()
    data.append(dict({
        "query_id": str(index),
        "street_address": str(entry[1 + COL_OFFSET]),
        "postal_code": str(entry[2 + COL_OFFSET]),
        "city": str(entry[3 + COL_OFFSET]),
        "region": str(entry[4 + COL_OFFSET]),
        "iso_country_code": ISO_CODE
    }))


batches = math.ceil(len(data) / BATCH_SIZE)

processing_progression_bar = PROGRESS_BARS.counter(total = batches, desc="Processing", unit="entries", color="white")

with ThreadPoolExecutor(max_workers=MAX_THREADS) as executor:

    for i in range(batches):
        executor.submit(process, i, get_batch(data, i), get_sub_dataframe(df, ROWS_OFFSET + i * BATCH_SIZE , ROWS_OFFSET + (i+1) *BATCH_SIZE), processing_progression_bar)
        

PROGRESS_BARS.stop()
