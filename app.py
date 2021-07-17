# To add a new cell, type '# %%'
# To add a new markdown cell, type '# %% [markdown]'
# %%
import pandas as pd
from placekey.api import PlacekeyAPI
import time
import math

# %% [Progress Bar]

def printProgressBar (iteration, total, prefix = '', suffix = '', decimals = 1, length = 100, fill = '█', printEnd = "\r"):
    
    percent = ("{0:." + str(decimals) + "f}").format(100 * (iteration / float(total)))
    filledLength = int(length * iteration // total)
    
    bar = fill * filledLength + '-' * (length - filledLength)
    print(f'\r{prefix} |{bar}| {percent}% {suffix}', end = printEnd)
    # Print New Line on Complete
    if iteration == total: 
        print()


# %% [Constants]
COUNTRY = "small-US"
ISO_CODE = "us"
SIZE = 1000
BATCH_SIZE = 10
ITERATIONS = math.ceil(SIZE / BATCH_SIZE)
PATH_IN= "./res"
PATH_OUT= "./out"

FILE_IN = '{0}/{1}.csv'.format(PATH_IN, COUNTRY)
FILE_OUT = '{0}/{1}_out.csv'.format(PATH_OUT, COUNTRY)


# %% [Extracting]
start_time = time.perf_counter()
print("Begining Extraction")

pk = PlacekeyAPI("gzOGnw0x8SsiJ4cM4TzI4F9yesp1Oul4")
df = pd.read_table(FILE_IN, sep=',', encoding="utf_8")
raw = [tuple(x) for x in df.values]

print(f"Extraction successfull - {int(time.perf_counter() - start_time)}\r\n")



# %% [Pre-Procesing]

start_time = time.perf_counter()
print("Begining Pre-Processing")

data = list()

printProgressBar(0, len(raw), prefix=f"Entry #0/{len(raw)}")
for i in range(len(raw)):
    printProgressBar(i+1, len(raw), prefix=f"Entry #{i+1}/{len(raw)}")

    entry = raw[i]

    data.append(dict({
        "query_id": str(entry[0]),
        "street_address": str(entry[1]),
        "postal_code": str(entry[2]),
        "city": str(entry[3]),
        "region": str(entry[4]),
        "iso_country_code": ISO_CODE
    }))

print(f"Pre-Processing successfull - {int(time.perf_counter() - start_time)}\r\n")

# %% [Processing]
start_time = time.perf_counter()
print("Begining Processing")

res = list()

printProgressBar(0, ITERATIONS, prefix=f"Batch #0/{ITERATIONS}", suffix=f"(batch size: {SIZE}")
for i in range(ITERATIONS):
    printProgressBar(i + 1, ITERATIONS, prefix=f"Batch #{i + 1}/{ITERATIONS}", suffix=f"Batch Size: {min((i + 1) * BATCH_SIZE, SIZE) - i * BATCH_SIZE}, Time Spent : {int(time.perf_counter() - start_time)} sec")
    res.append(pk.lookup_placekeys(data[i * BATCH_SIZE:min((i + 1) * BATCH_SIZE, SIZE)]))

print(f"Processing successfull - {int(time.perf_counter() - start_time)}\r\n")


# %% [Post-Processing]

start_time = time.perf_counter()
print("Begining Post-Processing")

values = dict()

printProgressBar(0, SIZE, prefix=f"Entry #0/{SIZE}")
for i in range(ITERATIONS):
    for j in range(len(res[i])):
        
        index = i * BATCH_SIZE + j
        printProgressBar(index, SIZE, prefix=f"Entry #{index}/{SIZE}")

        try:
            values[index] = res[i][j]["placekey"]
        except KeyError:
            try:
                values[index] = res[i][j]["error"]
            except KeyError:
                values[index] = ''

df["placekey"] = values.values()

df.to_csv(FILE_OUT)

print(f"Post-Processing successfull - {int(time.perf_counter() - start_time)}\r\n")
