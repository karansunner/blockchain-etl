import os
import pandas as pd
import json
from datetime import datetime
from sqlalchemy import create_engine

###
# maak connectie met database
###
my_conn = create_engine("mysql+mysqldb://root:Ipbdam4.@localhost/ipbdam4")

###
# elke file in de transacties folder met .json extensie wordt omgezet naar .txt formaat en geplaatst in de folder: transacties-txt
###
directory = 'transacties'  # naam van folder met .json files
outputNr = 1
for file in os.listdir(directory):
    with open(f'transacties\{file}', 'r') as firstFile, open(f'transacties-txt\{outputNr}.txt', 'w+') as secondFile:
        for line in firstFile:
            secondFile.write(line)
    outputNr += 1

###
# per transactie voor alle txt bestanden worden de timestamp, address en value weggeschreven naar de database
###
directory = 'transacties-txt'
for file in os.listdir(directory):
    with open(f'transacties-txt/{file}') as f:
        for jsonObj in f:
            blockchainDict = json.loads(jsonObj)
            timestamp_unix = blockchainDict['timestamp']
            timestamp = datetime.fromtimestamp(timestamp_unix)
            for tr in blockchainDict['tx_outs']:
                address = tr['address']
                value = tr['value']
                temp_dict = {
                    'timestamp': [timestamp],
                    'address': [address],
                    'value': [value]
                }
                df = pd.DataFrame.from_dict(temp_dict)
                df.to_sql(con=my_conn, name='blockchain',
                          if_exists='append', index=False)
