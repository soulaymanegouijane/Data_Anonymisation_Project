from os import name
import pandas as pd
import csv
from math import *
import json
import gc
import datetime


def curr_time():
    return datetime.datetime.now().strftime("%H:%M:%S")

def read(df_org, df_ano):
    dfAnon = pd.read_csv(df_ano, sep="\t", header=None).set_axis(['QId', 'Date', 'longitude', 'lattitude'], axis=1, inplace=False)
    print("Hello 1 : ", curr_time())
    dfAnon = dfAnon[dfAnon['QId']!='DEL']
    dfAnon['Date'] = pd.to_datetime(dfAnon['Date'], errors='coerce', dayfirst=True)
    print("Hello 2 : ", curr_time())
    dfAnon['week'] = dfAnon['Date'].dt.strftime('%Y-%W')  
    # Read from CSV's file  original
    dfOrig = pd.read_csv(df_org, sep="\t", header=None).set_axis(['Id', 'Date', 'longitude', 'lattitude'], axis=1, inplace=False)
    print("Hello 3 : ", curr_time())
    # Transform : Date to Year-week  Original
    dfOrig['Date'] = pd.to_datetime(dfOrig['Date'], errors='coerce', dayfirst=True)
    dfOrig['week'] = dfOrig['Date'].dt.strftime('%Y-%W')
    print("lecture terminée : ", curr_time())
    i=0
    for week in dfOrig['week'].unique():
        #  dfOrig[dfOrig['week']  == week].to_csv(f'bases_org/week_{week}.csv', header=False, index=False,sep='\t')
         dfAnon[dfAnon['week'] == week].to_csv(f'bases_ano/week_{week}.csv', header=False, index=False,sep='\t')
        #  print(f"week{i}")
         i+=1
        # print(dfOrig['week'].unique())
    print("Fichiers weeks: OK : ", curr_time())
    return dfOrig['week'].unique(),dfOrig['Id'].unique()

def counter(base_org, base_ano):
    gc.enable()
    weeks,ids = read(base_org,base_ano)
    Guesses = {}
    for id in ids:
        Guesses[str(id)]={}
    print("Fichier Json created: ", curr_time())
    for week in weeks:
        print(f"reidentification commencé pour {week}")
        dforig = pd.read_csv(f'bases_org/week_{week}.csv', sep='\t', names=["id", "date", "lat", "long","week"])
        dfano = pd.read_csv(f'bases_ano/week_{week}.csv', sep='\t', names=["qid", "date", "lat", "long","week"])
        deleted = len(dforig)-len(dfano)
        # cols = ['lat', 'long']
        # dforig[cols] = dforig[cols].round(2)
        # dfano[cols] = dfano[cols].round(2)
        dforig = dforig.groupby(['id'])[['lat','long']].aggregate(['count','mean']).reset_index()
        dfano = dfano.groupby(['qid'])[['lat','long']].aggregate(['count','mean']).reset_index()
        for id in ids:
            Guesses[str(id)][str(week)]=[]
        if deleted == 0:
            for index, row in dforig.iterrows():
                for index1, row1 in dfano.iterrows():
                     if dforig['lat']['count'][index] == dfano['lat']['count'][index1]:
                        Guesses[str(dforig['id'][index])][week].append(str(dfano['qid'][index1]))
        else:
            x=0
            for index, row in dforig.iterrows():
                data={}
                data[str(dforig['id'][index])]=[]
                I = str(dforig['id'][index])
                qids=[]
                for index1, row1 in dfano.iterrows():
                    if (dforig['lat']['count'][index]>= dfano['lat']['count'][index1] and dforig['lat']['count'][index]<= dfano['lat']['count'][index1]+deleted):
                        data[I].append(sqrt((dforig['lat']['mean'][index]-dfano['lat']['mean'][index1])**2 +(dforig['long']['mean'][index]-dfano['long']['mean'][index1])**2))
                        qids.append(str(dfano['qid'][index1]))
                df = pd.DataFrame.from_dict(data, orient='index',columns=qids)
                df = df.transpose()
                ordered = df.nsmallest(1,str(dforig['id'][index]))
                list1 = ordered.index.tolist()
                Guesses[str(dforig['id'][index])][week].extend(list1)


    with open('549moy.json','w') as f:
        json.dump(Guesses, f)
counter('Databases/Original','549')
