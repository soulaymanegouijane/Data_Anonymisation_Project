import pandas as pd

df = pd.read_csv("./Databases/Original", sep="\t", names=['id', 'date', 'longitude', 'lattitude'])
print(df)