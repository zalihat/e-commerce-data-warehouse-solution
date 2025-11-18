import pandas as pd
path = 'data/payments.csv'
df = pd.read_csv(path)
print(df.shape)