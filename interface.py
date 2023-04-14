import pandas as pd
from datetime import datetime

#Streaming Datasets
df_a=pd.read_csv('https://drive.google.com/uc?id=1WJfCKOAp2UhWfXEBSXHtiIe4ShBw48pB')
#df_d=pd.read_csv('https://drive.google.com/uc?id=187comTc0dz1aqGLSXQ5gLzY70RALoRpJ')
#df_h=pd.read_csv('https://drive.google.com/uc?id=1z6v7Sx4wBkjEBSKz5cp2lMID2EYYIQY4')
#df_n=pd.read_csv('https://drive.google.com/uc?id=1eArA5pc0zGgn1w2ujBiKkf1hSEf1_Fjk')

#Rating datasets
#df1=pd.read_csv('https://drive.google.com/uc?id=1RpQwvS0W8AqIsV8bP51iJzDwLSch_o1d')
#df2=pd.read_csv('https://drive.google.com/uc?id=1KELzh5ibLPv6J5OvFiCjCs3xBhtI2ILf')
#df3=pd.read_csv('https://drive.google.com/uc?id=1H-x_2SEIFHqtdrdev8jwBCzu28DKGNjO')
#df4=pd.read_csv('https://drive.google.com/uc?id=1Q9Yaf2O8pUn2_LQLSFYqPKn-iJBsJgog')
#df5=pd.read_csv('https://drive.google.com/uc?id=1-giwHEZExxJyaYIFls8POZBQ7VAa9luS')
#df6=pd.read_csv('https://drive.google.com/uc?id=1KdL6ZOalBpGcvqHf92l8omyGe_wib_Gx')
#df7=pd.read_csv('https://drive.google.com/uc?id=1xEAsohYePVW7oPm7mtQLXaPqW-PhBXZc')
#df8=pd.read_csv('https://drive.google.com/uc?id=1-kfhN2jPDGZVXOnswgUQa2031m3RUUiD')
#df_rate=[df1, df2, df3, df4, df5, df6, df7, df8]

def get_df():
    return df_a.to_json()
