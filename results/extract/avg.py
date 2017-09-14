import pandas as pd
from pandas import Series
from pandas import read_csv
from pandas import DataFrame

series = read_csv('extime.csv', parse_dates=[0], squeeze=True)
# print type(series)
# print series.get_value(0,'timestamp')
# print len(series)
exavg = []
sum = 0
count = 1
timer = 1
size = 10*60
for index, row in series.iterrows() :
    ts = int(row['timestamp'])/1000000
    ex = int(row['exTime'])/1000000
    print ts
    print ex
    print timer*size
    if timer*size < ts:
        timer += 1
        exavg.append(sum/count)
        sum = 0
        count = 0
    sum += ex
    count += 1
print exavg
print len(exavg)
DataFrame(exavg).to_csv('exavg.csv', encoding='utf-8')

