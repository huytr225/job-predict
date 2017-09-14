import pandas as pd
import numpy as np
from pandas import Series
from pandas import read_csv
from pandas import DataFrame
import time
import sys
import os
start = time.time()

folder_path = '/mnt/volume/ggcluster/clusterdata-2011-2/task_usage/'

for file_name in os.listdir(folder_path):
    print file_name
    series = read_csv("%s%s"%(folder_path,file_name),
        parse_dates=[0],
        squeeze=True,
        names=['startTime', 'endTime', 'jobID',
        'taskIndex', 'machineID', 'meanCPUUsage',
        'CMU', 'assignMem', 'unmapped_cache_usage',
        'page_cache_usage', 'page_cache_usage', 'page_cache_usage',
        'mean_local_disk_space', 'max_cpu_usage', 'max_disk_io_time',
        'cpi', 'mai', 'sampling_portion',
        'agg_type', 'sampled_cpu_usage'])

    df = pd.DataFrame(data=np.array([[0,0,0]]), columns=['endTime', 'exeTime', 'jobID'])
    df = df.drop(df.index[0])
    df.set_index('jobID', inplace=True)
    for index, row in series.iterrows() :
        sys.stdout.write("\r%f%%" % (float(index)*100/len(series)))
        sys.stdout.flush()
        idx = row['jobID']
        s = long(row['startTime'])
        e = long(row['endTime'])
        if(idx in df.index):
            if  df.loc[idx]['endTime'] >= s :
                df.loc[idx] = [e, df.loc[idx]['exeTime'] + e - df.loc[idx]['endTime']]
            else :
                df.loc[idx] = [e, df.loc[idx]['exeTime'] + e-s]
        else :
            df.loc[idx] = [e, e-s]
    end = time.time()
    df.to_csv('results/tgchay/%s'%(file_name), encoding='utf-8')
print end-start

