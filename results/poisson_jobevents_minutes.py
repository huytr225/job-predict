import matplotlib as mpl
mpl.use('Agg')
from statsmodels.tsa.ar_model import AR
from sklearn.metrics import mean_squared_error
import statsmodels.api as sm
import statsmodels.formula.api as smf
import matplotlib.pylab as plt
import numpy as np
import pandas as pd
from pandas import Series
from pandas import read_csv
from pandas import DataFrame
import sys

series = read_csv('jobevents-minutes-submit.csv', header=0, parse_dates=[0], index_col=0, squeeze=True)
print 'Series length:', len(series)
size_window = int(sys.argv[1]) #1000
slide_length = int(sys.argv[2]) #10
lenTest = len(series) - size_window
print 'Length Test:', lenTest
arlamda=[]

for i in range(0, lenTest, slide_length):
	print i
	test = series[i: i+size_window]
	df = DataFrame(test)
	model = smf.poisson("count ~ 1", data=df)
	result = model.fit()
	lmbda = np.exp(result.params)
	arlamda.append(lmbda)
print len(arlamda)
dflmbda = DataFrame(arlamda).to_csv('lambda-'+str(size_window)+'-'+str(slide_length)+'.csv', encoding='utf-8')
# train_size = int(len(arlamda) * 0.8)
# train, test = arlamda[1:train_size], arlamda[train_size:]
# model1 = AR(train)
# model_fit1 = model1.fit()
# print('Lag: %s' % model_fit1.k_ar)
# print('Coefficients: %s' % model_fit1.params)
# print len(model_fit1.params)
# predictions = model_fit1.predict(start=len(train), end=len(train)+len(test)-1, dynamic=False)
# error = mean_squared_error(test, predictions)
#
# print('Test MSE: %.3f' % error)
# plt.plot(train)
# plt.plot([None for i in train] + [x for x in test])
# plt.plot([None for i in train] + [x for x in predictions], color='red')
# plt.show()
