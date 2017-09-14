

import matplotlib as mpl
mpl.use('Agg')
import matplotlib.pyplot as plt
from pandas import Series
from pandas import read_csv
from pandas.plotting import autocorrelation_plot
from pandas.plotting import lag_plot
from matplotlib.backends.backend_pdf import PdfPages
from pandas import DataFrame

fig = plt.figure(figsize=(10,20))
#series = read_csv('extime.csv', header=0, parse_dates=[0], index_col=0, squeeze=True)
#series = DataFrame(series)
series = Series.from_csv( 'extime.csv' , header=0)

ax0 = plt.subplot2grid((4,1),(0,0))
ax0.set(title="", xlabel="submit time", ylabel="Execution time")
series.plot(style='k.',ax=ax0)

ax1 = plt.subplot2grid((4,1),(1,0))
ax1.set(title="", xlabel="Execution time", ylabel="Tong so job")
series.hist(ax=ax1,bins=100)

ax2 = plt.subplot2grid((4,1),(2,0))
series.plot(kind='kde',ax=ax2)

#ax3 = plt.subplot2grid((4,1),(3,0))
#autocorrelation_plot(series,ax=ax3)
plt.show()
#pp = PdfPages('out.pdf')
#pp.savefig()
fig.savefig('foo.png')
#pp.close()
