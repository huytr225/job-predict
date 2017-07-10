import matplotlib as mpl
mpl.use('Agg')
import matplotlib.pyplot as plt
from pandas import Series
from pandas.plotting import autocorrelation_plot
from pandas.plotting import lag_plot
from matplotlib.backends.backend_pdf import PdfPages

fig = plt.figure(figsize=(10,20))
series = Series.from_csv( 'jobevents-seconds.csv' , header=0)
ax0 = plt.subplot2grid((4,1),(0,0))
ax1 = plt.subplot2grid((4,1),(1,0))
ax2 = plt.subplot2grid((4,1),(2,0))
ax3 = plt.subplot2grid((4,1),(3,0))
ax0.set(title="Number of job in second", xlabel="Seconds", ylabel="Number of job")
series.plot(color = ['b'],ax=ax0)
series.hist(ax=ax1)
series.plot(kind='kde',ax=ax2)
#autocorrelation_plot(series,ax=ax3)
plt.show()
pp = PdfPages('jobevents-seconds.pdf')
pp.savefig()
pp.close()

