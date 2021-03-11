import pandas as pd
import matplotlib.pyplot as plt

df = pd.read_csv('e00fce682d99af4881ea8981_BQ_export(1).csv',
                 index_col='dataTimeStamp')
df.index = pd.to_datetime(df.index)
df = df.sort_index()
df.drop(['deviceID'], axis=1)

plt.figure()
df.battery.plot()
plt.figure()
df.temperature.plot()
plt.ylim(10, 40)

plt.figure()
df.netRadiation.plot()
plt.figure()
df.flux.plot()
plt.ylim(-100, 100)
