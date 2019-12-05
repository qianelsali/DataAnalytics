import matplotlib.pyplot as plt 
import seaborn as sns


def boxplot(df, var):
  sns.set(style="whitegrid",palette="muted")
  f,ax=plt.subplots(1,2,figsize=(15,6))
  sns.boxplot(df[var"], orient="v",ax=ax[0])
  #df.groupby('IsRetained').var.plot(kind='kde',ax=ax[1])
  print("Max {}:{}".format(var, df[var].max()))
  print("Min {}:{}".format(var, df[var].min()))

