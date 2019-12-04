def checkMissingValues(df):
    summary = pd.DataFrame({'Valid(count)':df.count(),
                            'Missing(count)':df.isna().sum(),
                            'Missing(%)': (df.isna().sum()/len(df)).map(lambda x: '{:.00%}'.format(x)),
                            })
    print(summary)
