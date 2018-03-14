'''
spaclust.py

    created by: sp, oct 15 2017
        
    - queries DB for rdp lat lon
    - classifies into cluster according to algo and pars.
'''

import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
import sklearn.cluster as cluster
import time, csv, hdbscan, sys, time, re
import pandas as pd
import scipy as scp
from pysqlite2 import dbapi2 as sql

def spatial_cluster(algo,par1,par2,database,suf):

    spar1 = re.sub("[^0-9]", "", str(par1))
    spar2 = re.sub("[^0-9]", "", str(par2))

    # connect to db
    con = sql.connect(database)
    cur = con.cursor()

    qry =   '''
            SELECT A.trans_id, A.purch_yr, C.latitude, C.longitude
            FROM transactions AS A
                JOIN rdp      AS B ON A.trans_id = B.trans_id
                JOIN erven    AS C ON A.property_id = C.property_id
            WHERE B.rdp_{}=1;
            '''.format(suf)

    # clear table and query
    cur.execute("DROP TABLE IF EXISTS rdp_clusters_{}_{}_{}_{};".format(suf,algo,spar1,spar2))
    cur.execute(qry)
    mat = np.array(cur.fetchall())
    print "    ... data has been queried! "

    # run spatial clustering algo
    if algo ==1:
        algoname = "DBSCAN"
        labels = cluster.DBSCAN(eps=par1,min_samples=par2).fit_predict(mat[:,2:])
    if algo ==2:
        algoname = "HDBSCAN"
        labels = hdbscan.HDBSCAN(min_cluster_size=par1,min_samples=par2).fit_predict(mat[:,2:])
    labels = labels +1 
    print "    ... data has been clustered! "

    # calculate mode-year and percentages
    df = pd.DataFrame(np.column_stack([mat[:,:2],labels]),columns=['id','yr','cl'])
    df['yr'] = df['yr'].astype('int64')
    df['cl'] = df['cl'].astype('int64')
    df['mxmodyr'] = df['yr'].groupby(df['cl']).transform(lambda x: pd.Series.mode(x)[0])
    df['mnmodyr'] = df['yr'].groupby(df['cl']).transform(lambda x: pd.Series.mode(x)[-1:])
    df['modyr']   = df[['mxmodyr','mnmodyr']].mean(axis=1)
    df['clsiz']   = df.groupby(df['cl'])['cl'].transform('count')
    df['close_1'] = np.where(abs(df['modyr']-df['yr'])<=.5, 1, 0)
    df['close_2'] = np.where(abs(df['modyr']-df['yr'])<=1 , 1, 0)
    df['clsum_1'] = df['close_1'].groupby(df['cl']).transform('sum')
    df['clsum_2'] = df['close_2'].groupby(df['cl']).transform('sum')
    df['frac_1']  = df['clsum_1']/df['clsiz']
    df['frac_2']  = df['clsum_2']/df['clsiz']

    # create table 
    cur.execute('''
        CREATE TABLE rdp_clusters_{}_{}_{}_{} (
            trans_id      VARCHAR(11) PRIMARY KEY,
            cluster       INTEGER,
            cluster_siz   INTEGER,
            mode_yr       INTEGER,
            frac1         REAL,
            frac2         REAL
        );'''.format(suf,algo,spar1,spar2))

    # fill-up table
    rowsqry = '''
        INSERT INTO rdp_clusters_{}_{}_{}_{}
        VALUES (?, ?, ?, ?, ?, ?);
        '''.format(suf,algo,spar1,spar2)
    for i in range(len(mat)):
        cur.execute(rowsqry,[df['id'][i],df['cl'][i],
            df['clsiz'][i],df['modyr'][i],df['frac_1'][i],df['frac_2'][i]])
    cur.execute('''CREATE INDEX clu_ind_{}
        ON rdp_clusters_{}_{}_{}_{} (cluster);'''.format(suf,suf,algo,spar1,spar2))
    cur.execute('''CREATE INDEX trans_ind_{}
        ON rdp_clusters_{}_{}_{}_{} (trans_id);'''.format(suf,suf,algo,spar1,spar2))
    print "    ... data has been pushed to DB! "

    # close-up
    con.commit()
    con.close()

    return

