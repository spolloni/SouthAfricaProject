'''
distfuns.py

    created by: sp, oct 23 2017
    - spatial functions for distance calculations
'''

from pysqlite2 import dbapi2 as sql
import sys, csv, os, re, subprocess
from sklearn.neighbors import NearestNeighbors
import fiona, glob, multiprocessing
import geopandas as gpd
import numpy as np
import pandas as pd

def gp2shp(db,qrys,geocol,out,espg):

    con = sql.connect(db)
    con.enable_load_extension(True)
    con.execute("SELECT load_extension('mod_spatialite');")
    if len(qrys)>1:     
        cur = con.cursor()
        for qry in qrys[:-1]:
            cur.execute(qry)
    df = gpd.GeoDataFrame.from_postgis(qrys[-1],con,geom_col=geocol,
            crs=fiona.crs.from_epsg(espg))
    df.to_file(driver = 'ESRI Shapefile', filename = out)
    con.close()

    return


def selfintersect(db,dir,bw,rdp,algo,par1,par2):

    spar1 = re.sub("[^0-9]", "", str(par1))
    spar2 = re.sub("[^0-9]", "", str(par2))

    qry ='''
        SELECT ST_Union(ST_Buffer(B.GEOMETRY,{})), 
        C.cluster, A.prov_code
        FROM transactions AS A
        JOIN erven AS B ON A.property_id = B.property_id
        JOIN rdp_clusters_{}_{}_{}_{} AS C ON A.trans_id = C.trans_id
        WHERE C.cluster !=0
        GROUP BY cluster
        '''.format(bw,rdp,algo,spar1,spar2)
    out1 = dir+'buff.shp'
    out2 = dir+'interbuff.shp'

    # fetch dissolved buffers
    if os.path.exists(out1): os.remove(out1)
    cmd = ['ogr2ogr -f "ESRI Shapefile"', out1, db, '-sql "'+qry+'"']
    subprocess.call(' '.join(cmd),shell=True)

    # self-intersect
    cmd = ['saga_cmd shapes_polygons 12 -POLYGONS', out1,
           '-ID cluster -INTERSECT', out2]
    subprocess.call(' '.join(cmd),shell=True)

    return


def concavehull(db,dir,sig,rdp,algo,par1,par2):

    spar1 = re.sub("[^0-9]", "", str(par1))
    spar2 = re.sub("[^0-9]", "", str(par2))

    qry ='''
        SELECT ST_MakeValid(ST_Buffer(ST_ConcaveHull(ST_Collect(B.GEOMETRY),{}),20)), 
        C.cluster, A.prov_code
        FROM transactions AS A
        JOIN erven AS B ON A.property_id = B.property_id
        JOIN rdp_clusters_{}_{}_{}_{} AS C ON A.trans_id = C.trans_id
        WHERE C.cluster !=0
        GROUP BY cluster
        '''.format(sig,rdp,algo,spar1,spar2)
    out1 = dir+'hull.shp'
    out2 = dir+'edgehull.shp'
    out3 = dir+'splitedgehull.shp'
    out4 = dir+'coordshull.csv'
    grid = dir+'grid_7.shp'

    # fetch concave hulls
    if os.path.exists(out1): os.remove(out1)
    cmd = ['ogr2ogr -f "ESRI Shapefile"', out1, db, '-sql "'+qry+'"']
    subprocess.call(' '.join(cmd),shell=True)

    # convert hulls to lines (edges)
    cmd = ['saga_cmd shapes_lines 0 -POLYGONS', out1,'-LINES', out2]
    subprocess.call(' '.join(cmd),shell=True)

    # split edges into many vertices  
    cmd = ['saga_cmd shapes_lines 6 -LINES', out2, '-SPLIT', grid,
            '-INTERSECT', out3, '-OUTPUT', '1']
    subprocess.call(' '.join(cmd),shell=True)

    # export vertices to csv
    if os.path.exists(out4): os.remove(out4)
    cmd = ['ogr2ogr -f "CSV"', out4, out3, '-lco GEOMETRY=AS_WKT']
    subprocess.call(' '.join(cmd),shell=True)
    
    return


def merge_n_push(db,dir,bw,sig,rdp,algo,par1,par2):

    spar1 = re.sub("[^0-9]", "", str(par1))
    spar2 = re.sub("[^0-9]", "", str(par2))
    ssig  = re.sub("[^0-9]", "", str(sig))

    # push buffers to db
    con = sql.connect(db)
    cur = con.cursor()
    cur.execute('''CREATE TABLE IF NOT EXISTS 
            rdp_buffers_{}_{}_{}_{}_{} (mock INT);'''.format(rdp,algo,spar1,spar2,bw))
    con.commit()
    con.close()
    cmd = ['ogr2ogr -f "SQLite" -update','-a_srs http://spatialreference.org/ref/epsg/2046/',
            db, dir+'interbuff.shp','-select cluster,prov_code ', '-where "cluster > 0"','-nlt PROMOTE_TO_MULTI',
             '-nln rdp_buffers_{}_{}_{}_{}_{}'.format(rdp,algo,spar1,spar2,bw), '-overwrite']
    subprocess.call(' '.join(cmd),shell=True)

    # add hulls perimeter and area
    cmd = ['saga_cmd shapes_polygons 2 -POLYGONS', dir+'hull.shp',
           '-OUTPUT', dir+'hullwproperties.shp'] 
    subprocess.call(' '.join(cmd),shell=True)

    # push to db
    con = sql.connect(db)
    cur = con.cursor()
    cur.execute('''CREATE TABLE IF NOT EXISTS 
            rdp_hulls_{}_{}_{}_{}_{} (mock INT);'''.format(rdp,algo,spar1,spar2,ssig))
    con.commit()
    con.close()
    cmd = ['ogr2ogr -f "SQLite" -update','-a_srs http://spatialreference.org/ref/epsg/2046/',
            db, dir+'hullwproperties.shp','-select cluster,prov_code,PERIMETER,AREA','-nlt PROMOTE_TO_MULTI',
             '-nln rdp_hulls_{}_{}_{}_{}_{}'.format(rdp,algo,spar1,spar2,ssig), '-overwrite']
    subprocess.call(' '.join(cmd),shell=True)

    return


def fetch_data(db,dir,bw,sig,rdp,algo,par1,par2,i):

    spar1 = re.sub("[^0-9]", "", str(par1))
    spar2 = re.sub("[^0-9]", "", str(par2))
    ssig  = re.sub("[^0-9]", "", str(sig))

    if i==1:

        # BBLU pre points in buffers
        qry ='''
            SELECT st_x(p.GEOMETRY) AS x, st_y(p.GEOMETRY) AS y, p.OGC_FID
            FROM bblu_pre AS p, rdp_buffers_{}_{}_{}_{}_{} AS b
            WHERE p.ROWID IN (SELECT ROWID FROM SpatialIndex 
                    WHERE f_table_name='bblu_pre' AND search_frame=b.GEOMETRY)
            AND st_within(p.GEOMETRY,b.GEOMETRY) 
            '''.format(rdp,algo,spar1,spar2,bw)

    if i==2:

        # BBLU pre points in hulls
        qry ='''
            SELECT p.OGC_FID
            FROM bblu_pre AS p, rdp_hulls_{}_{}_{}_{}_{} AS h
            WHERE p.ROWID IN (SELECT ROWID FROM SpatialIndex 
                    WHERE f_table_name='bblu_pre' AND search_frame=h.GEOMETRY)
            AND st_within(p.GEOMETRY,h.GEOMETRY) 
            '''.format(rdp,algo,spar1,spar2,ssig)

    if i==3:

        # BBLU post points in buffers
        qry ='''
            SELECT st_x(p.GEOMETRY) AS x, st_y(p.GEOMETRY) AS y, p.OGC_FID
            FROM bblu_post AS p, rdp_buffers_{}_{}_{}_{}_{} AS b
            WHERE p.ROWID IN (SELECT ROWID FROM SpatialIndex 
                    WHERE f_table_name='bblu_post' AND search_frame=b.GEOMETRY)
            AND st_within(p.GEOMETRY,b.GEOMETRY) 
            '''.format(rdp,algo,spar1,spar2,bw)

    if i==4:

        # BBLU post points in hulls
        qry ='''
            SELECT p.OGC_FID
            FROM bblu_post AS p, rdp_hulls_{}_{}_{}_{}_{} AS h
            WHERE p.ROWID IN (SELECT ROWID FROM SpatialIndex 
                    WHERE f_table_name='bblu_post' AND search_frame=h.GEOMETRY)
            AND st_within(p.GEOMETRY,h.GEOMETRY) 
            '''.format(rdp,algo,spar1,spar2,ssig)

    if i==5:

        # all RDP transactions
        qry ='''
            SELECT st_x(e.GEOMETRY) as x, st_y(e.GEOMETRY) as y,
                   t.trans_id, c.cluster 
            FROM erven AS e
            JOIN transactions AS t ON e.property_id = t.property_id
            JOIN rdp_clusters_{}_{}_{}_{} AS c ON t.trans_id = c.trans_id
            WHERE c.cluster !=0
            '''.format(rdp,algo,spar1,spar2)
            
    if i==6:

        # RDP centroids, per cluster
        qry ='''
            SELECT st_x(st_centroid(st_collect(e.GEOMETRY))) as x,
                   st_y(st_centroid(st_collect(e.GEOMETRY))) as y , c.cluster 
            FROM erven AS e
            JOIN transactions AS t ON e.property_id = t.property_id
            JOIN rdp_clusters_{}_{}_{}_{} AS c ON t.trans_id = c.trans_id
            WHERE c.cluster !=0
            GROUP BY c.cluster
            '''.format(rdp,algo,spar1,spar2)

    if i==7:

        # all transactions inside hulls
        qry ='''
            SELECT t.trans_id, r.rdp_ls
            FROM erven AS e, rdp_hulls_{}_{}_{}_{}_{} AS h
            JOIN transactions AS t ON e.property_id = t.property_id
            JOIN rdp AS r ON t.trans_id = r.trans_id
            WHERE e.ROWID IN (SELECT ROWID FROM SpatialIndex 
                    WHERE f_table_name='erven' AND search_frame=h.GEOMETRY)
            AND st_within(e.GEOMETRY,h.GEOMETRY) 
            '''.format(rdp,algo,spar1,spar2,ssig)

    if i==8:

        # all transactions inside buffers
        qry ='''
            SELECT st_x(e.GEOMETRY) AS x, st_y(e.GEOMETRY) AS y,
                   t.trans_id, r.rdp_ls, b.cluster
            FROM erven AS e, rdp_buffers_{}_{}_{}_{}_{} AS b
            JOIN transactions AS t ON e.property_id = t.property_id
            JOIN rdp AS r ON t.trans_id = r.trans_id
            WHERE e.ROWID IN (SELECT ROWID FROM SpatialIndex 
                    WHERE f_table_name='erven' AND search_frame=b.GEOMETRY)
            AND st_within(e.GEOMETRY,b.GEOMETRY) 
            '''.format(rdp,algo,spar1,spar2,bw)

    # fetch data
    con = sql.connect(db)
    con.enable_load_extension(True)
    con.execute("SELECT load_extension('mod_spatialite');")
    cur = con.cursor()
    cur.execute(qry)
    mat = np.array(cur.fetchall())
    con.close()

    return mat


def comb_coordinates(dir):

    # load ogr2ogr exported csv
    df = pd.read_csv(dir+'coordshull.csv')

    # cluster column
    cluster = df['cluster']

    # separate coordinates into own columns
    wkt     = df['WKT'].str[12:-1]
    wkt = wkt.str.split(',', expand=True)

    # stack coordinates into one column
    stack_df = pd.DataFrame()
    for col in range(len(wkt.columns)):

        temp_df = pd.concat([wkt[[col]],cluster],axis=1)
        temp_df = temp_df[temp_df[col].notnull()]
        temp_df.columns = ['xy', 'cluster']

        stack_df = stack_df.append(temp_df)
    
    # separate x from y
    coords = stack_df['xy'].str.split(' ', expand=True)
    coords.columns = ['x','y']
    coords = pd.concat([coords,stack_df['cluster']],axis=1)

    return coords


def dist_calc(in_mat,targ_mat):

    nbrs = NearestNeighbors(n_neighbors=1, algorithm='auto').fit(targ_mat)
    dist, ind = nbrs.kneighbors(in_mat)

    return [dist,ind]


def push_distNRDP2db(db,matrx,distances,coords,rdp,algo,par1,par2,bw,sig):

    spar1 = re.sub("[^0-9]", "", str(par1))
    spar2 = re.sub("[^0-9]", "", str(par2))
    ssig  = re.sub("[^0-9]", "", str(sig))

    # Retrieve cluster IDS 
    trans_id = pd.DataFrame(matrx[0][matrx[0][:,3]=='0.0'][:,2],columns=['tr_id'])
    labels   = pd.DataFrame(matrx[1][matrx[1][:,1]=='0.0'][:,0],columns=['tr_id'])
    trans_id = pd.merge(trans_id,labels,how='left',on='tr_id',
                sort=False,indicator=True,validate='1:1').as_matrix()
    centroid_id = matrx[2][:,2][distances[0][1]].astype(np.float)
    nearest_id  = matrx[3][:,3][distances[1][1]].astype(np.float)
    conhulls_id = coords[:,2][distances[2][1]].astype(np.float)

    con = sql.connect(db)
    cur = con.cursor()
    
    cur.execute('''DROP TABLE IF EXISTS 
        distance_nrdp_{}_{}_{}_{}_{}_{};'''.format(rdp,algo,spar1,spar2,bw,ssig))

    cur.execute(''' CREATE TABLE distance_nrdp_{}_{}_{}_{}_{}_{} (
            trans_id         VARCHAR(11) PRIMARY KEY,
            centroid_dist    numeric(10,10), 
            centroid_cluster INTEGER,
            nearest_dist     numeric(10,10), 
            nearest_cluster  INTEGER,
            conhulls_dist    numeric(10,10), 
            conhulls_cluster INTEGER,
            conhulls_inhull  INTEGER
        );'''.format(rdp,algo,spar1,spar2,bw,ssig))

    rowsqry = '''
        INSERT INTO distance_nrdp_{}_{}_{}_{}_{}_{}
        VALUES (?,?,?,?,?,?,?,?);
        '''.format(rdp,algo,spar1,spar2,bw,ssig)

    for i in range(len(trans_id[:,0])):

        inhull = 0
        if trans_id[:,1][i] == 'both':
            distances[2][0][i][0] = -distances[2][0][i][0]
            inhull = 1

        cur.execute(rowsqry, [trans_id[:,0][i],distances[0][0][i][0],
           centroid_id[i][0],distances[1][0][i][0],nearest_id[i][0],
           distances[2][0][i][0],conhulls_id[i][0],inhull])

    cur.execute('''CREATE INDEX dist_nrdpind_{}_{}_{}_{}_{}_{}
        ON distance_nrdp_{}_{}_{}_{}_{}_{} (trans_id);'''.format(rdp,
            algo,spar1,spar2,bw,ssig,rdp,algo,spar1,spar2,bw,ssig))

    con.commit()
    con.close()

    return


def push_distBBLU2db(db,matrx,distances,coords,rdp,algo,par1,par2,bw,sig):

    spar1 = re.sub("[^0-9]", "", str(par1))
    spar2 = re.sub("[^0-9]", "", str(par2))
    ssig  = re.sub("[^0-9]", "", str(sig))

    for t in ['pre','post']:

        if t == 'pre':
            int1 = 1
            int2 = 2
        else:
            int1 = 0
            int2 = 0

        # Retrieve cluster IDS 
        bblu_id  = pd.DataFrame(matrx[int(5+int2)][:,2],columns=['ogc_fid'])
        bblu_lab = pd.DataFrame(matrx[int(4+int2)],columns=['ogc_fid']).drop_duplicates()
        bblu_id  = pd.merge(bblu_id,bblu_lab,how='left',on='ogc_fid',
                        sort=False,indicator=True,validate='1:1').as_matrix()
        conhulls_id = coords[:,2][distances[int1][1]].astype(np.float)

        con = sql.connect(db)
        cur = con.cursor()

        cur.execute('''DROP TABLE IF EXISTS 
            distance_bblu{}_{}_{}_{}_{}_{}_{};'''.format(t,rdp,algo,spar1,spar2,bw,ssig))

        cur.execute(''' CREATE TABLE distance_bblu{}_{}_{}_{}_{}_{}_{} (
                STR_FID   VARCHAR(11) ,
                OGC_FID   VARCHAR(11) PRIMARY KEY,
                distance  numeric(10,10), 
                cluster   INTEGER,
                inhull    INTEGER
            );'''.format(t,rdp,algo,spar1,spar2,bw,ssig))

        rowsqry = '''
            INSERT INTO distance_bblu{}_{}_{}_{}_{}_{}_{}
            VALUES (?,?,?,?,?);
            '''.format(t,rdp,algo,spar1,spar2,bw,ssig)

        for i in range(len(bblu_id[:,0])):

            inhull = 0
            if bblu_id[:,1][i] == 'both':
                distances[int1][0][i][0] = -distances[int1][0][i][0]
                inhull = 1
    
            cur.execute(rowsqry,[t+'_'+str(int(bblu_id[:,0][i])),int(bblu_id[:,0][i]),
                distances[int1][0][i][0],conhulls_id[i][0],inhull])

        cur.execute('''CREATE INDEX dist_{}ind_{}_{}_{}_{}_{}_{}
        ON distance_bblu{}_{}_{}_{}_{}_{}_{} (OGC_FID);'''.format(t,rdp,
            algo,spar1,spar2,bw,ssig,t,rdp,algo,spar1,spar2,bw,ssig))

        con.commit()
        con.close()

    return

   