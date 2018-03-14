from pysqlite2 import dbapi2 as sql

def dissolve_BBLU(db,yr,geo):

    if yr=='pre' and geo=='sp':
        ggeo  = 'sp_code'
        yyr   = '2001'
    if yr=='post' and geo=='sp':
        ggeo = 'SP_CODE'
        yyr  = '2011'

    drop_qry = '''
               DROP TABLE IF EXISTS bblu_{}_{};
               '''.format(yr,geo)

    create_qry = ''' CREATE TABLE bblu_{}_{} AS

                 SELECT B.{} as {}, 

                 /* Formal Housing Count*/
                 cast(1.00*sum(CASE 
                    WHEN A.s_lu_code="7.1"
                        THEN 1
                        ELSE 0
                    END) AS FLOAT) as formal_count,

                /* Informal Housing Count*/
                 cast(1.00*sum(CASE 
                    WHEN A.s_lu_code="7.2"
                        THEN 1
                        ELSE 0
                    END) AS FLOAT) as informal_count,

                /* Relative Informal Housing */
                 cast(1.00*sum(CASE 
                    WHEN A.s_lu_code="7.2"
                        THEN 1
                        ELSE 0
                    END)/sum(CASE 
                    WHEN A.s_lu_code IN ("7.2","7.1")
                        THEN 1 
                        ELSE 0 
                    END) AS FLOAT) as informal_percent

                 FROM bblu_{} AS A, {}_{} AS B
                 
                 WHERE A.ROWID IN (SELECT ROWID FROM SpatialIndex 
                         WHERE f_table_name='bblu_{}' AND search_frame=B.GEOMETRY)
                 
                 AND st_within(A.GEOMETRY,B.GEOMETRY) 
                 GROUP BY {}
                 '''.format(yr,geo,ggeo,ggeo,yr,geo,yyr,yr,ggeo)

    con = sql.connect(db)
    con.enable_load_extension(True)
    con.execute("SELECT load_extension('mod_spatialite');")
    cur = con.cursor()

    cur.execute(drop_qry)
    cur.execute(create_qry)

    con.commit()
    con.close()

def dissolve_census(db,yr,geo):

    if yr=='2001' and geo=='sp':

        drop_qry = '''
                   DROP TABLE IF EXISTS census_2001_sp;
                   '''

        create_qry = ''' CREATE TABLE census_2001_sp AS

            SELECT A.* , B.* 

            FROM ( SELECT Subplace_Code,
    
                /* HOUSEHOLDS IN FORMAL HOUSE OR FLAT */
                cast(1.00*sum(CASE 
                    WHEN H23a_HU IN (1,3,4)
                        THEN 1 
                        ELSE 0 
                    END) as FLOAT) as formal_count,
        
                /* PERCENT HOUSEHOLDS IN FORMAL HOUSE OR FLAT */
                cast(1.00*sum(CASE 
                    WHEN H23a_HU IN (1,3,4)
                        THEN 1 
                        ELSE 0 
                    END)/sum(CASE 
                    WHEN H23a_HU IS NOT NULL
                        THEN 1 
                        ELSE 0 
                    END) as FLOAT) as formal_percent,
        
                /* HOUSEHOLDS LIVING IN A BACKYARD */
                cast(1.00*sum(CASE 
                    WHEN H23a_HU IN (5,6)
                        THEN 1 
                        ELSE 0 
                    END) as FLOAT) as backyard_count,
        
                /* PERCENT HOUSEHOLDS LIVING IN A BACKYARD */
                cast(1.00*sum(CASE 
                    WHEN H23a_HU IN (5,6)
                        THEN 1 
                        ELSE 0 
                    END)/sum(CASE 
                    WHEN H23a_HU IS NOT NULL
                        THEN 1 
                        ELSE 0 
                    END) as FLOAT) as backyard_percent,
        
                /* HOUSEHOLDS IN INFORMAL HOUSING NOT IN A BACKYARD */
                cast(1.00*sum(CASE 
                    WHEN H23a_HU=7
                        THEN 1 
                        ELSE 0 
                    END) as FLOAT) as informalnotbck_count,
        
                /* PERCENT HOUSEHOLDS IN INFORMAL HOUSING NOT IN A BACKYARD */
                cast(1.00*sum(CASE 
                    WHEN H23a_HU=7
                        THEN 1 
                        ELSE 0 
                    END)/sum(CASE 
                    WHEN H23a_HU IS NOT NULL
                        THEN 1 
                        ELSE 0 
                    END) as FLOAT) as informalnotbck_percent,
        
                /* HOUSEHOLDS IN INFORMAL HOUSING */
                cast(1.00*sum(CASE 
                    WHEN H23a_HU IN (6,7)
                        THEN 1 
                        ELSE 0 
                    END) as FLOAT) as informal_count,
        
                /* PERCENT HOUSEHOLDS IN INFORMAL HOUSING */
                cast(1.00*sum(CASE 
                    WHEN H23a_HU IN (6,7)
                        THEN 1 
                        ELSE 0 
                    END)/sum(CASE 
                    WHEN H23a_HU IS NOT NULL
                        THEN 1 
                        ELSE 0 
                    END) as FLOAT) as informal_percent,
        
                /* HOUSEHOLDS IN INFORMAL HOUSING (ALL BACKYARD) */
                cast(1.00*sum(CASE 
                    WHEN H23a_HU IN (5,6,7)
                        THEN 1 
                        ELSE 0 
                    END) as FLOAT) as informalwbck_count,
        
                /* PERCENT HOUSEHOLDS IN INFORMAL HOUSING */
                cast(1.00*sum(CASE 
                    WHEN H23a_HU IN (5,6,7)
                        THEN 1 
                        ELSE 0 
                    END)/sum(CASE 
                    WHEN H23a_HU IS NOT NULL
                        THEN 1 
                        ELSE 0 
                    END) as FLOAT) as informalwbck_percent
    
            FROM census_hh_2001
            GROUP BY Subplace_Code) AS A

            JOIN ( SELECT Subplace_Code,

                /* COUNT BLACK AFRICANS */
                cast(1.00*sum(CASE 
                    WHEN P06_Race=1
                        THEN 1 
                        ELSE 0 
                    END) as FLOAT) as black_count,
        
                /* PERCENT BLACK AFRICANS */
                cast(1.00*sum(CASE 
                    WHEN P06_Race=1
                        THEN 1 
                        ELSE 0 
                    END)/sum(CASE 
                    WHEN P06_Race IS NOT NULL
                        THEN 1 
                        ELSE 0 
                    END) as FLOAT) as black_percent,
        
                /* COUNT COLOURED */
                cast(1.00*sum(CASE 
                    WHEN P06_Race=2
                        THEN 1 
                        ELSE 0 
                    END) as FLOAT) as coloured_count,
        
                /* PERCENT COLOURED */
                cast(1.00*sum(CASE 
                    WHEN P06_Race=2
                        THEN 1 
                        ELSE 0 
                    END)/sum(CASE 
                    WHEN P06_Race IS NOT NULL
                        THEN 1 
                        ELSE 0 
                    END) as FLOAT) as coloured_percent,
        
                /* COUNT INDIAN/ASIAN */
                cast(1.00*sum(CASE 
                    WHEN P06_Race=3
                        THEN 1 
                        ELSE 0 
                    END) as FLOAT) as indas_count,
        
                /* PERCENT INDIAN/ASIAN */
                cast(1.00*sum(CASE 
                    WHEN P06_Race=3
                        THEN 1 
                        ELSE 0 
                    END)/sum(CASE 
                    WHEN P06_Race IS NOT NULL
                        THEN 1 
                        ELSE 0 
                    END) as FLOAT) as indas_percent,
        
                /* COUNT WHITE */
                cast(1.00*sum(CASE 
                    WHEN P06_Race=4
                        THEN 1 
                        ELSE 0 
                    END) as FLOAT) as white_count,
        
                /* PERCENT WHITE */
                cast(1.00*sum(CASE 
                    WHEN P06_Race=4
                        THEN 1 
                        ELSE 0 
                    END)/sum(CASE 
                    WHEN P06_Race IS NOT NULL
                        THEN 1 
                        ELSE 0 
                    END) as FLOAT) as white_percent,
        
                /* COUNT NO SCHOOLING */
                cast(1.00*sum(CASE 
                    WHEN P17_Educ=99
                        THEN 1 
                        ELSE 0 
                    END) as FLOAT) as noschool_count,
        
                /* PERCENT NO SCHOOLING */
                cast(1.00*sum(CASE 
                    WHEN P17_Educ=99
                        THEN 1 
                        ELSE 0 
                    END)/sum(CASE 
                    WHEN P17_Educ IS NOT NULL
                        THEN 1 
                        ELSE 0 
                    END) as FLOAT) as noschool_percent,
        
                /* COUNT SOME PRIMARY */
                cast(1.00*sum(CASE 
                    WHEN P17_Educ >=1 AND P17_Educ<=6
                        THEN 1 
                        ELSE 0 
                    END) as FLOAT) as somprim_count,
        
                /* PERCENT SOME PRIMARY */
                cast(1.00*sum(CASE 
                    WHEN P17_Educ >=1 AND P17_Educ<=6
                        THEN 1 
                        ELSE 0 
                    END)/sum(CASE 
                    WHEN P17_Educ IS NOT NULL
                        THEN 1 
                        ELSE 0 
                    END) as FLOAT) as somprim_percent,
        
                /* COUNT PRIMARY */
                cast(1.00*sum(CASE 
                    WHEN P17_Educ=7
                        THEN 1 
                        ELSE 0 
                    END) as FLOAT) as primary_count,
        
                /* PERCENT PRIMARY */
                cast(1.00*sum(CASE 
                    WHEN P17_Educ=7
                        THEN 1 
                        ELSE 0 
                    END)/sum(CASE 
                    WHEN P17_Educ IS NOT NULL
                        THEN 1 
                        ELSE 0 
                    END) as FLOAT) as primary_percent,
        
                /* COUNT SOME SECONDARY */
                cast(1.00*sum(CASE 
                    WHEN P17_Educ>=8 AND P17_Educ<=9
                        THEN 1 
                        ELSE 0 
                    END) as FLOAT) as somsec_count,
        
                /* PERCENT SOME SECONDARY */
                cast(1.00*sum(CASE 
                    WHEN P17_Educ>=8 AND P17_Educ<=9
                        THEN 1 
                        ELSE 0 
                    END)/sum(CASE 
                    WHEN P17_Educ IS NOT NULL
                        THEN 1 
                        ELSE 0 
                    END) as FLOAT) as somsec_percent,
        
                /* COUNT SECONDARY */
                cast(1.00*sum(CASE 
                    WHEN P17_Educ=10
                        THEN 1 
                        ELSE 0 
                    END) as FLOAT) as secondary_count,
        
                /* PERCENT SECONDARY */
                cast(1.00*sum(CASE 
                    WHEN P17_Educ=10
                        THEN 1 
                        ELSE 0 
                    END)/sum(CASE 
                    WHEN P17_Educ IS NOT NULL
                        THEN 1 
                        ELSE 0 
                    END) as FLOAT) as secondary_percent,
        
                /* COUNT HIGHER */
                cast(1.00*sum(CASE 
                    WHEN P17_Educ>=11 AND P17_Educ<=20
                        THEN 1 
                        ELSE 0 
                    END) as FLOAT) as higher_count,
        
                /* PERCENT HIGHER */
                cast(1.00*sum(CASE 
                    WHEN P17_Educ>=11 AND P17_Educ<=20
                        THEN 1 
                        ELSE 0 
                    END)/sum(CASE 
                    WHEN P17_Educ IS NOT NULL
                        THEN 1 
                        ELSE 0 
                    END) as FLOAT) as higher_percent,
        
                /* AVERAGE INCOME */
                cast(1.00*avg(CASE 
                    WHEN P22_Incm = 1  THEN 0
                    WHEN P22_Incm = 2  THEN 2400
                    WHEN P22_Incm = 3  THEN 7200
                    WHEN P22_Incm = 4  THEN 14400
                    WHEN P22_Incm = 5  THEN 28800
                    WHEN P22_Incm = 6  THEN 57600
                    WHEN P22_Incm = 7  THEN 115200
                    WHEN P22_Incm = 8  THEN 230400
                    WHEN P22_Incm = 9  THEN 460800
                    WHEN P22_Incm = 10 THEN 921600
                    WHEN P22_Incm = 11 THEN 1843200
                    WHEN P22_Incm = 12 THEN 3000000
                    ELSE NULL
                    END) as FLOAT) as avg_income

            FROM census_pers_2001
            GROUP BY Subplace_Code ) AS B

            ON A.Subplace_Code=B.Subplace_Code;
            '''
        index_qry = '''
                    CREATE INDEX census_2001_sp_ind ON census_2001_sp (Subplace_Code);
                    '''

    if yr=='2011' and geo=='sp':

        drop_qry = '''
                   DROP TABLE IF EXISTS census_2011_sp;
                   '''

        create_qry = ''' CREATE TABLE census_2011_sp AS

            SELECT A.*, B.*

            FROM (SELECT SP_CODE,
    
                /* HOUSEHOLDS IN FORMAL HOUSE OR FLAT */
                cast(1.00*sum(CASE 
                    WHEN H02_MAINDWELLING IN (1,3,4,5,6)
                        THEN 1 
                        ELSE 0 
                    END) as FLOAT) as formal_count,
        
                /* PERCENT HOUSEHOLDS IN FORMAL HOUSE OR FLAT */
                cast(1.00*sum(CASE 
                    WHEN H02_MAINDWELLING IN (1,3,4,5,6)
                        THEN 1 
                        ELSE 0 
                    END)/sum(CASE 
                    WHEN H02_MAINDWELLING IS NOT NULL
                        THEN 1 
                        ELSE 0 
                    END) as FLOAT) as formal_percent,
        
                /* HOUSEHOLDS LIVING IN A BACKYARD */
                cast(1.00*sum(CASE 
                    WHEN H02_MAINDWELLING IN (7,8)
                        THEN 1 
                        ELSE 0 
                    END) as FLOAT) as backyard_count,
        
                /* PERCENT HOUSEHOLDS LIVING IN A BACKYARD */
                cast(1.00*sum(CASE 
                    WHEN H02_MAINDWELLING IN (7,8)
                        THEN 1 
                        ELSE 0 
                    END)/sum(CASE 
                    WHEN H02_MAINDWELLING IS NOT NULL
                        THEN 1 
                        ELSE 0 
                    END) as FLOAT) as backyard_percent,
        
                /* HOUSEHOLDS IN INFORMAL HOUSING NOT IN A BACKYARD */
                cast(1.00*sum(CASE 
                    WHEN H02_MAINDWELLING=9
                        THEN 1 
                        ELSE 0 
                    END) as FLOAT) as informalnotbck_count,
        
                /* PERCENT HOUSEHOLDS IN INFORMAL HOUSING NOT IN A BACKYARD */
                cast(1.00*sum(CASE 
                    WHEN H02_MAINDWELLING=9
                        THEN 1 
                        ELSE 0 
                    END)/sum(CASE 
                    WHEN H02_MAINDWELLING IS NOT NULL
                        THEN 1 
                        ELSE 0 
                    END) as FLOAT) as informalnotbck_percent,
        
                /* HOUSEHOLDS IN INFORMAL HOUSING */
                cast(1.00*sum(CASE 
                    WHEN H02_MAINDWELLING IN (8,9)
                        THEN 1 
                        ELSE 0 
                    END) as FLOAT) as informal_count,
        
                /* PERCENT HOUSEHOLDS IN INFORMAL HOUSING */
                cast(1.00*sum(CASE 
                    WHEN H02_MAINDWELLING IN (8,9)
                        THEN 1 
                        ELSE 0 
                    END)/sum(CASE 
                    WHEN H02_MAINDWELLING IS NOT NULL
                        THEN 1 
                        ELSE 0 
                    END) as FLOAT) as informal_percent,
        
                /* HOUSEHOLDS IN INFORMAL HOUSING (ALL BACKYARD) */
                cast(1.00*sum(CASE 
                    WHEN H02_MAINDWELLING IN (7,8,9)
                        THEN 1 
                        ELSE 0 
                    END) as FLOAT) as informalwbck_count,
        
                /* PERCENT HOUSEHOLDS IN INFORMAL HOUSING */
                cast(1.00*sum(CASE 
                    WHEN H02_MAINDWELLING IN (7,8,9)
                        THEN 1 
                        ELSE 0 
                    END)/sum(CASE 
                    WHEN H02_MAINDWELLING IS NOT NULL
                        THEN 1 
                        ELSE 0 
                    END) as FLOAT) as informalwbck_percent
    
            FROM census_hh_2011
            GROUP BY SP_CODE) AS A

            JOIN (SELECT SP_CODE,

                /* COUNT BLACK AFRICANS */
                cast(1.00*sum(CASE 
                    WHEN P05_POP_GROUP=1
                        THEN 1 
                        ELSE 0 
                    END) as FLOAT) as black_count,
        
                /* PERCENT BLACK AFRICANS */
                cast(1.00*sum(CASE 
                    WHEN P05_POP_GROUP=1
                        THEN 1 
                        ELSE 0 
                    END)/sum(CASE 
                    WHEN P05_POP_GROUP IS NOT NULL
                        THEN 1 
                        ELSE 0 
                    END) as FLOAT) as black_percent,
        
                /* COUNT COLOURED */
                cast(1.00*sum(CASE 
                    WHEN P05_POP_GROUP=2
                        THEN 1 
                        ELSE 0 
                    END) as FLOAT) as coloured_count,
        
                /* PERCENT COLOURED */
                cast(1.00*sum(CASE 
                    WHEN P05_POP_GROUP=2
                        THEN 1 
                        ELSE 0 
                    END)/sum(CASE 
                    WHEN P05_POP_GROUP IS NOT NULL
                        THEN 1 
                        ELSE 0 
                    END) as FLOAT) as coloured_percent,
        
                /* COUNT INDIAN/ASIAN */
                cast(1.00*sum(CASE 
                    WHEN P05_POP_GROUP=3
                        THEN 1 
                        ELSE 0 
                    END) as FLOAT) as indas_count,
        
                /* PERCENT INDIAN/ASIAN */
                cast(1.00*sum(CASE 
                    WHEN P05_POP_GROUP=3
                        THEN 1 
                        ELSE 0 
                    END)/sum(CASE 
                    WHEN P05_POP_GROUP IS NOT NULL
                        THEN 1 
                        ELSE 0 
                    END) as FLOAT) as indas_percent,
        
                /* COUNT WHITE */
                cast(1.00*sum(CASE 
                    WHEN P05_POP_GROUP=4
                        THEN 1 
                        ELSE 0 
                    END) as FLOAT) as white_count,
        
                /* PERCENT WHITE */
                cast(1.00*sum(CASE 
                    WHEN P05_POP_GROUP=4
                        THEN 1 
                        ELSE 0 
                    END)/sum(CASE 
                    WHEN P05_POP_GROUP IS NOT NULL
                        THEN 1 
                        ELSE 0 
                    END) as FLOAT) as white_percent,
        
                /* COUNT NO SCHOOLING */
                cast(1.00*sum(CASE 
                    WHEN P20_EDULEVEL=98
                        THEN 1 
                        ELSE 0 
                    END) as FLOAT) as noschool_count,
        
                /* PERCENT NO SCHOOLING */
                cast(1.00*sum(CASE 
                    WHEN P20_EDULEVEL=98
                        THEN 1 
                        ELSE 0 
                    END)/sum(CASE 
                    WHEN P20_EDULEVEL IS NOT NULL
                        THEN 1 
                        ELSE 0 
                    END) as FLOAT) as noschool_percent,
        
                /* COUNT SOME PRIMARY */
                cast(1.00*sum(CASE 
                    WHEN P20_EDULEVEL>=0 AND P20_EDULEVEL<=6
                        THEN 1 
                        ELSE 0 
                    END) as FLOAT) as somprim_count,
        
                /* PERCENT SOME PRIMARY */
                cast(1.00*sum(CASE 
                    WHEN P20_EDULEVEL>=0 AND P20_EDULEVEL<=6
                        THEN 1 
                        ELSE 0 
                    END)/sum(CASE 
                    WHEN P20_EDULEVEL IS NOT NULL
                        THEN 1 
                        ELSE 0 
                    END) as FLOAT) as somprim_percent,
        
                /* COUNT PRIMARY */
                cast(1.00*sum(CASE 
                    WHEN P20_EDULEVEL=7
                        THEN 1 
                        ELSE 0 
                    END) as FLOAT) as primary_count,
        
                /* PERCENT PRIMARY */
                cast(1.00*sum(CASE 
                    WHEN P20_EDULEVEL=7
                        THEN 1 
                        ELSE 0 
                    END)/sum(CASE 
                    WHEN P20_EDULEVEL IS NOT NULL
                        THEN 1 
                        ELSE 0 
                    END) as FLOAT) as primary_percent,
        
                /* COUNT SOME SECONDARY */
                cast(1.00*sum(CASE 
                    WHEN P20_EDULEVEL>=8 AND P20_EDULEVEL<=11
                        THEN 1 
                        ELSE 0 
                    END) as FLOAT) as somsec_count,
        
                /* PERCENT SOME SECONDARY */
                cast(1.00*sum(CASE 
                    WHEN P20_EDULEVEL>=8 AND P20_EDULEVEL<=11
                        THEN 1 
                        ELSE 0 
                    END)/sum(CASE 
                    WHEN P20_EDULEVEL IS NOT NULL
                        THEN 1 
                        ELSE 0 
                    END) as FLOAT) as somsec_percent,
        
                /* COUNT SECONDARY */
                cast(1.00*sum(CASE 
                    WHEN P20_EDULEVEL=12
                        THEN 1 
                        ELSE 0 
                    END) as FLOAT) as secondary_count,
        
                /* PERCENT SECONDARY */
                cast(1.00*sum(CASE 
                    WHEN P20_EDULEVEL=12
                        THEN 1 
                        ELSE 0 
                    END)/sum(CASE 
                    WHEN P20_EDULEVEL IS NOT NULL
                        THEN 1 
                        ELSE 0 
                    END) as FLOAT) as secondary_percent,
        
                /* COUNT HIGHER */
                cast(1.00*sum(CASE 
                    WHEN P20_EDULEVEL>=14 AND P20_EDULEVEL<=28
                        THEN 1 
                        ELSE 0 
                    END) as FLOAT) as higher_count,
        
                /* PERCENT HIGHER */
                cast(1.00*sum(CASE 
                    WHEN P20_EDULEVEL>=14 AND P20_EDULEVEL<=28
                        THEN 1 
                        ELSE 0 
                    END)/sum(CASE 
                    WHEN P20_EDULEVEL IS NOT NULL
                        THEN 1 
                        ELSE 0 
                    END) as FLOAT) as higher_percent,
        
                /* AVERAGE INCOME */
                cast(1.00*avg(CASE 
                    WHEN P16_INCOME = 1  THEN 0
                    WHEN P16_INCOME = 2  THEN 2400
                    WHEN P16_INCOME = 3  THEN 7200
                    WHEN P16_INCOME = 4  THEN 14400
                    WHEN P16_INCOME = 5  THEN 28800
                    WHEN P16_INCOME = 6  THEN 57600
                    WHEN P16_INCOME = 7  THEN 115200
                    WHEN P16_INCOME = 8  THEN 230400
                    WHEN P16_INCOME = 9  THEN 460800
                    WHEN P16_INCOME = 10 THEN 921600
                    WHEN P16_INCOME = 11 THEN 1843200
                    WHEN P16_INCOME = 12 THEN 3000000
                    ELSE NULL
                    END) as FLOAT) as avg_income

            FROM census_pers_2011
            GROUP BY SP_CODE) AS B

            ON A.SP_CODE=B.SP_CODE;
            '''

        index_qry = '''
                    CREATE INDEX census_2011_sp_ind ON census_2011_sp (SP_CODE);
                    '''

    if yr=='1996' and geo=='ea':

        drop_qry = '''
                   DROP TABLE IF EXISTS census_1996_ea;
                   '''

        create_qry = ''' CREATE TABLE census_1996_ea AS

            SELECT A.*, B.*

            FROM (SELECT EACODE,
    
                /* HOUSEHOLDS IN FORMAL HOUSE OR FLAT */
                cast(1.00*sum(CASE 
                    WHEN DWELLING IN (1,3,4,5)
                        THEN 1 
                        ELSE 0 
                    END) as FLOAT) as formal_count,
        
                /* PERCENT HOUSEHOLDS IN FORMAL HOUSE OR FLAT */
                cast(1.00*sum(CASE 
                    WHEN DWELLING IN (1,3,4,5)
                        THEN 1 
                        ELSE 0 
                    END)/sum(CASE 
                    WHEN DWELLING IS NOT NULL
                        THEN 1 
                        ELSE 0 
                    END) as FLOAT) as formal_percent,
        
                /* HOUSEHOLDS LIVING IN A BACKYARD */
                cast(1.00*sum(CASE 
                    WHEN DWELLING IN (6,7)
                        THEN 1 
                        ELSE 0 
                    END) as FLOAT) as backyard_count,
        
                /* PERCENT HOUSEHOLDS LIVING IN A BACKYARD */
                cast(1.00*sum(CASE 
                    WHEN DWELLING IN (6,7)
                        THEN 1 
                        ELSE 0 
                    END)/sum(CASE 
                    WHEN DWELLING IS NOT NULL
                        THEN 1 
                        ELSE 0 
                    END) as FLOAT) as backyard_percent,
        
                /* HOUSEHOLDS IN INFORMAL HOUSING NOT IN A BACKYARD */
                cast(1.00*sum(CASE 
                    WHEN DWELLING=8
                        THEN 1 
                        ELSE 0 
                    END) as FLOAT) as informalnotbck_count,
        
                /* PERCENT HOUSEHOLDS IN INFORMAL HOUSING NOT IN A BACKYARD */
                cast(1.00*sum(CASE 
                    WHEN DWELLING=8
                        THEN 1 
                        ELSE 0 
                    END)/sum(CASE 
                    WHEN DWELLING IS NOT NULL
                        THEN 1 
                        ELSE 0 
                    END) as FLOAT) as informalnotbck_percent,
        
                /* HOUSEHOLDS IN INFORMAL HOUSING */
                cast(1.00*sum(CASE 
                    WHEN DWELLING IN (7,8)
                        THEN 1 
                        ELSE 0 
                    END) as FLOAT) as informal_count,
        
                /* PERCENT HOUSEHOLDS IN INFORMAL HOUSING */
                cast(1.00*sum(CASE 
                    WHEN DWELLING IN (7,8)
                        THEN 1 
                        ELSE 0 
                    END)/sum(CASE 
                    WHEN DWELLING IS NOT NULL
                        THEN 1 
                        ELSE 0 
                    END) as FLOAT) as informal_percent,
        
                /* HOUSEHOLDS IN INFORMAL HOUSING (ALL BACKYARD) */
                cast(1.00*sum(CASE 
                    WHEN DWELLING IN (6,7,8)
                        THEN 1 
                        ELSE 0 
                    END) as FLOAT) as informalwbck_count,
        
                /* PERCENT HOUSEHOLDS IN INFORMAL HOUSING */
                cast(1.00*sum(CASE 
                    WHEN DWELLING IN (6,7,8)
                        THEN 1 
                        ELSE 0 
                    END)/sum(CASE 
                    WHEN DWELLING IS NOT NULL
                        THEN 1 
                        ELSE 0 
                    END) as FLOAT) as informalwbck_percent
    
            FROM census_hh_1996
            GROUP BY EACODE) AS A

            JOIN (SELECT EACODE,

                /* COUNT BLACK AFRICANS */
                cast(1.00*sum(CASE 
                    WHEN RACE=1
                        THEN 1 
                        ELSE 0 
                    END) as FLOAT) as black_count,
        
                /* PERCENT BLACK AFRICANS */
                cast(1.00*sum(CASE 
                    WHEN RACE=1
                        THEN 1 
                        ELSE 0 
                    END)/sum(CASE 
                    WHEN RACE IS NOT NULL
                        THEN 1 
                        ELSE 0 
                    END) as FLOAT) as black_percent,
        
                /* COUNT COLOURED */
                cast(1.00*sum(CASE 
                    WHEN RACE=2
                        THEN 1 
                        ELSE 0 
                    END) as FLOAT) as coloured_count,
        
                /* PERCENT COLOURED */
                cast(1.00*sum(CASE 
                    WHEN RACE=2
                        THEN 1 
                        ELSE 0 
                    END)/sum(CASE 
                    WHEN RACE IS NOT NULL
                        THEN 1 
                        ELSE 0 
                    END) as FLOAT) as coloured_percent,
        
                /* COUNT INDIAN/ASIAN */
                cast(1.00*sum(CASE 
                    WHEN RACE=3
                        THEN 1 
                        ELSE 0 
                    END) as FLOAT) as indas_count,
        
                /* PERCENT INDIAN/ASIAN */
                cast(1.00*sum(CASE 
                    WHEN RACE=3
                        THEN 1 
                        ELSE 0 
                    END)/sum(CASE 
                    WHEN RACE IS NOT NULL
                        THEN 1 
                        ELSE 0 
                    END) as FLOAT) as indas_percent,
        
                /* COUNT WHITE */
                cast(1.00*sum(CASE 
                    WHEN RACE=4
                        THEN 1 
                        ELSE 0 
                    END) as FLOAT) as white_count,
        
                /* PERCENT WHITE */
                cast(1.00*sum(CASE 
                    WHEN RACE=4
                        THEN 1 
                        ELSE 0 
                    END)/sum(CASE 
                    WHEN RACE IS NOT NULL
                        THEN 1 
                        ELSE 0 
                    END) as FLOAT) as white_percent,
        
                /* COUNT NO SCHOOLING */
                cast(1.00*sum(CASE 
                    WHEN DEDUCODE=1
                        THEN 1 
                        ELSE 0 
                    END) as FLOAT) as noschool_count,
        
                /* PERCENT NO SCHOOLING */
                cast(1.00*sum(CASE 
                    WHEN DEDUCODE=1
                        THEN 1 
                        ELSE 0 
                    END)/sum(CASE 
                    WHEN DEDUCODE IS NOT NULL
                        THEN 1 
                        ELSE 0 
                    END) as FLOAT) as noschool_percent,
        
                /* COUNT SOME PRIMARY */
                cast(1.00*sum(CASE 
                    WHEN DEDUCODE>=2 AND DEDUCODE<=8
                        THEN 1 
                        ELSE 0 
                    END) as FLOAT) as somprim_count,
        
                /* PERCENT SOME PRIMARY */
                cast(1.00*sum(CASE 
                    WHEN DEDUCODE>=2 AND DEDUCODE<=8
                        THEN 1 
                        ELSE 0 
                    END)/sum(CASE 
                    WHEN DEDUCODE IS NOT NULL
                        THEN 1 
                        ELSE 0 
                    END) as FLOAT) as somprim_percent,
        
                /* COUNT PRIMARY */
                cast(1.00*sum(CASE 
                    WHEN DEDUCODE=9
                        THEN 1 
                        ELSE 0 
                    END) as FLOAT) as primary_count,
        
                /* PERCENT PRIMARY */
                cast(1.00*sum(CASE 
                    WHEN DEDUCODE=9
                        THEN 1 
                        ELSE 0 
                    END)/sum(CASE 
                    WHEN DEDUCODE IS NOT NULL
                        THEN 1 
                        ELSE 0 
                    END) as FLOAT) as primary_percent,
        
                /* COUNT SOME SECONDARY */
                cast(1.00*sum(CASE 
                    WHEN DEDUCODE>=10 AND DEDUCODE<=13
                        THEN 1 
                        ELSE 0 
                    END) as FLOAT) as somsec_count,
        
                /* PERCENT SOME SECONDARY */
                cast(1.00*sum(CASE 
                    WHEN DEDUCODE>=10 AND DEDUCODE<=13
                        THEN 1 
                        ELSE 0 
                    END)/sum(CASE 
                    WHEN DEDUCODE IS NOT NULL
                        THEN 1 
                        ELSE 0 
                    END) as FLOAT) as somsec_percent,
        
                /* COUNT SECONDARY */
                cast(1.00*sum(CASE 
                    WHEN DEDUCODE=15
                        THEN 1 
                        ELSE 0 
                    END) as FLOAT) as secondary_count,
        
                /* PERCENT SECONDARY */
                cast(1.00*sum(CASE 
                    WHEN DEDUCODE=15
                        THEN 1 
                        ELSE 0 
                    END)/sum(CASE 
                    WHEN DEDUCODE IS NOT NULL
                        THEN 1 
                        ELSE 0 
                    END) as FLOAT) as secondary_percent,
        
                /* COUNT HIGHER */
                cast(1.00*sum(CASE 
                    WHEN DEDUCODE>=16 AND DEDUCODE<=22
                        THEN 1 
                        ELSE 0 
                    END) as FLOAT) as higher_count,
        
                /* PERCENT HIGHER */
                cast(1.00*sum(CASE 
                    WHEN DEDUCODE>=16 AND DEDUCODE<=22
                        THEN 1 
                        ELSE 0 
                    END)/sum(CASE 
                    WHEN DEDUCODE IS NOT NULL
                        THEN 1 
                        ELSE 0 
                    END) as FLOAT) as higher_percent,
        
                /* AVERAGE INCOME */
                cast(1.00*avg(CASE 
                    WHEN INCOME = 1  THEN 0
                    WHEN INCOME = 2  THEN 1200
                    WHEN INCOME = 3  THEN 4200
                    WHEN INCOME = 4  THEN 9000
                    WHEN INCOME = 5  THEN 15000
                    WHEN INCOME = 6  THEN 24000
                    WHEN INCOME = 7  THEN 36000
                    WHEN INCOME = 8  THEN 48000
                    WHEN INCOME = 9  THEN 63000
                    WHEN INCOME = 10 THEN 84000
                    WHEN INCOME = 11 THEN 114000
                    WHEN INCOME = 12 THEN 162000
                    WHEN INCOME = 13 THEN 276000
                    WHEN INCOME = 14 THEN 550000
                    ELSE NULL
                    END) as FLOAT) as avg_income

            FROM census_pers_1996
            GROUP BY EACODE) AS B

            ON A.EACODE=B.EACODE;
            '''

        index_qry = '''
                    CREATE INDEX census_1996_ea_ind ON census_1996_ea (EACODE);
                    '''

    con = sql.connect(db)
    cur = con.cursor()

    cur.execute(drop_qry)
    cur.execute(create_qry)
    cur.execute(index_qry)

    con.commit()
    con.close()