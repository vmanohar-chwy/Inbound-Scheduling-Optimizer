# -*- coding: utf-8 -*-
"""
Created on Fri Oct  4 10:27:07 2019

@author: vmanohar
"""
import logging
import logging.handlers
import csv
import pandas as pd
#from vertica_python import connect
import pyodbc
import datetime as dtm
from io import BytesIO as StringIO
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.base import MIMEBase
from email import encoders
import unicodedata
date = str(dtm.datetime.today().date())
logger = logging.getLogger('csv template process')
logger.setLevel(logging.DEBUG)
rh = logging.handlers.RotatingFileHandler("ISO_process.log",maxBytes = 500*1024,backupCount = 1)
rh.setLevel(logging.DEBUG)
ch = logging.StreamHandler()
ch.setLevel(logging.DEBUG)
formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(name)s - %(message)s')
rh.setFormatter(formatter)
ch.setFormatter(formatter)
for handler in logger.handlers[:]:
    logger.removeHandler(handler)
logger.addHandler(rh)
logger.addHandler(ch)
 #conn_info = {'host' : 'bidb.chewy.local',
     #'port': 5433,
     #'user': 'vmanohar',
     #'password':'Venkat0SU2018',
     #'database':'bidb'
    #}
    #connecting to vertica
    #connection = connect(**conn_info)
    #cur = connection.cursor()
    
cxn = pyodbc.connect("DSN=BIDB",autocommit = True)
cur = cxn.cursor()

try:
    FC_list = ['AVP1','CFC1','DAY1','MCO1']
    FC_weekend = []
    for fc in FC_list:
        query = """
        WITH data2 AS 
                    (
                     SELECT cpl.PO_no AS document_number,MAX(cpl.Ref_no) AS reference_number , MAX(cpl.VRDD:: DATE) AS requested_appt_date , MAX(cpl.Created_dt) AS created_dttm
                    FROM sandbox_supply_chain.carrier_portal_new_test AS cpl
                    WHERE cpl.FC_nm = '{0}'  AND cpl.Created_dt BETWEEN (SELECT CASE WHEN DAYOFWEEK(current_date) = 2 THEN current_date-3 + INTERVAL '0 HOUR' ELSE current_date-1 + INTERVAL '0 SECOND' END ) AND (SELECT current_date - INTERVAL '1 SECOND')   
                    AND cpl.Ref_no NOT IN (SELECT Ref_no FROM  sandbox_supply_chain.iso_exception)
                    AND cpl.Ref_no <> '191121-030927'
                    GROUP BY 1
                    )
            ,data AS
                    (
                    SELECT d1.reference_number AS Ref_no, d1.requested_appt_date AS VRDD1, d1.document_number AS PO_no,d1.created_dttm AS cr_dt,cpl.carrier_scac AS sc,cpl.carrier_name AS csr
                    FROM data2 AS d1
                    JOIN sandbox_supply_chain.carrier_portal_new_test AS cpl
                    ON  d1.reference_number = cpl.Ref_no AND cpl.PO_no = d1.document_number
                    )
            , parameters AS
                    (
                    SELECT d.Ref_no, SUM(pdp.qty) AS IB_units, COUNT(DISTINCT pdp.item_number) AS sku, 
                CASE
                        WHEN (SUM(pdp.qty)/COUNT(DISTINCT pdp.item_number)) > 61 THEN 1
                        WHEN (SUM(pdp.qty)/COUNT(DISTINCT pdp.item_number)) > 51 THEN 2
                        WHEN (SUM(pdp.qty)/COUNT(DISTINCT pdp.item_number)) > 41 THEN 3
                        WHEN (SUM(pdp.qty)/COUNT(DISTINCT pdp.item_number)) > 21 THEN 4
                        WHEN (SUM(pdp.qty)/COUNT(DISTINCT pdp.item_number)) <= 21 THEN 5
                END as high_jump_rank,COUNT(DISTINCT pdp.po_number) AS po_count 
                FROM data AS d
                JOIN aad.t_po_detail AS pdp
                ON d.PO_no = pdp.po_number
                GROUP BY 1
                    )
             ,obj1 AS 
                    (
                     SELECT d.Ref_no, AVG(DATEDIFF(day,pdpm.document_original_requested_delivery_dttm,d.VRDD1)) AS obj
                     FROM data AS d
                     JOIN chewybi.procurement_document_product_measures AS pdpm
                     ON d.PO_no = pdpm.document_number
                     GROUP BY 1
                    ) 
            ,obj AS 
                    (
                     SELECT Ref_no, CASE WHEN obj IS NULL THEN 0 ELSE obj END AS obj
                     FROM obj1
                    )
            ,vas_parameters AS
                    (
                    SELECT d.Ref_no,sum(pdp.qty) AS vas_units
                    FROM data AS d
                    JOIN aad.t_po_detail AS pdp
                    ON d.PO_no = pdp.po_number
                    JOIN chewybi.products AS p
                    ON pdp.item_number = p.product_part_number 
                    WHERE p.product_merch_classification2 = 'Litter'  AND p.product_vas_profile_description IN ('SHRINKWRAP')  
                    GROUP BY 1
                    )
                        
            ,cont_flag AS
                    (
                    SELECT DISTINCT d.Ref_no, d.VRDD1,
                    CASE WHEN v.vendor_number IN ('P000533','B000050','1760','9295','9302','P000544','P000508','P000486','P000400','7701','P000398','B000064','P000421','P000476','3755','3722','8038','5223') THEN 3
                         ELSE NULL 
                    END AS cont_flag
                    FROM data AS d
                    JOIN chewybi.procurement_document_measures AS pdm
                    ON d.PO_no = pdm.document_number
                    JOIN chewybi.vendors AS v
                    USING (vendor_key)
                    )
            ,cont_fl AS
                    (
                    SELECT * , ROW_NUMBER() OVER (PARTITION BY VRDD1) AS rank
                    FROM cont_flag
                    WHERE cont_flag IS NOT NULL
                    )
            ,stand_appt AS
                    (
                    SELECT DISTINCT d.Ref_no,d.PO_no,d.VRDD1,d.cr_dt,
                           CASE WHEN LOWER(d.csr) LIKE 'estes%' THEN '000000012'
                            WHEN LOWER(d.csr) LIKE 'yrc%' THEN '00000007'
                            WHEN LOWER(d.csr) LIKE 'saia%' THEN '00000004'
                            WHEN LOWER(d.csr) LIKE 'fedex%' THEN '9000'
                            WHEN LOWER(d.csr) LIKE 'ups%' THEN '00000002'
                            ELSE v.vendor_number END as vendor_number ,v.vendor_name
                    FROM data AS d
                    JOIN chewybi.procurement_document_measures AS pdm
                    ON d.PO_no = pdm.document_number
                    JOIN chewybi.vendors AS v
                    ON pdm.vendor_key = v.vendor_key
                    )
            ,stand_slot AS
                    (
                    SELECT Ref_no,PO_no, VRDD1,vendor_number,cr_dt, ROW_NUMBER() OVER(PARTITION BY VRDD1,vendor_number) AS rank
                    FROM stand_appt
                    WHERE stand_flag = 1
                    ORDER BY VRDD1
                    )
            ,vas_final AS 
                    (
                    SELECT d.Ref_no, CASE WHEN vp.vas_units IS NULL THEN 0 ELSE vp.vas_units END AS vas_units
                    FROM data AS d
                    LEFT JOIN vas_parameters AS vp
                    ON vp.Ref_no = d.Ref_no
                    )           
            SELECT d.Ref_no,d.VRDD1 AS VRDD, 
                        CASE WHEN p1.obj < -1 AND DAYOFWEEK(d.VRDD1-p1.obj) IN (2,3,4,5,6) AND c.cont_flag IS NULL  THEN CAST(d.VRDD1-p1.obj AS DATE) 
                              WHEN p1.obj < -1 AND DAYOFWEEK(d.VRDD1-p1.obj) = 1 AND c.cont_flag IS NULL  THEN CAST(d.VRDD1-p1.obj + 1 AS DATE) 
                              WHEN p1.obj < -1 AND DAYOFWEEK(d.VRDD1-p1.obj) = 7 AND c.cont_flag IS NULL   THEN CAST(d.VRDD1-p1.obj+2 AS DATE)  ELSE CAST(d.VRDD1 AS DATE) END AS VRDD1, 
                         CASE WHEN p1.obj < -1 AND DAYOFWEEK(d.VRDD1-p1.obj) IN (3,4,5,6) AND c.cont_flag IS NULL  THEN CAST(d.VRDD1-p1.obj-1 AS DATE) 
                              WHEN p1.obj < -1 AND DAYOFWEEK(d.VRDD1-p1.obj) IN (2,7,1) AND c.cont_flag IS NULL THEN CAST(d.VRDD1-p1.obj-3 AS DATE)  
                              WHEN p1.obj >= -1 AND DAYOFWEEK(d.VRDD1) = 6 AND c.cont_flag IS NULL  THEN CAST(d.VRDD1+3 AS DATE) 
                              WHEN c.cont_flag IS NOT NULL AND DAYOFWEEK(d.VRDD1) = 6 THEN CAST(d.VRDD1+3 AS DATE) ELSE CAST(d.VRDD1+1 AS DATE) END AS VRDD2,
                         CASE WHEN p1.obj < -1 AND DAYOFWEEK(d.VRDD1-p1.obj) IN (4,5,6) AND c.cont_flag IS NULL  THEN CAST(d.VRDD1-p1.obj-2 AS DATE) 
                              WHEN p1.obj < -1 AND DAYOFWEEK(d.VRDD1-p1.obj) IN (1,2,3,7) AND c.cont_flag IS NULL  THEN CAST(d.VRDD1-p1.obj-4 AS DATE) 
                              WHEN p1.obj >= -1 AND DAYOFWEEK(d.VRDD1) IN (5,6) AND c.cont_flag IS NULL THEN CAST(d.VRDD1+4 AS DATE) 
                              WHEN c.cont_flag IS NOT NULL AND DAYOFWEEK(d.VRDD1) IN (5,6) THEN CAST(d.VRDD1+4 AS DATE)  ELSE CAST(d.VRDD1+2 AS DATE) END AS VRDD3,
                   p.IB_units, p.sku,p1.obj, p.high_jump_rank,c.cont_flag,
                   CASE WHEN c.cont_flag IS NULL AND p.po_count <= 1 AND p.high_jump_rank IN (1,2,3,4) THEN 0 ELSE 1 END AS UPT,d.cr_dt,sa.vendor_number,sa.vendor_name,vl.vas_units
                   ,CASE WHEN vl.vas_units > 0 THEN 1 ELSE 0 END AS vas_flag,d.csr
            FROM data AS d
            JOIN parameters AS p
            ON d.Ref_no = p.Ref_no
            JOIN cont_flag AS c
            ON p.Ref_no = c.Ref_no
            JOIN stand_appt AS sa
            ON c.Ref_no = sa.Ref_no and d.PO_no = sa.PO_no
            JOIN obj AS p1
            ON p1.Ref_no = d.Ref_no
            LEFT JOIN vas_final AS vl
            ON vl.Ref_no = d.Ref_no;
        """.format(fc)
        #cur.execute(query)
        #logger.info("Vertica Query is Executed")
        #result = cur.fetchall()
        df = pd.read_sql(query,cxn)
        df.columns = ['appt_id','vrdd','vrdd1','vrdd2','vrdd3','units','sku','obj','high_jump_rank','con_fl','upt','cr_dt','vendor','vendor_name','vas_units','vas_flag','carrier_name']
        dt1 = dict([(str(i),[str(j),str(k),str(l)]) for i,j,k,l in zip(df.appt_id,df.vrdd1,df.vrdd2,df.vrdd3)])
        #st_fl = dict([str(i),str(j)] for i,j in zip(df.appt_id,df.st_fl))
        cont_fl = {str(k):g['appt_id'] for k,g in df.groupby('con_fl')}
        cnt_fl = dict([str(i),str(j)] for i,j in zip(df.appt_id,df.con_fl))
        vendor = dict([str(i),str(j)] for i,j in zip(df.appt_id,df.vendor))
        units_sku_obj = dict([(str(i),[int(j),int(k),float(l)]) for i,j,k,l in zip(df.appt_id,df.units,df.sku,df.obj)])
        b = dict([str(i),str(j)] for i,j in zip(df.appt_id,df.upt))
        cr_dt = dict([str(i),str(j)] for i,j in zip(df.appt_id,df.cr_dt))
        v_name = dict([str(i),str(j).replace(',',';')] for i,j in zip(df.appt_id,df.vendor_name))
        vas_units = dict([str(i),int(j)] for i,j in zip(df.appt_id,df.vas_units))
        vas_flag = dict([str(i),str(j)] for i,j in zip(df.appt_id,df.vas_flag))
        hj_rank = dict([str(i),str(j)] for i,j in zip(df.appt_id,df.high_jump_rank))
        csr = dict([str(i),str(j).replace(',',';')] for i,j in zip(df.appt_id,df.carrier_name))
        vrdd = dict([str(i),str(j)] for i,j in zip(df.appt_id,df.vrdd))
        
        #po_number
        query = """
                SELECT cpl.Ref_no,pdp.po_number, SUM(pdp.qty), COUNT(DISTINCT pdp.item_number)
                FROM sandbox_supply_chain.carrier_portal_new_test AS cpl
                JOIN aad.t_po_detail AS pdp
                ON cpl.PO_no = pdp.po_number
                WHERE cpl.FC_nm = '{0}' AND  cpl.Created_dt BETWEEN (SELECT CASE WHEN DAYOFWEEK(current_date) = 2 THEN current_date-3 + INTERVAL '0 SECOND' ELSE current_date-1 + INTERVAL '0 SECOND' END) AND (SELECT current_date - INTERVAL '1 SECOND') AND cpl.Ref_no NOT IN (SELECT Ref_no FROM  sandbox_supply_chain.iso_exception)  
                GROUP BY 1,2
        """.format(fc)
        #cur.execute(query)
        #logger.info("Vertica Query is Executed")
        #result = cur.fetchall()
        df = pd.read_sql(query,cxn)
        df.columns = ['ref','po','units','sku']
        ref_num = {str(k):g['po'].unique().tolist()for k,g in df.groupby('ref')}
        po = dict([(str(i),str(j)),[int(k),int(l)]] for i,j,k,l in zip(df.ref,df.po,df.units,df.sku))
        
        #ordd
        query = """
                SELECT cpl.Ref_no,pdp.document_number,ISNULL(pdp.document_original_requested_delivery_dttm:: DATE,'1900-01-01')
                FROM sandbox_supply_chain.carrier_portal_new_test AS cpl
                JOIN chewybi.procurement_document_measures AS pdp
                ON cpl.PO_no = pdp.document_number
                WHERE  cpl.FC_nm = '{0}' AND cpl.Created_dt  BETWEEN (SELECT CASE WHEN DAYOFWEEK(current_date) = 2 THEN current_date-3 + INTERVAL '0 SECOND' ELSE current_date-1 + INTERVAL '0 SECOND' END) AND (SELECT current_date - INTERVAL '1 SECOND') AND cpl.Ref_no NOT IN (SELECT Ref_no FROM  sandbox_supply_chain.iso_exception)
        """.format(fc)
        #cur.execute(query)
        #logger.info("Vertica Query is Executed")
        #result = cur.fetchall()
        df = pd.read_sql(query,cxn)
        df.columns = ['ref','po','ordd']
        ordd = dict([str(i),str(j)] for i,j in zip(df.po,df.ordd))
        
        query = """
        WITH data AS (SELECT cpl.Ref_no, cpl.VRDD:: DATE, cpl.Created_dt:: DATE,pdm.document_number
        FROM chewybi.procurement_document_measures AS pdm
        JOIN sandbox_supply_chain.carrier_portal_new_test AS cpl
        ON cpl.PO_no = pdm.document_number
        WHERE cpl.FC_nm = '{0}' AND cpl.Created_dt BETWEEN (SELECT CASE WHEN DAYOFWEEK(current_date) = 2 THEN current_date-3 + INTERVAL '0 SECOND' ELSE current_date-1 + INTERVAL '0 SECOND' END) AND (SELECT current_date - INTERVAL '1 SECOND') AND cpl.Ref_no NOT IN (SELECT Ref_no FROM  sandbox_supply_chain.iso_exception)
        )
        SELECT d.Ref_no,apl.appointment_id,apl.request_date:: DATE, request_time:: TIME, d.document_number
        FROM data AS d
        JOIN aad.t_appt_appointment_log_po AS pol
        ON d.document_number = pol.po_number
        JOIN aad.t_appt_appointment_log AS apl
        ON apl.appointment_id = pol.appointment_id
        """.format(fc)
        #cur.execute(query)
        #result = cur.fetchall()
        df = pd.read_sql(query,cxn)
        rsch = {}
        if df.empty == False:
            df.columns = ['reference_number','appointment_id','Date','Time','PO_number']
            rsch = dict([(str(i),str(j)),[str(k),str(l)]] for i,j,k,l in zip(df.appointment_id,df.PO_number,df.Date,df.Time))
   
        else:
            pass
        outfile = open('Schedule_'+fc+'_'+str(date)+'_.csv','w')
        outfile.write('Reference_number'+','+'PO_number'+','+'Scheduled_date'+','+'Scheduled_time'+','+'units'+','+'sku'+','+'hj_rank'+','+'vendor'+','+'carrier'+','+'delete'+','+'ORDD'+','+'VRDD'+','+'vas_units'+','+'VNA'+','+'Reason')
        outfile.write('\n')
        cnt = 0
        for i in sorted(dt1.keys()):
            for j in ref_num[i]:
                outfile.write(str(i)+','+str(j)+','+'MM/DD/YYYY'+','+'HH:MM:SS'+','+str(po[(i,j)][0])+','+str(po[(i,j)][1])+','+str(hj_rank[i])+','+str(v_name[i])+','+str(csr[i])+','+'N'+','+str(ordd[j])+','+str(vrdd[i])+','+str(vas_units[i])+','+',')
                outfile.write('\n')
        for i,j in rsch.keys():
            cnt = cnt+1
            outfile.write(str(i)+','+str(j)+','+str(rsch[(i,j)][0])+','+str(rsch[(i,j)][1])+','+','+','+','+','+','+'Y')
            if cnt < len(rsch.keys()):
                outfile.write('\n')
            else:
                pass
        logger.info("CSV file for %s is created"%(fc))
        outfile.close()
        
# =============================================================================
#     
#     for fc in FC_weekend:
#         query = """
#         WITH data2 AS 
#                     (
#                      SELECT cpl.PO_no AS document_number,MAX(cpl.Ref_no) AS reference_number , MAX(cpl.VRDD:: DATE) AS requested_appt_date , MAX(cpl.Created_dt) AS created_dttm
#                     FROM sandbox_supply_chain.carrier_portal_new_test AS cpl
#                     WHERE cpl.FC_nm = '{0}'  AND cpl.Created_dt BETWEEN (SELECT CASE WHEN DAYOFWEEK(current_date) = 2 THEN current_date-3 + INTERVAL '0 SECOND' ELSE current_date-1 + INTERVAL '0 SECOND' END ) AND (SELECT current_date - INTERVAL '1 SECOND')   
#                     AND cpl.Ref_no NOT IN (SELECT Ref_no FROM  sandbox_supply_chain.iso_exception)
#                     AND cpl.Ref_no <> '190922-029070'
#                     GROUP BY 1
#                     )
#             ,data AS
#                     (
#                     SELECT d1.reference_number AS Ref_no, CASE WHEN DAYOFWEEK(d1.requested_appt_date) = 7 THEN d1.requested_appt_date+2 WHEN DAYOFWEEK(d1.requested_appt_date) = 1 THEN d1.requested_appt_date+1 ELSE d1.requested_appt_date END  AS VRDD1, d1.document_number AS PO_no,d1.created_dttm AS cr_dt,cpl.carrier_scac AS sc,cpl.carrier_name AS csr
#                     FROM data2 AS d1
#                     JOIN sandbox_supply_chain.carrier_portal_new_test AS cpl
#                     ON  d1.reference_number = cpl.Ref_no AND cpl.PO_no = d1.document_number
#                     )
#             , parameters AS
#                     (
#                     SELECT d.Ref_no, SUM(pdp.qty) AS IB_units, COUNT(DISTINCT pdp.item_number) AS sku, 
#                 CASE
#                         WHEN (SUM(pdp.qty)/COUNT(DISTINCT pdp.item_number)) > 61 THEN 1
#                         WHEN (SUM(pdp.qty)/COUNT(DISTINCT pdp.item_number)) > 51 THEN 2
#                         WHEN (SUM(pdp.qty)/COUNT(DISTINCT pdp.item_number)) > 41 THEN 3
#                         WHEN (SUM(pdp.qty)/COUNT(DISTINCT pdp.item_number)) > 21 THEN 4
#                         WHEN (SUM(pdp.qty)/COUNT(DISTINCT pdp.item_number)) <= 21 THEN 5
#                 END as high_jump_rank,COUNT(DISTINCT pdp.po_number) AS po_count 
#                 FROM data AS d
#                 JOIN aad.t_po_detail AS pdp
#                 ON d.PO_no = pdp.po_number
#                 GROUP BY 1
#                     )
#              ,obj1 AS 
#                     (
#                      SELECT d.Ref_no, AVG(DATEDIFF(day,pdpm.document_original_requested_delivery_dttm,d.VRDD1)) AS obj
#                      FROM data AS d
#                      JOIN chewybi.procurement_document_product_measures AS pdpm
#                      ON d.PO_no = pdpm.document_number
#                      GROUP BY 1
#                     ) 
#             ,obj AS 
#                     (
#                      SELECT Ref_no, CASE WHEN obj IS NULL THEN 0 ELSE obj END AS obj
#                      FROM obj1
#                     )
#             ,vas_parameters AS
#                     (
#                     SELECT d.Ref_no,sum(pdp.qty) AS vas_units
#                     FROM data AS d
#                     JOIN aad.t_po_detail AS pdp
#                     ON d.PO_no = pdp.po_number
#                     JOIN chewybi.products AS p
#                     ON pdp.item_number = p.product_part_number 
#                     WHERE p.product_merch_classification2 = 'Litter'  AND p.product_vas_profile_description IN ('SHRINKWRAP')  
#                     GROUP BY 1
#                     )
#                         
#             ,cont_flag AS
#                     (
#                     SELECT DISTINCT d.Ref_no, d.VRDD1,
#                     CASE WHEN v.vendor_number IN ('P000533','B000050','1760','9295','9302','P000544','P000508','P000486','P000400','7701','P000398','B000064','P000421','P000476','3755','3722','8038','5223') THEN 3
#                          ELSE NULL 
#                     END AS cont_flag
#                     FROM data AS d
#                     JOIN chewybi.procurement_document_measures AS pdm
#                     ON d.PO_no = pdm.document_number
#                     JOIN chewybi.vendors AS v
#                     USING (vendor_key)
#                     )
#             ,cont_fl AS
#                     (
#                     SELECT * , ROW_NUMBER() OVER (PARTITION BY VRDD1) AS rank
#                     FROM cont_flag
#                     WHERE cont_flag IS NOT NULL
#                     )
#             ,stand_appt AS
#                     (
#                     SELECT DISTINCT d.Ref_no,d.PO_no,d.VRDD1,d.cr_dt,
#                            CASE WHEN LOWER(d.csr) LIKE 'estes%' THEN '000000012'
#                             WHEN LOWER(d.csr) LIKE 'yrc%' THEN '00000007'
#                             WHEN LOWER(d.csr) LIKE 'saia%' THEN '00000004'
#                             WHEN LOWER(d.csr) LIKE 'fedex%' THEN '9000'
#                             WHEN LOWER(d.csr) LIKE 'ups%' THEN '00000002'
#                             ELSE v.vendor_number END as vendor_number ,v.vendor_name
#                     FROM data AS d
#                     JOIN chewybi.procurement_document_measures AS pdm
#                     ON d.PO_no = pdm.document_number
#                     JOIN chewybi.vendors AS v
#                     ON pdm.vendor_key = v.vendor_key
#                     )
#             ,stand_slot AS
#                     (
#                     SELECT Ref_no,PO_no, VRDD1,vendor_number,cr_dt, ROW_NUMBER() OVER(PARTITION BY VRDD1,vendor_number) AS rank
#                     FROM stand_appt
#                     WHERE stand_flag = 1
#                     ORDER BY VRDD1
#                     )
#             ,vas_final AS 
#                     (
#                     SELECT d.Ref_no, CASE WHEN vp.vas_units IS NULL THEN 0 ELSE vp.vas_units END AS vas_units
#                     FROM data AS d
#                     LEFT JOIN vas_parameters AS vp
#                     ON vp.Ref_no = d.Ref_no
#                     )           
#             SELECT d.Ref_no,d.VRDD1 AS VRDD, 
#                         CASE WHEN p1.obj < -1 AND DAYOFWEEK(d.VRDD1-p1.obj) IN (2,3,4,5,6) AND c.cont_flag IS NULL  THEN CAST(d.VRDD1-p1.obj AS DATE) 
#                               WHEN p1.obj < -1 AND DAYOFWEEK(d.VRDD1-p1.obj) = 1 AND c.cont_flag IS NULL  THEN CAST(d.VRDD1-p1.obj + 1 AS DATE) 
#                               WHEN p1.obj < -1 AND DAYOFWEEK(d.VRDD1-p1.obj) = 7 AND c.cont_flag IS NULL   THEN CAST(d.VRDD1-p1.obj+2 AS DATE)  ELSE CAST(d.VRDD1 AS DATE) END AS VRDD1, 
#                          CASE WHEN p1.obj < -1 AND DAYOFWEEK(d.VRDD1-p1.obj) IN (3,4,5,6) AND c.cont_flag IS NULL  THEN CAST(d.VRDD1-p1.obj-1 AS DATE) 
#                               WHEN p1.obj < -1 AND DAYOFWEEK(d.VRDD1-p1.obj) IN (2,7,1) AND c.cont_flag IS NULL THEN CAST(d.VRDD1-p1.obj-3 AS DATE)  
#                               WHEN p1.obj >= -1 AND DAYOFWEEK(d.VRDD1) = 6 AND c.cont_flag IS NULL  THEN CAST(d.VRDD1+3 AS DATE) 
#                               WHEN c.cont_flag IS NOT NULL AND DAYOFWEEK(d.VRDD1) = 6 THEN CAST(d.VRDD1+3 AS DATE) ELSE CAST(d.VRDD1+1 AS DATE) END AS VRDD2,
#                          CASE WHEN p1.obj < -1 AND DAYOFWEEK(d.VRDD1-p1.obj) IN (4,5,6) AND c.cont_flag IS NULL  THEN CAST(d.VRDD1-p1.obj-2 AS DATE) 
#                               WHEN p1.obj < -1 AND DAYOFWEEK(d.VRDD1-p1.obj) IN (1,2,3,7) AND c.cont_flag IS NULL  THEN CAST(d.VRDD1-p1.obj-4 AS DATE) 
#                               WHEN p1.obj >= -1 AND DAYOFWEEK(d.VRDD1) IN (5,6) AND c.cont_flag IS NULL THEN CAST(d.VRDD1+4 AS DATE) 
#                               WHEN c.cont_flag IS NOT NULL AND DAYOFWEEK(d.VRDD1) IN (5,6) THEN CAST(d.VRDD1+4 AS DATE)  ELSE CAST(d.VRDD1+2 AS DATE) END AS VRDD3,
#                    p.IB_units, p.sku,p1.obj, p.high_jump_rank,c.cont_flag,
#                    CASE WHEN c.cont_flag IS NULL AND p.po_count <= 1 AND p.high_jump_rank IN (1,2,3,4) THEN 0 ELSE 1 END AS UPT,d.cr_dt,sa.vendor_number,sa.vendor_name,vl.vas_units
#                    ,CASE WHEN vl.vas_units > 0 THEN 1 ELSE 0 END AS vas_flag,d.csr
#             FROM data AS d
#             JOIN parameters AS p
#             ON d.Ref_no = p.Ref_no
#             JOIN cont_flag AS c
#             ON p.Ref_no = c.Ref_no
#             JOIN stand_appt AS sa
#             ON c.Ref_no = sa.Ref_no and d.PO_no = sa.PO_no
#             JOIN obj AS p1
#             ON p1.Ref_no = d.Ref_no
#             LEFT JOIN vas_final AS vl
#             ON vl.Ref_no = d.Ref_no;
#         """.format(fc)
#         #cur.execute(query)
#         logger.info("Vertica Query is Executed")
#         #result = cur.fetchall()
#         df = pd.read_sql(query,cxn)
#         df.columns = ['appt_id','vrdd','vrdd1','vrdd2','vrdd3','units','sku','obj','high_jump_rank','con_fl','upt','cr_dt','vendor','vendor_name','vas_units','vas_flag','carrier_name']
#         dt1 = dict([(str(i),[str(j),str(k),str(l)]) for i,j,k,l in zip(df.appt_id,df.vrdd1,df.vrdd2,df.vrdd3)])
#         #st_fl = dict([str(i),str(j)] for i,j in zip(df.appt_id,df.st_fl))
#         cont_fl = {str(k):g['appt_id'] for k,g in df.groupby('con_fl')}
#         cnt_fl = dict([str(i),str(j)] for i,j in zip(df.appt_id,df.con_fl))
#         vendor = dict([str(i),str(j)] for i,j in zip(df.appt_id,df.vendor))
#         units_sku_obj = dict([(str(i),[int(j),int(k),float(l)]) for i,j,k,l in zip(df.appt_id,df.units,df.sku,df.obj)])
#         b = dict([str(i),str(j)] for i,j in zip(df.appt_id,df.upt))
#         cr_dt = dict([str(i),str(j)] for i,j in zip(df.appt_id,df.cr_dt))
#         v_name = dict([str(i),str(j).replace(',',';')] for i,j in zip(df.appt_id,df.vendor_name))
#         vas_units = dict([str(i),int(j)] for i,j in zip(df.appt_id,df.vas_units))
#         vas_flag = dict([str(i),str(j)] for i,j in zip(df.appt_id,df.vas_flag))
#         hj_rank = dict([str(i),str(j)] for i,j in zip(df.appt_id,df.high_jump_rank))
#         csr = dict([str(i),str(j).replace(',',';')] for i,j in zip(df.appt_id,df.carrier_name))
#         vrdd = dict([str(i),str(j)] for i,j in zip(df.appt_id,df.vrdd))
#         
#         #po_number
#         query = """
#                 SELECT cpl.Ref_no,pdp.po_number, SUM(pdp.qty), COUNT(DISTINCT pdp.item_number)
#                 FROM sandbox_supply_chain.carrier_portal_new_test AS cpl
#                 JOIN aad.t_po_detail AS pdp
#                 ON cpl.PO_no = pdp.po_number
#                 WHERE cpl.Created_dt BETWEEN (SELECT CASE WHEN DAYOFWEEK(current_date) = 2 THEN current_date-3 + INTERVAL '0 SECOND' ELSE current_date-1 + INTERVAL '0 SECOND' END) AND (SELECT current_date - INTERVAL '1 SECOND') AND cpl.FC_nm = '{0}' AND cpl.Ref_no NOT IN (SELECT Ref_no FROM  sandbox_supply_chain.iso_exception)
#                 GROUP BY 1,2
#         """.format(fc)
#         cur.execute(query)
#         logger.info("Vertica Query is Executed")
#         result = cur.fetchall()
#         df = pd.read_sql(query,cxn)
#         df.columns = ['ref','po','units','sku']
#         ref_num = {str(k):g['po'].unique().tolist()for k,g in df.groupby('ref')}
#         po = dict([(str(i),str(j)),[int(k),int(l)]] for i,j,k,l in zip(df.ref,df.po,df.units,df.sku))
#         
#         #ordd
#         query = """
#                 SELECT cpl.Ref_no,pdp.document_number,ISNULL(pdp.document_original_requested_delivery_dttm:: DATE,'1900-01-01')
#                 FROM sandbox_supply_chain.carrier_portal_new_test AS cpl
#                 JOIN chewybi.procurement_document_measures AS pdp
#                 ON cpl.PO_no = pdp.document_number
#                 WHERE cpl.Created_dt BETWEEN (SELECT CASE WHEN DAYOFWEEK(current_date) = 2 THEN current_date-3 + INTERVAL '0 SECOND' ELSE current_date-1 + INTERVAL '0 SECOND' END) AND (SELECT current_date - INTERVAL '1 SECOND') AND cpl.FC_nm = '{0}' AND cpl.Ref_no NOT IN (SELECT Ref_no FROM  sandbox_supply_chain.iso_exception)
#         """.format(fc)
#         cur.execute(query)
#         logger.info("Vertica Query is Executed")
#         result = cur.fetchall()
#         df = pd.DataFrame(data = result)
#         df.columns = ['ref','po','ordd']
#         ordd = dict([str(i),str(j)] for i,j in zip(df.po,df.ordd))
#         
#         query = """
#         WITH data AS (SELECT cpl.Ref_no, cpl.VRDD:: DATE, cpl.Created_dt:: DATE,pdm.document_number
#         FROM chewybi.procurement_document_measures AS pdm
#         JOIN sandbox_supply_chain.carrier_portal_new_test AS cpl
#         ON cpl.PO_no = pdm.document_number
#         WHERE cpl.FC_nm = '{0}' AND cpl.Created_dt BETWEEN (SELECT CASE WHEN DAYOFWEEK(current_date) = 2 THEN current_date-3 + INTERVAL '0 SECOND' ELSE current_date-1 + INTERVAL '0 SECOND' END) AND (SELECT current_date - INTERVAL '1 SECOND') AND cpl.Ref_no NOT IN (SELECT Ref_no FROM  sandbox_supply_chain.iso_exception)
#         )
#         SELECT d.Ref_no,apl.appointment_id,apl.request_date:: DATE, request_time:: TIME, d.document_number
#         FROM data AS d
#         JOIN aad.t_appt_appointment_log_po AS pol
#         ON d.document_number = pol.po_number
#         JOIN aad.t_appt_appointment_log AS apl
#         ON apl.appointment_id = pol.appointment_id
#         """.format(fc)
#         cur.execute(query)
#         result = cur.fetchall()
#         df = pd.DataFrame(data=result)
#         rsch = {}
#         if df.empty == False:
#             df.columns = ['reference_number','appointment_id','Date','Time','PO_number']
#             rsch = dict([(str(i),str(j)),[str(k),str(l)]] for i,j,k,l in zip(df.appointment_id,df.PO_number,df.Date,df.Time))
#    
#         else:
#             pass
#         outfile = open('E:\\VManohar\\ISO\\FC\\Mail Script\\Schedule_'+fc+'_.csv','w')
#         outfile.write('Reference_number'+','+'PO_number'+','+'Scheduled_date'+','+'Scheduled_time'+','+'units'+','+'sku'+','+'hj_rank'+','+'vendor'+','+'carrier'+','+'delete'+','+'ORDD'+','+'VRDD'+','+'vas_units'+','+'VNA'+','+'Reason')
#         outfile.write('\n')
#         cnt = 0
#         for i in sorted(dt1.keys()):
#             for j in ref_num[i]:
#                 outfile.write(str(i)+','+str(j)+','+'MM/DD/YYYY'+','+'HH:MM:SS'+','+str(po[(i,j)][0])+','+str(po[(i,j)][1])+','+str(hj_rank[i])+','+str(v_name[i])+','+str(csr[i])+','+'N'+','+str(ordd[j])+','+str(vrdd[i])+','+str(vas_units[i])+','+',')
#                 outfile.write('\n')
#         for i,j in rsch.keys():
#             cnt = cnt+1
#             outfile.write(str(i)+','+str(j)+','+str(rsch[(i,j)][0])+','+str(rsch[(i,j)][1])+','+','+','+','+','+','+'Y')
#             if cnt < len(rsch.keys()):
#                 outfile.write('\n')
#             else:
#                 pass
#         logger.info("CSV file for %s is created"%(fc))
#         outfile.close()
# =============================================================================
    fromaddr = 'scsystems@chewy.com'
    toaddr = 'vmanohar@chewy.com'
    to = ', '.join(toaddr)
    msg = MIMEMultipart()
    msg['From'] = fromaddr
    msg['To'] = toaddr
    msg['Subject'] = "CSV template Process Successfully ran" 
    body = "Hello,\nCSV process ran for all FC.\nThanks"
    msg.attach(MIMEText(body, 'plain'))
    server = smtplib.SMTP('smtp.chewymail.com', 25)
    text = msg.as_string()
    server.sendmail(fromaddr,toaddr.split(','), text)
    logger.info("Email was sent to the recipients: %s" %(toaddr))
    server.quit()
    print("Email was sent to the recipients: %s" %(toaddr))
except Exception as e:
    print("Error Reported")
    logger.error("Error in the code: "+str(e))
    fromaddr = 'scsystems@chewy.com'
    toaddr = 'vmanohar@chewy.com,igonzalez1@chewy.com,EAlfonso@chewy.com,jxie@chewy.com'
    to = ', '.join(toaddr)
    msg = MIMEMultipart()
    msg['From'] = fromaddr
    msg['To'] = toaddr
    msg['Subject'] = "CSV Template Process did not run" 
    body = "Hello, The CSV template process for FC not on algorithm failed for the following reason: "+str(e)+"\nThanks"
    msg.attach(MIMEText(body, 'plain'))
    server = smtplib.SMTP('smtp.chewymail.com', 25)
    text = msg.as_string()
    server.sendmail(fromaddr,toaddr.split(','), text)
    logger.info("Email was sent to the recipients: %s" %(toaddr))
    server.quit()
    print("Email was sent to the recipients: %s" %(toaddr))
    cxn.close()
    logger.info("Vertica is Disconnected")
    
