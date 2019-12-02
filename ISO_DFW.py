# -*- coding: utf-8 -*-
"""
Created on Tue Oct  8 14:19:54 2019

@author: AA-VManohar
"""


try:
    import logging
    import logging.handlers
    import csv
    import pandas as pd
    import pyodbc
    import datetime as dtm
    from gurobipy import *
    import time
    import smtplib
    from email.mime.multipart import MIMEMultipart
    from email.mime.text import MIMEText
    from email.mime.base import MIMEBase
    from email import encoders
    start_time = time.time()
    #inputs
    out_1 = {}
    out_2 = {}
    out_3 = {}
    out_copy = {}
    missed_ref =[]
    infeas_shift ={}
    infeasible_day = {}
    infeas_ref = []
    date_fl = {}
    #a = 1.10
    objec = {}
    M1 = 0
    M2 = 0
    sch_sh_check = {}
    TODAY = dtm.datetime.today()
    exp_units = {}
    exp_sku = {}
    slot_count = {}
    logger = logging.getLogger('DFW_run')
    logger.setLevel(logging.DEBUG)
    rh = logging.handlers.RotatingFileHandler('ISO_process.log',maxBytes = 500*1024,backupCount = 1)
    rh.setLevel(logging.DEBUG)
    ch = logging.StreamHandler()
    ch.setLevel(logging.DEBUG)
    formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(name)s - %(message)s')
    rh.setFormatter(formatter)
    ch.setFormatter(formatter)
    if (logger.hasHandlers()):
        logger.handlers.clear()
    logger.addHandler(rh)
    logger.addHandler(ch)
    today = dtm.datetime.today().date()
    for i in range(1,108):
        a = today + dtm.timedelta(days = i-15)
        if a.weekday() == 4:
            date_fl[str(a)] = '1'
        elif a.weekday() in [0,1,2,3]:
            date_fl[str(a)] = '0'
        else:
            pass
        
    #giving slots 
    day_slots={'0': ['05:30:00','06:00:00','06:30:00','07:30:00','08:00:00','08:30:00','09:00:00','09:30:00','10:00:00','11:00:00'],
               '1':['05:30:00','06:00:00','06:30:00','07:00:00','07:30:00','08:00:00','08:30:00','09:00:00','10:00:00','11:00:00','12:00:00','12:30:00','13:00:00','13:30:00','14:00:00','15:00:00','15:30:00','16:00:00']}
    night_slots={'0':['16:00:00','16:30:00','17:00:00','17:30:00','19:00:00','19:30:00','20:00:00','20:30:00'],
                 '1':[]}
    cxn = pyodbc.connect("DSN=BIDB",autocommit = True)
    cur = cxn.cursor()
    logger.info("Vertica is Connected")
    std_no = {
    ('9011','16:30:00') : '271',
    ('9000','06:30:00') : '266',
    ('9000','17:00:00') : '376',
    ('9282','06:30:00') : '329',
    ('9282','09:00:00') : '325',
    ('9283','08:00:00') : '327',
    ('9283','09:00:00') : '372',
    ('9285','07:30:00') : '273',
    ('000000012','09:00:00') : '640',
    ('00000002','22:45:00') : '369',
    ('00000004','10:00:00') : '602',
    ('00000007','21:45:00') : '368'
    }
    #Sch_units, Sch_SKU and Sch_appt at day level
    query = """
    SELECT apl.request_date:: DATE, SUM(pdp.qty) AS IB_units, COUNT(pdp.item_number) AS SKU, COUNT(DISTINCT apl.appointment_id) AS slots
    FROM aad.t_appt_appointment_log AS apl
    JOIN aad.t_appt_appointment_log_po AS pol
    USING(appointment_id)
    JOIN aad.t_po_detail AS pdp
    ON pdp.po_number = pol.po_number
    WHERE apl.wh_id = 'DFW1' AND LOWER(apl.status) <> 'cancelled' AND apl.request_date:: DATE > current_date and dayofweek(apl.request_date:: DATE) NOT IN (6,7)
    GROUP BY 1
    ORDER BY 1
    """
    df = pd.read_sql(query,cxn)
    df.columns = ['date','U','S','Sl']
    sch_dict = dict([str(i),[int(j),int(k),int(l)]] for i,j,k,l in zip(df.date,df.U,df.S,df.Sl))
    #Sch_appt at slot level
    query = """
    SELECT apl.request_date:: DATE, apl.request_time:: TIME,COUNT(DISTINCT apl.appointment_id) AS slots
    FROM aad.t_appt_appointment_log AS apl
    JOIN aad.t_appt_appointment_log_po AS pol
    USING(appointment_id)
    WHERE apl.wh_id = 'DFW1' AND LOWER(apl.status) <> 'cancelled' AND apl.request_date:: DATE > current_date and dayofweek(apl.request_date:: DATE) NOT IN (6,7)
    GROUP BY 1,2
    ORDER BY 1,2
    """
    df = pd.read_sql(query,cxn)
    df.columns = ['dt','t','s']
    sch_slot = dict([(str(i),str(j)),int(k)] for i,j,k in zip(df.dt,df.t,df.s))
    
    #Sch_units, Sch_SKU and Sch_appt at day and shift level
    query = """
    SELECT apl.request_date:: DATE,CASE WHEN apl.request_time:: TIME BETWEEN '04:00:00' AND '14:30:00' THEN 1 ELSE 2 END AS shift,  SUM(pdp.qty) AS IB_units, COUNT(DISTINCT pdp.item_number) AS SKU, COUNT(DISTINCT apl.appointment_id) AS slots
    FROM aad.t_appt_appointment_log AS apl
    JOIN aad.t_appt_appointment_log_po AS pol
    USING(appointment_id)
    JOIN aad.t_po_detail AS pdp
    ON pdp.po_number = pol.po_number
    WHERE apl.wh_id = 'DFW1' AND LOWER(apl.status) <> 'cancelled' AND apl.request_date:: DATE > current_date and dayofweek(apl.request_date:: DATE) NOT IN (6,7)
    GROUP BY 1,2
    ORDER BY 1,2
    """
    df = pd.read_sql(query,cxn)
    df.columns = ['dt','sh','u','s','sl']
    sch_sh = dict([(str(i),str(j)),[int(k),int(l),int(m)]] for i,j,k,l,m in zip(df.dt,df.sh,df.u,df.s,df.sl))
    logger.info("HJ data are collected")
    
    #Getting S&OP forecast
    query = """
    with FC_max_date as
    (
    select distinct date::date,wh_id,max(scrape_update_dttm) as max_date 
    from sandbox_fulfillment.t_labor_model_inbound_forward_looking_capacity_new 
    --where date::date = scrape_update_dttm::date + 14 --rolling 14 day lock
    --date_trunc('week',date::date+1)-1 = timestampadd('week',2,date_trunc('week',scrape_update_dttm+1)-1) --2 week lock
    group by 1,2
    )
    select iblm.wh_id,iblm.scrape_update_dttm,iblm.date::date as date,
    ROUND(abs(iblm.planned_operations_units_received),0) AS planned_operations_units_received ,
    ROUND(abs(iblm.planned_operations_units_received),0)-ROUND(abs(iblm.planned_units_received_nights),0) as planned_units_received_days,
    ROUND(abs(iblm.planned_units_received_nights),0) AS planned_units_received_nights 
    from sandbox_fulfillment.t_labor_model_inbound_forward_looking_capacity_new iblm
    join FC_max_date fmd on iblm.scrape_update_dttm = fmd.max_date and iblm.wh_id = fmd.wh_id and iblm.date::date = fmd.date
    where iblm.date::date >= current_date AND iblm.wh_id = 'DFW1'
    order by wh_id, date;
    """
    df = pd.read_sql(query,cxn)
    df.columns = ['fc_nm','update_dttm','date','units','day_units','night_units']
    f = dict([str(i),[float(j),float(k)]] for i,j,k in zip(df.date,df.day_units,df.night_units))
    
# =============================================================================
#     query = """
#     SELECT common_date_dttm, forecast_percent
#     FROM sandbox_supply_chain.daily_inbound_forecast_percent
#     WHERE common_date_dttm between current_date-7 and current_date+123 AND location = 'DFW1'
#     ORDER BY 1
#     """
#     #cur.execute(query)
#     #result = cur.fetchall()
#     #df = pd.DataFrame(data = result)
#     df = pd.read_sql(query,cxn)
#     df.columns = ['date','units']
#     temp_perc = dict([i,float(j)] for i,j in zip(df.date,df.units))
#     f = {}
#     f_sch = {}
#     for i in temp_f.keys():
#         k = 0
#         while k < 5:
#             a = i + dtm.timedelta(days = k+1)
#             if a in temp_perc:
#                 f[str(a)] = temp_perc[a] * temp_f[i]
#             else:
#                 f[str(a)] = 0.2* temp_f[i]
#             k = k+1
# =============================================================================
    logger.info("Forecast data is collected") 

  
    cnt = 1
    for i in range(1,108):
        a = today + dtm.timedelta(days = i-1)
        if 1 <= cnt <= 3:
            if str(a) in f:
                f[str(a)][0] = 1 * f[str(a)][0]
                f[str(a)][1] = 1 * f[str(a)][1]
                cnt = cnt + 1
            else:
                pass
        
        elif 4 <= cnt <= 6:
            if str(a) in f:
                f[str(a)][0] = 0.9 * f[str(a)][0]
                f[str(a)][1] = 0.9 * f[str(a)][1]
                cnt = cnt + 1
            else:
                pass
        elif 7 <= cnt <= 9:
            if str(a) in f:
                f[str(a)][0] = 0.8 * f[str(a)][0]
                f[str(a)][1] = 0.8 * f[str(a)][1]
                cnt = cnt + 1
            else:
                pass
        elif 10 <= cnt <= 12:
            if str(a) in f:
                f[str(a)][0] = 0.7 * f[str(a)][0]
                f[str(a)][1] = 0.7 * f[str(a)][1]
                cnt = cnt + 1
            else:
                pass
        elif 13 <= cnt <= 15:
            if str(a) in f:
                f[str(a)][0] = 0.6 * f[str(a)][0]
                f[str(a)][1] = 0.6 * f[str(a)][1]
                cnt = cnt + 1
            else:
                pass
        elif 16 <= cnt <= 18:
            if str(a) in f:
                f[str(a)][0] = 0.5 * f[str(a)][0]
                f[str(a)][1] = 0.5 * f[str(a)][1]
                cnt = cnt + 1
            else:
                pass
        else:
            if str(a) in f:
                f[str(a)][0] = 0.4 * f[str(a)][0]
                f[str(a)][1] = 0.4 * f[str(a)][1]
            else:
                pass
    logger.info("Added Dynamic weights to the S&OP forecast")
     
    #getting vas_units
    query = """
    SELECT apl.request_date:: DATE, sum(pdp.qty)
    FROM aad.t_appt_appointment_log AS apl
    JOIN aad.t_appt_appointment_log_po AS pol
    USING(appointment_id)
    JOIN aad.t_po_detail AS pdp
    ON pol.po_number = pdp.po_number
    JOIN chewybi.products AS p
    ON pdp.item_number = p.product_part_number
    WHERE apl.wh_id = 'DFW1' AND apl.status <> 'Cancelled' AND p.product_merch_classification2 = 'Litter'  AND p.product_vas_profile_description IN ('SHRINKWRAP') AND apl.request_date:: DATE >= current_date and dayofweek(apl.request_date:: DATE) NOT IN (6,7)
    GROUP BY 1
    ORDER BY 1
    """
    df = pd.read_sql(query,cxn)
    df.columns = ['date','vas_units']
    sch_vas = dict([str(i),int(j)] for i,j in zip(df.date,df.vas_units))
    
    vas_dt = []
    
    for i in sch_vas.keys():
        if sch_vas[i]  > 6000:
            vas_dt.append(i)
        else:
            pass
    
    #getting vas units by shift
    query = """
    SELECT apl.request_date:: DATE,CASE WHEN apl.request_time:: TIME BETWEEN '04:00:00' AND '14:30:00' THEN 1 ELSE 2 END AS shift,  SUM(pdp.qty) AS vas_units
    FROM aad.t_appt_appointment_log AS apl
    JOIN aad.t_appt_appointment_log_po AS pol
    USING(appointment_id)
    JOIN aad.t_po_detail AS pdp
    ON pol.po_number = pdp.po_number
    JOIN chewybi.products AS p
    ON pdp.item_number = p.product_part_number
    WHERE apl.wh_id = 'DFW1' AND apl.status <> 'Cancelled' AND p.product_merch_classification2 = 'Litter'  AND p.product_vas_profile_description IN ('SHRINKWRAP') AND apl.request_date:: DATE >= current_date and dayofweek(apl.request_date:: DATE) NOT IN (6,7)
    GROUP BY 1,2
    ORDER BY 1,2
    """
    df = pd.read_sql(query,cxn)
    df.columns = ['dt','sh','vas_units']
    sch_vas_sh = dict([(str(i),str(j)),float(k)] for i,j,k in zip(df.dt,df.sh,df.vas_units))
    
    for (i,j) in sch_vas_sh.keys():
        if sch_vas_sh[(i,j)] > 3000:
            vas_dt.append((i,j))
        else:
            pass
    #getting and initializing vas slot
    query = """
    SELECT DISTINCT apl.request_date:: DATE,request_time:: TIME, CASE WHEN p.product_vas_profile_description IN ('SHRINKWRAP') THEN 1 ELSE 0 END vas_slot
    FROM aad.t_appt_appointment_log AS apl
    LEFT JOIN aad.t_appt_appointment_log_po AS pol
    USING(appointment_id)
    LEFT JOIN aad.t_po_detail AS pdp
    ON pol.po_number = pdp.po_number
    LEFT JOIN chewybi.products AS p
    ON pdp.item_number = p.product_part_number
    WHERE apl.wh_id = 'DFW1' AND apl.status <> 'Cancelled' AND p.product_merch_classification2 = 'Litter'  AND p.product_vas_profile_description IN ('SHRINKWRAP') AND apl.request_date:: DATE >= current_date and dayofweek(apl.request_date:: DATE) NOT IN (6,7)
    ORDER BY 1,2
    """
    df = pd.read_sql(query,cxn)
    df.columns = ['date','time','vas_flag']
    sch_vas_fl = dict([(str(i),str(j)),str(k)] for i,j,k in zip(df.date,df.time,df.vas_flag))
    
    temp_fl = sch_vas_fl.copy()
    for (i,j) in temp_fl.keys():
        gh = 0
        for k in sorted(day_slots[date_fl[i]]):
            if j == k and temp_fl[(i,j)] == '1' and gh != 0 and gh != len(day_slots[date_fl[i]])-1:
                w = gh-1
                if w < len(day_slots[date_fl[i]]):
                    sch_vas_fl[(i,day_slots[date_fl[i]][w])] = '2'
                w = gh+1
                if w < len(day_slots[date_fl[i]]):
                    sch_vas_fl[(i,day_slots[date_fl[i]][w])] = '2'
            elif j == k and temp_fl[(i,j)] == '1' and gh == 0:
                w = gh+1
                sch_vas_fl[(i,day_slots[date_fl[i]][w])] = '2'
            elif j == k and temp_fl[(i,j)] == '1' and gh == len(day_slots[date_fl[i]])-1:
                w = gh-1
                sch_vas_fl[(i,day_slots[date_fl[i]][w])] = '2'
            else:
                pass
            gh = gh+1
    
    for (i,j) in temp_fl.keys():
        gh = 0
        if date_fl[i] == '0':
            for k in sorted(night_slots[date_fl[i]]):
                if j == k and temp_fl[(i,j)] == '1' and gh != 0 and gh != len(night_slots[date_fl[i]])-1:
                    w = gh-1
                    if w < len(night_slots[date_fl[i]]):
                        sch_vas_fl[(i,night_slots[date_fl[i]][w])] = '2'
                    w = gh+1
                    if w < len(night_slots[date_fl[i]]):
                        sch_vas_fl[(i,night_slots[date_fl[i]][w])] = '2'
                elif j == k and temp_fl[(i,j)] == '1' and gh == 0:
                    w = gh+1
                    sch_vas_fl[(i,night_slots[date_fl[i]][w])] = '2'
                elif j == k and temp_fl[(i,j)] == '1' and gh == len(night_slots[date_fl[i]])-1:
                    w = gh-1
                    sch_vas_fl[(i,night_slots[date_fl[i]][w])] = '2'
                else:
                    pass
                gh = gh+1
        else:
            pass
    logger.info("VAS data is collected and slots are initialized")        
    #getting input data from carrier portal
    query = """
    WITH data2 AS 
                (
                 SELECT cpl.PO_no AS document_number,MAX(cpl.Ref_no) AS reference_number , MAX(cpl.VRDD:: DATE) AS requested_appt_date , MAX(cpl.Created_dt) AS created_dttm
                FROM sandbox_supply_chain.carrier_portal_new_test AS cpl
                WHERE cpl.FC_nm = 'DFW1'  AND cpl.Created_dt BETWEEN (SELECT MAX(Created_dt) FROM sandbox_supply_chain.ISO_OUTPUT_NEW WHERE FC_nm = 'DFW1') + INTERVAL '1 SECOND' AND (SELECT current_date - INTERVAL '1 SECOND')   
                AND cpl.Ref_no NOT IN (SELECT Ref_no FROM  sandbox_supply_chain.iso_exception)
                AND cpl.Ref_no <> '190922-029070'
                GROUP BY 1
                )
        ,data AS
                (
                SELECT d1.reference_number AS Ref_no, CASE WHEN DAYOFWEEK(d1.requested_appt_date) = 7 THEN d1.requested_appt_date+2 WHEN DAYOFWEEK(d1.requested_appt_date) = 1 THEN d1.requested_appt_date+1 ELSE d1.requested_appt_date END  AS VRDD1, d1.document_number AS PO_no,d1.created_dttm AS cr_dt,cpl.carrier_scac AS sc,cpl.carrier_name AS csr
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
                     CASE WHEN p1.obj < -1 AND DAYOFWEEK(d.VRDD1) <> 6 AND DAYOFWEEK(d.VRDD1-p1.obj) IN (4,5,6) AND c.cont_flag IS NULL  THEN CAST(d.VRDD1-p1.obj-2 AS DATE) 
                          WHEN p1.obj < -1 AND DAYOFWEEK(d.VRDD1) <> 6 AND DAYOFWEEK(d.VRDD1-p1.obj) IN (1,2,3,7) AND c.cont_flag IS NULL  THEN CAST(d.VRDD1-p1.obj-4 AS DATE) 
                          WHEN p1.obj < -1 AND DAYOFWEEK(d.VRDD1) = 6 AND DAYOFWEEK(d.VRDD1-p1.obj) IN (4,5,6) AND c.cont_flag IS NULL  THEN CAST(d.VRDD1-p1.obj-2 AS DATE) 
                          WHEN p1.obj < -1 AND DAYOFWEEK(d.VRDD1) = 6 AND DAYOFWEEK(d.VRDD1-p1.obj) IN (1,3,7) AND c.cont_flag IS NULL  THEN CAST(d.VRDD1-p1.obj-4 AS DATE) 
                          WHEN p1.obj < -1 AND DAYOFWEEK(d.VRDD1) = 6 AND c.cont_flag IS NULL AND DAYOFWEEK(d.VRDD1-p1.obj) IN (2) THEN CAST(d.VRDD1+4 AS DATE)
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
    """
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
    logger.info("Getting Carrier Portal Data")
    date = str(dtm.datetime.today().date())
    dt = {}
    for i in dt1.keys():
        cnt = 0 
        for j in dt1[i]:
            if j < date:
                pass
            else:
                if i in dt:
                    dt[i].append(j)
                else:
                    dt[i] = [j]
            
    #po_number
    query = """
            SELECT cpl.Ref_no,pdp.po_number, SUM(pdp.qty), COUNT(DISTINCT pdp.item_number)
            FROM sandbox_supply_chain.carrier_portal_new_test AS cpl
            JOIN aad.t_po_detail AS pdp
            ON cpl.PO_no = pdp.po_number
            WHERE cpl.FC_nm = 'DFW1' AND UPPER(cpl.request_type) LIKE 'CREATE%' AND cpl.VRDD:: DATE >= '20190701' AND cpl.Created_dt BETWEEN (SELECT MAX(Created_dt) FROM sandbox_supply_chain.ISO_OUTPUT_NEW WHERE FC_nm = 'DFW1') + INTERVAL '1 SECOND' AND (SELECT current_date - INTERVAL '1 SECOND')  
            GROUP BY 1,2
    """
    df = pd.read_sql(query,cxn)
    df.columns = ['ref','po','units','sku']
    ref_num = {str(k):g['po'].unique().tolist()for k,g in df.groupby('ref')}
    po = dict([(str(i),str(j)),[int(k),int(l)]] for i,j,k,l in zip(df.ref,df.po,df.units,df.sku))
    
    query = """
            SELECT cpl.Incident_No,cpl.Ref_no,pdp.document_number,ISNULL(pdp.document_original_requested_delivery_dttm:: DATE,'1900-01-01')
            FROM sandbox_supply_chain.carrier_portal_new_test AS cpl
            JOIN chewybi.procurement_document_measures AS pdp
            ON cpl.PO_no = pdp.document_number
            WHERE cpl.FC_nm = 'DFW1' AND UPPER(cpl.request_type) LIKE 'CREATE%' AND cpl.VRDD:: DATE >= '20190701' AND  cpl.Created_dt BETWEEN (SELECT MAX(Created_dt) FROM sandbox_supply_chain.ISO_OUTPUT_NEW WHERE FC_nm = 'DFW1') + INTERVAL '1 SECOND' AND (SELECT current_date - INTERVAL '1 SECOND') 
            ORDER BY 1,2
    """
    df = pd.read_sql(query,cxn)
    df.columns = ['inc','ref','po','ordd']
    ordd = dict([str(i),str(j)] for i,j in zip(df.po,df.ordd))
    inc = dict([str(i),str(j)] for i,j in zip(df.ref,df.inc))
    
    logger.info("Getting PO details in terms of units,sku and ORDD")
    #cont_appt_scheduled
    query = """
    SELECT apl.request_date:: DATE, COUNT(DISTINCT apl.appointment_id)
    FROM aad.t_appt_appointment_log AS apl
    JOIN aad.t_appt_appointment_log_po AS pol
    ON apl.appointment_id = pol.appointment_id
    JOIN chewybi.procurement_document_measures AS pdm
    ON pol.po_number = pdm.document_number
    JOIN chewybi.vendors AS v
    ON pdm.vendor_key = v.vendor_key
    WHERE apl.wh_id = 'DFW1' AND LOWER(apl.status) <> 'cancelled' AND apl.request_date:: DATE >= '2019-07-01' AND v.vendor_number IN ('P000533','B000050','1760','9295','9302','P000544','P000508','P000486','P000400','7701','P000398','B000064','P000421','P000476','3755','3722','8038','5223')
    GROUP BY 1
    ORDER BY 1
    """
    df = pd.read_sql(query,cxn)
    if df.empty == False:    
        df.columns = ['date','cnt']
        cont_appt = dict([str(i),2-int(j)] for i,j in zip(df.date,df.cnt))
    else:
        cont_appt = {}
    logger.info("Getting Container appointments data")
    #reschedules
    query = """
    WITH data AS (SELECT cpl.Ref_no, cpl.VRDD:: DATE, cpl.Created_dt:: DATE,pdm.document_number
            FROM chewybi.procurement_document_measures AS pdm
            JOIN sandbox_supply_chain.carrier_portal_new_test AS cpl
            ON cpl.PO_no = pdm.document_number
            WHERE cpl.FC_nm = 'DFW1' AND cpl.Ref_no NOT IN (SELECT Ref_no FROM  sandbox_supply_chain.iso_exception) AND cpl.Created_dt BETWEEN (SELECT MAX(Created_dt) FROM sandbox_supply_chain.ISO_OUTPUT_NEW WHERE FC_nm = 'DFW1') + INTERVAL '1 SECOND' AND (SELECT current_date - INTERVAL '1 SECOND') 
    )
    SELECT d.Ref_no,apl.appointment_id,apl.request_date:: DATE, request_time:: TIME, d.document_number
    FROM data AS d
    JOIN aad.t_appt_appointment_log_po AS pol
    ON d.document_number = pol.po_number
    JOIN aad.t_appt_appointment_log AS apl
    ON apl.appointment_id = pol.appointment_id
    """
    df = pd.read_sql(query,cxn)
    rsch = {}
    if df.empty == False:
        df.columns = ['reference_number','appointment_id','Date','Time','PO_number']
        rsch = dict([(str(i),str(j)),[str(k),str(l)]] for i,j,k,l in zip(df.appointment_id,df.PO_number,df.Date,df.Time))
       
    else:
        pass
    logger.info("Getting Reschedule appointment data")
    
    #scheduling standing appointment
    query = """
    SELECT apl.request_date:: DATE, apl.request_time:: TIME, apl.vendor
    FROM aad.t_appt_appointment_log AS apl
    LEFT JOIN aad.t_appt_appointment_log AS pol
    USING(appointment_id)
    WHERE apl.request_date:: DATE BETWEEN current_date+1 AND current_date+60 AND LOWER(apl.status) <> 'cancelled' AND apl.standing_appt_id IS NOT NULL AND apl.vendor IS NOT NULL AND apl.wh_id = 'DFW1' AND pol.po_number IS NULL AND dayofweek(apl.request_date) NOT IN (1,7)
    ORDER BY 1,2
    """
    df = pd.read_sql(query,cxn)
    df.columns = ['dt','tm','vendor']
    stnd_date = {str(k):g['dt'].unique().tolist() for k,g in df.groupby('vendor')}
    stnd_time = {str(k):g['tm'].unique().tolist()for k,g in df.groupby('vendor')}
    stnd_fl = dict([(str(i),str(j),str(k)),'0'] for i,j,k in zip(df.dt,df.tm,df.vendor))
    
    for i in stnd_date.keys():
        for j in range(1,len(stnd_date[i])+1):
            stnd_date[i][j-1] = str(stnd_date[i][j-1])
        for k in range(1,len(stnd_time[i])+1):
            stnd_time[i][k-1] = str(stnd_time[i][k-1])
    
    #standing appointment_vendor_units       
    query = """
    with standings_history as (
    select distinct poq."Location Code",poq.appt_date,poq.No_,poq.standing_appt_id,poq."Buy-from Vendor No_",poq.appt_quantity_fill,sum(pdm.document_receipt_hj_quantity) as received_units,count(pdm.product_part_number) as SKU_count
    from sandbox_supply_chain.scheduled_po_quantity poq 
    left join chewybi.procurement_document_product_measures pdm on pdm.document_number = poq.No_ and poq.appt_date::date = pdm.appointment_dttm::date
    where poq.appt_date >= timestampadd('month',-3,current_date)
    and standing_appt_id is not null
    group by 1,2,3,4,5,6)
    
    ,standings_schedule as (
    select distinct wh_id, standing_appt_id, request_time:: TIME AS scheduled_time, date_part('dow',request_date) as dow, vendor, vendor_name, units_expected
    from aad.t_appt_appointment_log aal left join chewybi.vendors v on aal.vendor = v.vendor_number
    where standing_appt_id is not null and units_expected is not null and request_date::date > current_date)
    
    ,final AS (
    select ss.wh_id,
    ss.dow,
    ss.scheduled_time,
    ss.standing_appt_id,
    vendor,
    --case vendor
    --when '00000004' then 'SAIA LTL'
    --when '00000007' then 'YRC LTL'
    --when '00000002' then 'UPS LTL'
    --when '000000012' then 'ESTES LTL'
    --when '9000' then 'FEDEX LTL'
    --else vendor_name end as vendor_or_carrier,
    ss.units_expected as units_expected,
    sum(sh.appt_quantity_fill)/count(distinct sh.appt_date::date) as scheduled_units_per_day,
    isnull(sum(sh.received_units)/nullif(sum(sh.SKU_count),0),0) as UPT
    from standings_schedule ss 
    join standings_history sh 
    on ss.wh_id = sh."Location Code" 
    and ss.dow = date_part('dow',sh.appt_date) 
    and ss.standing_appt_id = sh.standing_appt_id
    where vendor not in ('AVP1','CFC1','DAY1','DFW1','EFC3','MCO1','PHX1','WFC2') and ss.wh_id = 'DFW1'
    group by 1,2,3,4,5,6
    order by 1,4,2
    )
    SELECT vendor, CASE WHEN dow <> 5 AND   scheduled_time BETWEEN '04:00:00' AND '15:30:00' THEN 1 WHEN dow = 5 AND scheduled_time BETWEEN '04:00:00' AND '16:00:00' THEN 1 ELSE 2 END AS shift, ROUND(AVG(scheduled_units_per_day),0) AS Units, ROUND((AVG(scheduled_units_per_day) /AVG(UPT)),0) AS SKU
    FROM final
    GROUP BY 1,2
    """
    df = pd.read_sql(query,cxn)
    df.columns = ['y','x','r','t']
    v_units = dict([(str(i),str(j)),[float(k),float(l)]] for i,j,k,l in zip(df.y,df.x,df.r,df.t))
    logger.info("Getting Standing Appointment Data")
    query = """
    WITH break AS 
            (
           SELECT apl.appointment_id,apl.request_date:: DATE, apl.request_time:: TIME,COUNT(DISTINCT pol.po_number) AS po_count, (SUM(pdp.qty)/COUNT(DISTINCT pdp.item_number)) AS hj_rank
           FROM aad.t_appt_appointment_log AS apl
           JOIN aad.t_appt_appointment_log_po AS pol
           USING(appointment_id)
           JOIN aad.t_po_detail AS pdp
           ON pol.po_number = pdp.po_number
           WHERE apl.status <> 'Cancelled' AND apl.request_date:: DATE BETWEEN current_date AND current_date+60 AND apl.wh_id = 'DFW1'
           GROUP BY 1,2,3
           ORDER BY 1,2
           )
    ,break_final AS 
           ( 
            SELECT appointment_id,request_date,request_time, CASE WHEN po_count > 1 or hj_rank < 35 THEN 1 ELSE 0 END AS bulk_or_breakdown
            FROM break
            ORDER BY 2,3
           )
    SELECT request_date,request_time,bulk_or_breakdown
    FROM break_final
    WHERE bulk_or_breakdown = 1
    ORDER BY 1,2
    """
    df = pd.read_sql(query,cxn)
    df.columns = ['Dt','tm','bi']
    temp_bulk = dict([(str(i),str(j)),str(k)] for i,j,k in zip(df.Dt,df.tm,df.bi))
    logger.info("Initializing breakdown slots")
    
    ref = {}
    units = {}
    slot = {}
    #scheduling stand_appointments
    for j in dt.keys():
        for k in dt[j]:
            if k in ref:
                ref[k].append(j)
            else:
                ref[k] = [j]
    
    for j in ref.keys():
        if j in sch_dict:
            pass
            #sch_dict[j] = [0,0,0]
        else:
            sch_dict[j] = [0,0,0]
    for j in sch_dict.keys():
        for k in range(1,3):
            if (j,str(k)) in sch_sh:
                pass
                #sch_sh[(j,str(k))] = [0,0,0]
            else:
                sch_sh[(j,str(k))] = [0,0,0]
    for j in sch_dict.keys():
        for k in day_slots[date_fl[j]]:
            if (j,k) in sch_slot:
                pass
                #sch_slot[(j,k)] = 0
            else:
                sch_slot[(j,k)] = 0
        for k in night_slots[date_fl[j]]:
            if (j,k) in sch_slot:
                pass
                #sch_slot[(j,k)] = 0
            else:
                sch_slot[(j,k)] = 0
    
    for j in ref.keys():
        if j in sch_vas:
            pass
            #sch_vas[j] = 0
        else:
            sch_vas[j] = 0
              
    for j in sch_vas.keys():
        for k in range(1,3):
            if (j,str(k)) in sch_vas_sh:
                pass
                #sch_sh[(j,str(k))] = [0,0,0]
            else:
                sch_vas_sh[(j,str(k))] = 0
    
    for j in ref.keys():
        for k in day_slots[date_fl[j]]:
            if (j,k) in sch_vas_fl:
                pass
                #sch_vas_fl[(j,k)] = '0'
            else:
                sch_vas_fl[(j,k)] = '0'
        for k in night_slots[date_fl[j]]:
            if (j,k) in sch_vas_fl:
                pass
                #sch_vas_fl[(j,k)] = '0'
            else:
                sch_vas_fl[(j,k)] = '0'
    
    for j in ref.keys():
        if j in cont_appt:
            pass
        else:
            cont_appt[j] = 4
    for (i,j) in sch_sh.keys():
        if j == '1':
            slot_count[(i,j)] = len(day_slots[date_fl[i]])*2 + 4 
        elif j == '2' and date_fl[i] == '0':
            slot_count[(i,j)] = len(night_slots[date_fl[i]])*2 + 4
        else:
            slot_count[(i,j)] = 0
    for j in cont_appt.keys():
        if cont_appt[j]  < 0:
            cont_appt[j] = 0
        else:
            pass
    logger.info("Initializing Container appointment slots") 
    for j in cont_appt.keys():
        if cont_appt[j] == 0:
            sch_slot[(j,'05:00:00','c')] = 1
            sch_slot[(j,'08:00:00','c')] = 1
            sch_slot[(j,'15:00:00','c')] = 1
            sch_slot[(j,'19:00:00','c')] = 1
        elif cont_appt[j] == 1:
            sch_slot[(j,'05:00:00','c')] = 1
            sch_slot[(j,'08:00:00','c')] = 1
            sch_slot[(j,'15:00:00','c')] = 1
            sch_slot[(j,'19:00:00','c')] = 0
            if (j,'19:00:00') in sch_slot:
                sch_slot[(j,'19:00:00')] = sch_slot[(j,'19:00:00')] + 1
            else:
                sch_slot[(j,'19:00:00')] = 1
        elif cont_appt[j] == 2:
            sch_slot[(j,'05:00:00','c')] = 1
            sch_slot[(j,'08:00:00','c')] = 0
            sch_slot[(j,'15:00:00','c')] = 1
            sch_slot[(j,'19:00:00','c')] = 0
            if (j,'19:00:00') in sch_slot:
                sch_slot[(j,'19:00:00')] = sch_slot[(j,'19:00:00')] + 1
            else:
                sch_slot[(j,'19:00:00')] = 1
            if (j,'08:00:00') in sch_slot:
                sch_slot[(j,'08:00:00')] = sch_slot[(j,'08:00:00')] + 1
            else:
                sch_slot[(j,'08:00:00')] = 1
        elif cont_appt[j] == 3:
            sch_slot[(j,'05:00:00','c')] = 1
            sch_slot[(j,'08:00:00','c')] = 0
            sch_slot[(j,'15:00:00','c')] = 0
            sch_slot[(j,'19:00:00','c')] = 0
            if (j,'19:00:00') in sch_slot:
                sch_slot[(j,'19:00:00')] = sch_slot[(j,'19:00:00')] + 1
            else:
                sch_slot[(j,'19:00:00')] = 1
            if (j,'08:00:00') in sch_slot:
                sch_slot[(j,'08:00:00')] = sch_slot[(j,'08:00:00')] + 1
            else:
                sch_slot[(j,'08:00:00')] = 1
        else:
            sch_slot[(j,'05:00:00','c')] = 0
            sch_slot[(j,'08:00:00','c')] = 0
            sch_slot[(j,'15:00:00','c')] = 0
            sch_slot[(j,'19:00:00','c')] = 0
            if (j,'19:00:00') in sch_slot:
                sch_slot[(j,'19:00:00')] = sch_slot[(j,'19:00:00')] + 1
            else:
                sch_slot[(j,'19:00:00')] = 1
            if (j,'08:00:00') in sch_slot:
                sch_slot[(j,'08:00:00')] = sch_slot[(j,'08:00:00')] + 1
            else:
                sch_slot[(j,'08:00:00')] = 1
            
    for j in units_sku_obj.keys():
        if j in dt.keys():
            for k in range(1,len(dt[j])+1):
                if j in objec:
                    objec[j].append(abs(units_sku_obj[j][2])+k-1)
                else:
                    objec[j] = [abs(units_sku_obj[j][2])+k-1]
        
    logger.info("Starting to schedule standing appointments")        
    for j in sch_dict.keys():
        units[j] = sch_dict[j][0]
        slot[j] = sch_dict[j][2]
    
    #calculating expected units
    for (i,j,k) in stnd_fl.keys():
        if stnd_fl[(i,j,k)] == '0':
            if j in day_slots[date_fl[i]]:
                if(k,'1') in v_units:
                    if (i,'1') in exp_units:
                        exp_units[(i,'1')] = exp_units[(i,'1')] + v_units[(k,'1')][0]
                        print(exp_units[(i,'1')])
                    else:
                         exp_units[(i,'1')] = v_units[(k,'1')][0]
                         print(exp_units[(i,'1')])
                else:
                    pass
            else:
                if(k,'2') in v_units:
                    if (i,'2') in exp_units:
                        exp_units[(i,'2')] = exp_units[(i,'2')] + v_units[(k,'2')][0]
                    else:
                         exp_units[(i,'2')] = v_units[(k,'2')][0]
                else:
                    pass
                
        else:
            pass        
    #required data structures for building model
    
    for i in ref.keys():
        for j in range(1,3):
            if (i,str(j)) in exp_units:
                pass
            else:
                exp_units[(i,str(j))] = 0
    
    std_ref = {}
    std_sh = {}
    stnd_ref = []
    stnd_ref2 = []
    stnd_ref3 = []
    logger.info("Starting to schedule standing appointments")
    for j in dt.keys():
        p = 0
        if vendor[j] in stnd_date.keys():
            for k in dt[j]:
                if k in stnd_date[vendor[j]]:
                    cnt = 1
                    for l in stnd_time[vendor[j]]:
                        if (k,l,vendor[j]) in stnd_fl.keys():
                            cnt = cnt+1
                            if stnd_fl[(k,l,vendor[j])] == '0' and p == 0 and slot[k] < (slot_count[(k,'1')] + slot_count[(k,'2')]) and units[k] < 1.05 * (f[k][0]+f[k][1]):
                                if l in ['06:30:00','08:00:00','09:00:00','10:00:00'] and sch_sh[(k,'1')][2] < slot_count[(k,'1')] and date_fl[k] == '0' and (vendor[j],'1') in v_units: 
                                    if (k,l) in out_3:
                                        out_3[(k,l)].append(j)
                                    else:
                                        out_3[(k,l)] = [j]
                                    stnd_fl[(k,l,vendor[j])] = '1'
                                    p = 1
                                    exp_units[(k,'1')] = exp_units[(k,'1')] - v_units[(vendor[j],'1')][0]
                                elif l in ['16:00:00','16:30:00','17:00:00','21:45:00','22:45:00'] and sch_sh[(k,'2')][2] < slot_count[(k,'2')] and date_fl[k] == '0' and (vendor[j],'2') in v_units:
                                    if (k,l) in out_3:
                                        out_3[(k,l)].append(j)
                                    else:
                                        out_3[(k,l)] = [j]
                                    stnd_fl[(k,l,vendor[j])] = '1'
                                    p = 1
                                    exp_units[(k,'2')] = exp_units[(k,'2')] - v_units[(vendor[j],'2')][0]
                                elif l in ['06:30:00','07:30:00','09:00:00','10:00:00','16:30:00','21:45:00','22:45:00'] and sch_sh[(k,'1')][2] < slot_count[(k,'1')] and date_fl[k] == '1' and (vendor[j],'1') in v_units:
                                    if (k,l) in out_3:
                                        out_3[(k,l)].append(j)
                                    else:
                                        out_3[(k,l)] = [j]
                                    stnd_fl[(k,l,vendor[j])] = '1'
                                    p = 1
                                    exp_units[(k,'1')] = exp_units[(k,'1')] - v_units[(vendor[j],'1')][0]
                                else:
                                    pass
                                if p == 1:
                                    stnd_ref.append(j)
                                    slot[k] = slot[k]+1
                                    units[k] = units[k] + units_sku_obj[j][0]
                                    if (k,l) in sch_slot:
                                        sch_slot[(k,l)] = sch_slot[(k,l)] + 1
                                    else:
                                        sch_slot[(k,l)] = 1
                                    if k in std_ref:
                                        std_ref[k].append(j)
                                    else:
                                        std_ref[k] = [j]
                                    if l in day_slots[date_fl[k]]:
                                        if (k,1) in std_sh:
                                            std_sh[(k,1)].append(j)
                                        else:
                                            std_sh[(k,1)] = [j]
                                    else:
                                        if (k,2) in std_sh:
                                            std_sh[(k,2)].append(j)
                                        else:
                                            std_sh[(k,2)] = [j]
                                    if (k,l,'B000046') in stnd_fl.keys():
                                        if stnd_fl[(k,l,'B000046')] == '1':
                                            sch_vas_fl[(k,l)] = '1'
                                            gh = 0
                                            for d in sorted(day_slots[date_fl[k]]):
                                                if d == l and gh != 0 and gh != len(day_slots[date_fl[k]])-1:
                                                    w = gh-1
                                                    if w < len(day_slots[date_fl[k]]):
                                                        sch_vas_fl[(k,day_slots[date_fl[k]][w])] = '2'
                                                    w = gh+1
                                                    if w < len(day_slots[date_fl[k]]):
                                                        sch_vas_fl[(k,day_slots[date_fl[k]][w])] = '2'
                                                gh = gh+1
                                            
                                            gh = 0
                                            for d in sorted(night_slots[date_fl[k]]):
                                                if d == l and gh != 0 and gh != len(night_slots[date_fl[k]])-1:
                                                    w = gh-1
                                                    if w < len(night_slots[date_fl[k]]):
                                                        sch_vas_fl[(k,night_slots[date_fl[k]][w])] = '2'
                                                    w = gh+1
                                                    if w < len(night_slots[date_fl[k]]):
                                                        sch_vas_fl[(k,night_slots[date_fl[k]][w])] = '2'
                                                gh = gh+1 
                            else:
                                if p == 0 and slot[k] + 1 < (slot_count[(k,'1')] + slot_count[(k,'2')]) -5 and units[k] + exp_units[(k,'1')] + exp_units[(k,'2')] + units_sku_obj[j][0] < 1.05 * (f[k][0]+f[k][1]) and cnt > len(stnd_time[vendor[j]]):
                                    if k in std_ref:
                                        std_ref[k].append(j)
                                    else:
                                        std_ref[k] = [j]
                                    p = 1
                                    slot[k] = slot[k] + 1
                                    units[k] = units[k] + units_sku_obj[j][0]
                                    stnd_ref3.append(j)
                        else:
                            pass
                else:
                    pass
        else:
            pass
    for i,j in out_3.keys():
        for k in out_3[(i,j)]:
            if b[k] == '1':
                temp_bulk[(i,j)] = '1'
    for i in sch_dict.keys():
        if date_fl[i] == '0':
            if (sch_dict[i][0] < 1.05 * (f[i][0]+f[i][1])) and (sch_sh[(i,'1')][2] >= len(day_slots[date_fl[i]])*2 or sch_sh[(i,'2')][2] >= len(night_slots[date_fl[i]])*2):
                slot_count[(i,'1')] = len(day_slots[date_fl[i]])*3 
                slot_count[(i,'2')] = len(night_slots[date_fl[i]])*3 
            else:
                pass
        else:
            if (sch_dict[i][0] < 1.05 * f[i]) and (sch_sh[(i,'1')][2] >= len(day_slots[date_fl[i]])*2):
                slot_count[(i,'1')] = len(day_slots[date_fl[i]])*3 
            else:
                pass
    logger.info("Standing Appointment Scheduled")
    #adding exsisting standing appointment slots
    for (i,j,k) in stnd_fl.keys():
        if stnd_fl[(i,j,k)] == '0':
            if (i,j) in sch_slot:
                sch_slot[(i,j)] = sch_slot[(i,j)]+1
                if i in sch_dict:
                    sch_dict[i][2] = sch_dict[i][2] + 1
                else:
                    sch_dict[i] = [0,0,0]
                    sch_dict[i][2] = 1
                if j in day_slots[date_fl[i]]:
                    if (i,'1') in sch_sh:
                        sch_sh[(i,'1')][2] = sch_sh[(i,'1')][2] + 1
                    else:
                        sch_sh[(i,'1')] = [0,0,0]
                        sch_sh[(i,'1')][2] = 1
                else:
                    if (i,'2') in sch_sh:
                        sch_sh[(i,'2')][2] = sch_sh[(i,'2')][2] + 1
                    else:
                        sch_sh[(i,'2')] = [0,0,0]
                        sch_sh[(i,'2')][2] = 1
            else:
                sch_slot[(i,j)] = 1
                if i in sch_dict:
                    sch_dict[i][2] = sch_dict[i][2] + 1
                else:
                    sch_dict[i] = [0,0,0]
                    sch_dict[i][2] = 1
                if j in day_slots[date_fl[i]]:
                    if (i,'1') in sch_sh:
                        sch_sh[(i,'1')][2] = sch_sh[(i,'1')][2] + 1
                    else:
                        sch_sh[(i,'1')] = [0,0,0]
                        sch_sh[(i,'1')][2] = 1
                else:
                    if (i,'2') in sch_sh:
                        sch_sh[(i,'2')][2] = sch_sh[(i,'2')][2] + 1
                    else:
                        sch_sh[(i,'2')] = [0,0,0]
                        sch_sh[(i,'2')][2] = 1
    #calculating expected units
    for (i,j,k) in stnd_fl.keys():
        if stnd_fl[(i,j,k)] == '0':
            if j in day_slots[date_fl[i]]:
                if(k,'1') in v_units:
                    if (i,'1') in exp_units:
                        exp_units[(i,'1')] = exp_units[(i,'1')] + v_units[(k,'1')][0]
                        print(exp_units[(i,'1')])
                    else:
                         exp_units[(i,'1')] = v_units[(k,'1')][0]
                         print(exp_units[(i,'1')])
                else:
                    pass
            else:
                if(k,'2') in v_units:
                    if (i,'2') in exp_units:
                        exp_units[(i,'2')] = exp_units[(i,'2')] + v_units[(k,'2')][0]
                    else:
                         exp_units[(i,'2')] = v_units[(k,'2')][0]
                else:
                    pass
                
        else:
            pass
    for (i,j) in sch_sh.keys():
        if (i,j) in exp_units:
            pass
        else:
            exp_units[(i,j)] = 0 
    
    bulk_break = {}
    for i in sch_dict.keys():
        gh = 0
        for j in day_slots[date_fl[i]]:
            if (i,j) in temp_bulk.keys():
                bulk_break[(i,j)] = '1'
                if temp_bulk[(i,j)] == '1' and gh != 0 and gh != len(day_slots[date_fl[i]])-1:
                    w = gh-1
                    if w < len(day_slots[date_fl[i]]):
                        bulk_break[(i,day_slots[date_fl[i]][w])] = '2'
                    w = gh+1
                    if w < len(day_slots[date_fl[i]]):
                        bulk_break[(i,day_slots[date_fl[i]][w])] = '2'
                elif temp_bulk[(i,j)] == '1' and gh == 0:
                    w = gh+1
                    bulk_break[(i,day_slots[date_fl[i]][w])] = '2'
                elif temp_bulk[(i,j)] == '1' and gh == len(day_slots[date_fl[i]])-1:
                    w = gh-1
                    bulk_break[(i,day_slots[date_fl[i]][w])] = '2'
                else:
                    pass
            else:
                bulk_break[(i,j)] = '0'
            gh = gh+1
        gh = 0
        for j in night_slots[date_fl[i]]:
            if (i,j) in temp_bulk.keys():
                bulk_break[(i,j)] = '1'
                if temp_bulk[(i,j)] == '1' and gh != 0 and gh != len(night_slots[date_fl[i]])-1:
                    w = gh-1
                    if w < len(night_slots[date_fl[i]]):
                        bulk_break[(i,night_slots[date_fl[i]][w])] = '2'
                    w = gh+1
                    if w < len(night_slots[date_fl[i]]):
                        bulk_break[(i,night_slots[date_fl[i]][w])] = '2'
                elif temp_bulk[(i,j)] == '1' and gh == 0:
                    w = gh+1
                    bulk_break[(i,night_slots[date_fl[i]][w])] = '2'
                elif temp_bulk[(i,j)] == '1' and gh == len(night_slots[date_fl[i]])-1:
                    w = gh-1
                    bulk_break[(i,night_slots[date_fl[i]][w])] = '2'
                else:
                    pass
            else:
                bulk_break[(i,j)] = '0'
            gh = gh+1
    for (i,j) in slot_count.keys():
        if  sch_sh[(i,j)][2] >= slot_count[(i,j)]:
            slot_count[(i,j)] = sch_sh[(i,j)][2] 
        else:
            pass
    logger.info("Building LP Model at Day level")
    # Solver Part I
    #Intializing day model
    while True:
        m1 = Model()
        #variable declaration
        x = {}
        slack = {}
        for j in dt.keys():
            if j not in infeas_ref and j not in stnd_ref2:
                for k in dt[j]:
                    x[j,k] = m1.addVar(lb=0,ub=1,vtype=GRB.BINARY,name='x[%s;%s]' %(j,k))
            m1.update()
        for j in ref.keys():
            slack[j] = m1.addVar(lb=0,ub=GRB.INFINITY,vtype=GRB.INTEGER,name='slack[%s]'%(j))
        U = m1.addVar(lb=0,ub=GRB.INFINITY,vtype=GRB.CONTINUOUS,name ='U')
        S = m1.addVar(lb=0,ub=GRB.INFINITY,vtype=GRB.CONTINUOUS,name='S')
        #objective function declaration
        o ={}
        for j in objec.keys():
            if j not in infeas_ref and j not in stnd_ref2:
                o[j] = quicksum(objec[j][k-1]*x[j,dt[j][k-1]] for k in range(1,len(objec[j])+1))
            
        m1.setObjectiveN(quicksum(o[j] for j in o.keys()),index = 0,priority = 3, name ='ORDD')
        m1.setObjectiveN(U,index = 1,priority = 1, name = 'unit_dist')
        m1.setObjectiveN(S,index = 2,priority = 1, name = 'UPT')
        m1.setObjectiveN(quicksum(100000000*slack[j] for j in slack.keys()),index = 4,priority = 3, name ='Slack Variable')
        #decalaring model sense
        m1.modelSense = GRB.MINIMIZE
        #adding constraints
        m1.update()
        cap = {}
        unit ={}
        sku = {}
        break_appt ={}
        day_assign = {}
        appt_assign = {}
        stand_appt = {}
        con_appt = {}
        temp_out1 = {}
        slack_cons = {}
        vas_cons = {}
        vas_appt_assign = {}
        for j in ref.keys():
            #capcity constraint
            cap[j] = m1.addConstr(quicksum(units_sku_obj[k][0]*x[k,j] for k in ref[j] if k not in infeas_ref and k not in stnd_ref2)-slack[j],GRB.LESS_EQUAL,(1.05*(f[j][0]+f[j][1]))-sch_dict[j][0]-exp_units[j,'1']-exp_units[j,'2'], name ='cap[%s]' %(j))
            #cap[j] = m1.addConstr(quicksum(units_sku_obj[k][0]*x[k,j] for k in ref[j] if k not in infeas_ref and k not in stnd_ref2),GRB.LESS_EQUAL,(1.05*f[j])-sch_dict[j][0]-exp_units[j,'1']-exp_units[j,'2'], name ='cap[%s]' %(j))
        #unit distribution constraint
            unit[j,1] = m1.addConstr(U,GRB.GREATER_EQUAL,(exp_units[j,'1']+exp_units[j,'2']+sch_dict[j][0]+quicksum(units_sku_obj[k][0]*x[k,j] for k in ref[j] if k not in infeas_ref and k not in stnd_ref2)-f[j][0]-f[j][1]),name = 'unit[%s;%d]' %(j,1))
            unit[j,2] = m1.addConstr(U,GRB.GREATER_EQUAL,(-exp_units[j,'1']-exp_units[j,'2']-sch_dict[j][0]-quicksum(units_sku_obj[k][0]*x[k,j] for k in ref[j] if k not in infeas_ref and k not in stnd_ref2)+f[j][0]+f[j][1]),name = 'unit[%s;%d]' %(j,2))
            #sku distribution constraint
            sku[j] = m1.addConstr(S,GRB.GREATER_EQUAL,sch_dict[j][1]+quicksum(units_sku_obj[k][1]*x[k,j] for k in ref[j] if k not in infeas_ref and k not in stnd_ref2), name = 'sku[%s]'%(j))
            #vas constraint
            vas_cons[j] = m1.addConstr(quicksum(vas_units[k] * x[k,j] for k in ref[j] if k not in infeas_ref and k not in stnd_ref),GRB.LESS_EQUAL,8000 - sch_vas[j],name='vas_day[%s]'%(j))
            #day assignment constraint
            day_assign[j] = m1.addConstr(quicksum(x[k,j] for k in ref[j] if k not in infeas_ref and k not in stnd_ref2), GRB.LESS_EQUAL,(slot_count[(j,'1')]+slot_count[(j,'2')])- sch_dict[j][2], name='day_assign[%s]' %(j))
            #container appointment constraints
            con_appt[j] = m1.addConstr(quicksum(x[k,j] for k in ref[j] if cnt_fl[k]=='3.0' and k not in infeas_ref and date_fl[j] == '0'),GRB.LESS_EQUAL,cont_appt[j],name='con_appt[%s]' %(j))
            m1.update()
        for j in dt.keys():
            if j not in infeas_ref and j not in stnd_ref2:
                appt_assign[j] = m1.addConstr(quicksum(x[j,k] for k in dt[j]), GRB.EQUAL,1, name = 'appt_assign[%s]'%(j))
            m1.update()
        for j in dt.keys():
            if j not in infeas_ref and j not in stnd_ref2 and vas_flag[j] == '1' and j not in std_ref:
                vas_appt_assign[j] = m1.addConstr(quicksum(x[j,k] for k in dt[j] if k not in vas_dt),GRB.EQUAL,1)
            m1.update()
        #standing_appointment_constraint
        for j in std_ref.keys():
            for k in std_ref[j]:
                stand_appt[(j,k)] = m1.addConstr(x[k,j],GRB.EQUAL,1,name='stand_appt[%s;%s]'%(j,k))
        m1.Params.timeLimit = 600 #declaring timelimit for running model
        m1.write('day_model.lp') #writing the day model
        m1.optimize()#Optimizing the day model
        #printing Solver Part I Solution
        if m1.status == GRB.OPTIMAL or m1.status == GRB.TIME_LIMIT:
            for j,k in x.keys():
                if x[j,k].x > 0:
                    if k in out_1:
                        out_1[k].append(j)
                    else:
                        out_1[k]= [j]
            break
        else:
            print("The day model became infeasible")
            logging.info("The day model became infeasible")
            m1.computeIIS()#computing infeasibility
            m1.write('day_model_DFW.ilp')#writing causes of infeasibility
            m1.write('day_model_failed.lp')
            M1 = M1+1
            if len(infeas_ref) + len(stnd_ref) < len(dt.keys()):
                a = max(units_sku_obj[j][0] for j in units_sku_obj.keys() if j not in stnd_ref and j not in infeas_ref)
                for j in units_sku_obj.keys():
                    if units_sku_obj[j][0] == a:
                        infeas_ref.append(j)
                    else:
                        pass
            else:
                break
    if m1.status == GRB.OPTIMAL or m1.status == GRB.TIME_LIMIT:        
        for j in infeas_ref:
            for k in ref_num[j]:
                infeasible_day[(j,k)] = [str(TODAY),'DFW1','None',str(j),str(k),dt[j][0],cr_dt[j],'NOT_OPTIMAL']
    else:
        for j in dt.keys():
            for k in ref_num[j]:
                infeasible_day[(j,k)] = [str(TODAY),'DFW1','None',str(j),str(k),dt[j][0],cr_dt[j],'NOT_OPTIMAL']
    df_day = pd.DataFrame(data = infeasible_day.values())
    logger.info("Solved LP Model at Day level")
    #Solver Part II
    #Initializing Shift Model
    logger.info("Building LP Model at Shift level")
    for j in out_1.keys():
        if date_fl[j] == '0':
            m2 = Model()
            #variable declaration and objective function declaration
            y = {}
            for k in out_1[j]:
                y[j,k,1] = m2.addVar(lb=0,ub=1,vtype=GRB.BINARY, name= 'y[%s;%s;%d]'%(j,k,1))
                y[j,k,2] = m2.addVar(lb=0,ub=1,vtype=GRB.BINARY,name='y[%s;%s;%d]'%(j,k,2))
            m2.update()
            #declaring model Sense
            US = m2.addVar(lb=0,ub=GRB.INFINITY,vtype=GRB.CONTINUOUS,name ='US')
            SS = m2.addVar(lb=0,ub=GRB.INFINITY,vtype=GRB.CONTINUOUS,name ='SS')
            m2.setObjectiveN(US,index=0,priority=1,name='unit_dist')
            m2.setObjectiveN(SS,index=1,priority=1,name='UPT')
            m2.modelSense = GRB.MINIMIZE
            #adding constraint
            shift_limit = {}
            shift_dist = {}
            shift_assign = {}
            shift_stand = {}
            unit_shift = {}
            sku_shift = {}
            day1 = {}
            night1 = {}
            day2 = {} 
            night2 ={}
            vas_shift_cons = {}
            vas_shift_appt = {}
            #day_shift Slot limitation
            shift_limit[j,1] = m2.addConstr(quicksum(y[j,k,1] for k in out_1[j]),GRB.LESS_EQUAL,slot_count[(j,'1')]-sch_sh[(j,'1')][2],name='shift_limit[%s;%d]'%(j,1))
            #night shift limitation
            shift_limit[j,2] = m2.addConstr(quicksum(y[j,k,2] for k in out_1[j]), GRB.LESS_EQUAL,slot_count[(j,'2')]-sch_sh[(j,'2')][2],name='shift_limit[%s;%d]'%(j,2))
             #vas limitation day shift
            if sch_vas_sh[j,'1'] <= 4000:
                vas_shift_cons[j,1] = m2.addConstr(quicksum(vas_units[k] * y[j,k,1] for k in out_1[j] if k not in stnd_ref),GRB.LESS_EQUAL,4000-sch_vas_sh[j,'1'],name= 'vas_shift_cons[%s;%d]' %(j,1))
            #vas limitation night shift
            if sch_vas_sh[j,'2'] <= 4000:
                vas_shift_cons[j,2] = m2.addConstr(quicksum(vas_units[k] * y[j,k,2] for k in out_1[j] if k not in stnd_ref),GRB.LESS_EQUAL,4000-sch_vas_sh[j,'2'],name= 'vas_shift_cons[%s;%d]' %(j,2))
            #data structures for model
            day1[(j,1)] = quicksum(units_sku_obj[k][0] * y[j,k,1] for k in out_1[j])
            night1[(j,1)] = quicksum(units_sku_obj[k][0] * y[j,k,2] for k in out_1[j])
            day2[(j,2)] = quicksum(units_sku_obj[k][1] * y[j,k,1] for k in out_1[j])
            night2[(j,2)] = quicksum(units_sku_obj[k][1] * y[j,k,2] for k in out_1[j])
                        
            m2.update()
            #slot assignment constraint 
            for k in out_1[j]:
                shift_assign[j,k] = m2.addConstr(quicksum(y[j,k,l] for l in range(1,3)),GRB.EQUAL,1,name='shift_assign[%s;%s]'%(j,k))
            m2.update()
            for k in out_1[j]:
                if vas_flag[k] == '1' and k not in std_ref:
                    vas_shift_appt[j,k]= m2.addConstr(quicksum(y[j,k,l] for l in range(1,3) if (j,str(l)) not in vas_dt),GRB.EQUAL,1)
            #Shift Standing appointments
            for (k,l) in std_sh.keys():
                if k == j:
                    for m in std_sh[(k,l)]:
                        shift_stand[(k,l)] = m2.addConstr(y[k,m,l],GRB.EQUAL,1,name = 'shift_stand[%s;%s]'%(k,m))
                        
                            
                        
            #container Standing appointments
            pw=0
            for k in out_1[j]:
                if cnt_fl[k]== '3.0':
                    if pw == 0 and cont_appt[j] == 0:
                        shift_stand[j,k] = m2.addConstr(y[j,k,1],GRB.EQUAL,1,name = 'shift_stand[%s;%s]'%(j,k))
                        pw = pw+1
                    else:
                        shift_stand[j,k] = m2.addConstr(y[j,k,2],GRB.EQUAL,1,name = 'shift_stand[%s;%s]'%(j,k))
            
            #day shift unit limitation
            unit_shift[j,1]= m2.addConstr(US,GRB.GREATER_EQUAL,f[j][0]-sch_sh[(j,'1')][0]-day1[(j,1)]-exp_units[(j,'1')],name='unit_shift[%s;%d]'%(j,1))
            unit_shift[j,3]= m2.addConstr(US,GRB.GREATER_EQUAL,sch_sh[(j,'1')][0]+day1[(j,1)]+exp_units[(j,'1')]-f[j][0],name='unit_shift[%s;%d]'%(j,3))
            #night shift unit limitation
            unit_shift[j,2]= m2.addConstr(US,GRB.GREATER_EQUAL,f[j][1]-sch_sh[(j,'2')][0]-night1[(j,1)]-exp_units[(j,'2')],name='unit_shift[%s;%d]'%(j,2))
            unit_shift[j,4]= m2.addConstr(US,GRB.GREATER_EQUAL,sch_sh[(j,'2')][0]+night1[(j,1)]+exp_units[(j,'2')]-f[j][1],name='unit_shift[%s;%d]'%(j,4))
            #day shift SKU limitation
            sku_shift[j,1]= m2.addConstr(SS,GRB.GREATER_EQUAL,sch_sh[(j,'1')][1]+day2[(j,2)],name='sku_shift[%s;%d]'%(j,1))
            #night shift SKU limitation
            sku_shift[j,2]= m2.addConstr(SS,GRB.GREATER_EQUAL,sch_sh[(j,'2')][1]+night2[(j,2)],name='sku_shift[%s;%d]'%(j,2))
            m2.update
            m2.Params.timeLimit = 600 #declaring timelimit for running model
            m2.write('shift_model.lp')#writing shift model
            m2.optimize()#Optimizing shift model
            #Printing Solver Part II solutions
            if m2.status == GRB.OPTIMAL or m2.status == GRB.TIME_LIMIT:
                for j,k,l in y.keys():
                    if y[j,k,l].x > 0:
                        if (j,l) in out_2:
                            out_2[(j,l)].append(k)
                        else:
                            out_2[(j,l)] = [k]
                        
            else:
                print("The shift model became infeasible")
                logging.info("The shift model became infeasible")
                m2.computeIIS()#computing infeasibility
                m2.write('shift_model_DFW.ilp')#writing causes of infeasibility
                m2.write('shift_model_failed.lp')
                m2.write('day_model_failed.lp')
                M2 = M2 +1 
                std = out_1[j]
                for k in std:
                    for l in ref_num[k]:
                        infeas_shift[(k,l)] = [str(TODAY),'DFW1','None',str(k),str(l),dt[k][0],cr_dt[k],'NOT_OPTIMAL']
        else:
            for k in out_1[j]:
                if (j,1) in out_2:
                    out_2[(j,1)].append(k)
                else:
                    out_2[(j,1)] = [k]
    df_sh = pd.DataFrame(data = infeas_shift.values())
    logger.info("Solved LP Model at Shift level")
    #scheduling time slots
    #standing_appointment_slots
    out_copy = out_2.copy()    
    st_slot = []
    
        #container_appointment_slots
        #Scheduling time slots
    logger.info("Scheduling time slots")
    M3 = 1
    for (k,j) in out_2.keys():
        while len(out_2[(k,j)]) > 0:
            a = out_2[(k,j)].pop(0)
            if a not in stnd_ref:
                p = 0
                if cnt_fl[a] == '3.0': #container appointments
                    if sch_slot[(k,'05:00:00','c')] < 1:
                        if p == 0:
                            if (k,'05:00:00') in out_3:
                                out_3[(k,'05:00:00')].append(a)
                                p = 1
                                sch_slot[(k,'05:00:00','c')] = sch_slot[(k,'05:00:00','c')] + 1
                                bulk_break[(k,'05:00:00')] = '1'
                                bulk_break[(k,'05:30:00')] = '2'
                            else:
                                out_3[k,'05:00:00'] = [a]
                                p = 1
                                sch_slot[(k,'05:00:00','c')] = sch_slot[(k,'05:00:00','c')] + 1
                                bulk_break[(k,'05:00:00')] = '1'
                                bulk_break[(k,'05:30:00')] = '2'
                    elif sch_slot[(k,'15:00:00','c')] < 1:
                        if p == 0:
                            if (k,'15:00:00') in out_3:
                                out_3[(k,'15:00:00')].append(a)
                                p = 1
                                sch_slot[(k,'15:00:00','c')] = sch_slot[(k,'15:00:00','c')] + 1
                                bulk_break[(k,'15:00:00')] = '1'
                            else:
                                out_3[k,'15:00:00'] = [a]
                                p = 1
                                sch_slot[(k,'15:00:00','c')] = sch_slot[(k,'15:00:00','c')] + 1
                                bulk_break[(k,'15:00:00')] = '1'
                    elif sch_slot[(k,'08:00:00','c')] < 1:
                        if p == 0:
                            if (k,'08:00:00') in out_3:
                                out_3[(k,'08:00:00')].append(a)
                                p = 1
                                sch_slot[(k,'08:00:00','c')] = sch_slot[(k,'08:00:00','c')] + 1
                                bulk_break[(k,'08:00:00')] = '1'
                            else:
                                out_3[k,'08:00:00'] = [a]
                                p = 1
                                sch_slot[(k,'08:00:00','c')] = sch_slot[(k,'08:00:00','c')] + 1
                                bulk_break[(k,'08:00:00')] = '1'
                    elif sch_slot[(k,'19:00:00','c')] < 1:
                        if p == 0:
                            if (k,'19:00:00') in out_3:
                                out_3[(k,'19:00:00')].append(a)
                                p = 1
                                sch_slot[(k,'19:00:00','c')] = sch_slot[(k,'19:00:00','c')] + 1
                                bulk_break[(k,'19:00:00')] = '1'
                            else:
                                out_3[k,'19:00:00'] = [a]
                                p = 1
                                sch_slot[(k,'19:00:00','c')] = sch_slot[(k,'19:00:00','c')] + 1
                                bulk_break[(k,'19:00:00')] = '1'
                    else:
                        pass
                
                elif vas_flag[a] == '1':
                    if j == 1:
                        for d in sorted(day_slots[date_fl[k]]):
                            if p == 0 and sch_vas_fl[(k,d)] == '0' and sch_slot[(k,d)] < 2 :
                                if (k,d) in out_3:
                                    out_3[(k,d)].append(a)
                                else:
                                    out_3[(k,d)] = [a]
                                p = 1
                                sch_vas_fl[(k,d)] = '1'
                            else:
                                pass
                        for d in sorted(day_slots[date_fl[k]]):
                            if p == 0 and sch_vas_fl[(k,d)] == '0' and sch_slot[(k,d)] <= 2:
                                if (k,d) in out_3:
                                    out_3[(k,d)].append(a)
                                else:
                                    out_3[(k,d)] = [a]
                                p = 1
                                sch_vas_fl[(k,d)] = '1'
                            else:
                                pass
                        for d in sorted(day_slots[date_fl[k]]):
                            if p == 0 and sch_slot[(k,d)] <= 2 and sch_vas_fl[(k,d)] == '0':
                                if (k,d) in out_3:
                                    out_3[(k,d)].append(a)
                                else:
                                    out_3[(k,d)] = [a]
                                p = 1
                                sch_vas_fl[(k,d)] = '1'
                            else: 
                                pass
                    else:
                        for d in sorted(night_slots[date_fl[k]]):
                            if p == 0 and sch_vas_fl[(k,d)] == '0' and sch_slot[(k,d)] < 2:
                                if (k,d) in out_3:
                                    out_3[(k,d)].append(a)
                                else:
                                    out_3[(k,d)] = [a]
                                p = 1
                                sch_vas_fl[(k,d)] = '1'
                            else:
                                pass
                        for d in sorted(night_slots[date_fl[k]]):
                            if p == 0 and sch_vas_fl[(k,d)] == '0' and sch_slot[(k,d)] <= 2:
                                if (k,d) in out_3:
                                    out_3[(k,d)].append(a)
                                else:
                                    out_3[(k,d)] = [a]
                                p = 1
                                sch_vas_fl[(k,d)] = '1'
                            else:
                                pass
                        for d in sorted(night_slots[date_fl[k]]):
                            if p == 0 and sch_slot[(k,d)] <= 3 and sch_vas_fl[(k,d)] == '0':
                                if (k,d) in out_3:
                                    out_3[(k,d)].append(a)
                                else:
                                    out_3[(k,d)] = [a]
                                p = 1
                                sch_vas_fl[(k,d)] = '1'
                            else: 
                                pass
                                
                    if p == 1:
                       gh = 0
                       for d in sorted(day_slots[date_fl[k]]):
                           if sch_vas_fl[(k,d)] == '1' and gh != 0 and gh != len(day_slots[date_fl[k]])-1:
                               w = gh-1
                               if w < len(day_slots[date_fl[k]]):
                                   sch_vas_fl[(k,day_slots[date_fl[k]][w])] = '2'
                               w = gh+1
                               if w < len(day_slots[date_fl[k]]):
                                   sch_vas_fl[(k,day_slots[date_fl[k]][w])] = '2'
                           elif sch_vas_fl[(k,d)] == '1' and gh == 0:
                               w = gh+1
                               sch_vas_fl[(k,day_slots[date_fl[k]][w])] = '2'
                           elif j == k and sch_vas_fl[(k,d)] == '1' and gh == len(day_slots[date_fl[k]])-1:
                               w = gh-1
                               sch_vas_fl[(k,day_slots[date_fl[k]][w])] = '2'
                           else:
                               pass
                           gh = gh+1
                       gh = 0
                       for d in sorted(night_slots[date_fl[k]]):
                           if sch_vas_fl[(k,d)] == '1' and gh != 0 and gh != len(night_slots[date_fl[k]])-1:
                               w = gh-1
                               if w < len(night_slots[date_fl[k]]):
                                   sch_vas_fl[(k,night_slots[date_fl[k]][w])] = '2'
                               w = gh+1
                               if w < len(night_slots[date_fl[k]]):
                                   sch_vas_fl[(k,night_slots[date_fl[k]][w])] = '2'
                           elif sch_vas_fl[(k,d)] == '1' and gh == 0:
                               w = gh+1
                               sch_vas_fl[(k,night_slots[date_fl[k]][w])] = '2'
                           elif sch_vas_fl[(k,d)] == '1' and gh == len(night_slots)-1:
                               w = gh-1
                               sch_vas_fl[(k,night_slots[date_fl[k]][w])] = '2'
                           else:
                               pass
                           gh = gh+1
                    elif b[a] == '1':
                        if j == 1:
                            for d in sorted(day_slots[date_fl[k]]):
                                if p == 0 and bulk_break[(k,d)] == '0' and sch_slot[(k,d)] < 2:
                                    if (k,d) in out_3:
                                        out_3[(k,d)].append(a)
                                    else:
                                        out_3[(k,d)] = [a]
                                    p = 1
                                    bulk_break[(k,d)] = '1'
                                else:
                                    pass
                            for d in sorted(day_slots[date_fl[k]]):
                                if p == 0 and bulk_break[(k,d)] == '0' and sch_slot[(k,d)] <= 2:
                                    if (k,d) in out_3:
                                        out_3[(k,d)].append(a)
                                    else:
                                        out_3[(k,d)] = [a]
                                    p = 1
                                    bulk_break[(k,d)] = '1'
                                else:
                                    pass
                            for d in sorted(day_slots[date_fl[k]]):
                                if p == 0 and bulk_break[(k,d)] == '0' and sch_slot[(k,d)] <= 2:
                                    if (k,d) in out_3:
                                        out_3[(k,d)].append(a)
                                    else:
                                        out_3[(k,d)] = [a]
                                    p = 1
                                    bulk_break[(k,d)] = '1'
                                else: 
                                    pass
                        else:
                            for d in sorted(night_slots[date_fl[k]]):
                                if p == 0 and bulk_break[(k,d)] == '0' and sch_slot[(k,d)] < 2:
                                    if (k,d) in out_3:
                                        out_3[(k,d)].append(a)
                                    else:
                                        out_3[(k,d)] = [a]
                                    p = 1
                                    bulk_break[(k,d)] = '1'
                                else:
                                    pass
                            for d in sorted(night_slots[date_fl[k]]):
                                if p == 0 and bulk_break[(k,d)] == '0' and sch_slot[(k,d)] <= 2:
                                    if (k,d) in out_3:
                                        out_3[(k,d)].append(a)
                                    else:
                                        out_3[(k,d)] = [a]
                                    p = 1
                                    bulk_break[(k,d)] = '1'
                                else:
                                    pass
                            for d in sorted(night_slots[date_fl[k]]):
                                if p == 0 and bulk_break[(k,d)] == '0' and sch_slot[(k,d)] <= 2:
                                    if (k,d) in out_3:
                                        out_3[(k,d)].append(a)
                                    else:
                                        out_3[(k,d)] = [a]
                                    p = 1
                                    bulk_break[(k,d)] = '1'
                                else: 
                                    pass
                                    
                        if p == 1:
                           gh = 0
                           for d in sorted(day_slots[date_fl[k]]):
                               if bulk_break[(k,d)] == '1' and gh != 0 and gh != len(day_slots[date_fl[k]])-1:
                                   w = gh-1
                                   if w < len(day_slots[date_fl[k]]):
                                       bulk_break[(k,day_slots[date_fl[k]][w])] = '2'
                                   w = gh+1
                                   if w < len(day_slots):
                                       bulk_break[(k,day_slots[date_fl[k]][w])] = '2'
                               gh = gh+1
                           gh = 0
                           for d in sorted(night_slots[date_fl[k]]):
                               if bulk_break[(k,d)] == '1' and gh != 0 and gh != len(night_slots[date_fl[k]])-1:
                                   w = gh-1
                                   if w < len(night_slots[date_fl[k]]):
                                       bulk_break[(k,night_slots[date_fl[k]][w])] = '2'
                                   w = gh+1
                                   if w < len(night_slots):
                                       bulk_break[(k,night_slots[date_fl[k]][w])] = '2'
                               gh = gh+1
                else: #other appointments
                    if j == 1:
                        for l in sorted(day_slots[date_fl[k]]):
                            if l not in st_slot:
                                if sch_slot[(k,l)] < 1:
                                    if p == 0:
                                        if (k,l) in out_3:
                                            out_3[(k,l)].append(a)
                                            p = 1
                                            sch_slot[(k,l)] = sch_slot[(k,l)] + 1
                                        else:
                                            out_3[(k,l)] = [a]
                                            p = 1
                                            sch_slot[(k,l)] = sch_slot[(k,l)] + 1
                                
                        if p == 0:
                            for l in sorted(day_slots[date_fl[k]]):
                                if sch_slot[(k,l)] < 2:
                                    if p == 0:
                                        if (k,l) in out_3:
                                            out_3[(k,l)].append(a)
                                            p = 1
                                            sch_slot[(k,l)] = sch_slot[(k,l)] + 1
                                        else:
                                            out_3[(k,l)] = [a]
                                            p = 1
                                            sch_slot[(k,l)] = sch_slot[(k,l)] + 1 
                        if p == 0:
                            for l in sorted(day_slots[date_fl[k]]):
                                if sch_slot[(k,l)] < 3:
                                    if p == 0:
                                        if (k,l) in out_3:
                                            out_3[(k,l)].append(a)
                                            p = 1
                                            sch_slot[(k,l)] = sch_slot[(k,l)] + 1
                                        else:
                                            out_3[(k,l)] = [a]
                                            p = 1
                                            sch_slot[(k,l)] = sch_slot[(k,l)] + 1
                        if p == 0:
                            for l in sorted(day_slots[date_fl[k]]):
                                if sch_slot[(k,l)] <= 3:
                                    if p == 0:
                                        if (k,l) in out_3:
                                            out_3[(k,l)].append(a)
                                            p = 1
                                            sch_slot[(k,l)] = sch_slot[(k,l)] + 1
                                        else:
                                            out_3[(k,l)] = [a]
                                            p = 1
                                            sch_slot[(k,l)] = sch_slot[(k,l)] + 1
                    else:
                        for l in sorted(night_slots[date_fl[k]]):
                            if l not in st_slot:
                                if sch_slot[(k,l)] < 1:
                                    if p == 0:
                                        if (k,l) in out_3:
                                            out_3[(k,l)].append(a)
                                            p = 1
                                            sch_slot[(k,l)] = sch_slot[(k,l)] + 1
                                        else:
                                            out_3[(k,l)] = [a]
                                            p = 1
                                            sch_slot[(k,l)] = sch_slot[(k,l)] + 1
                        if p == 0:
                            for l in sorted(night_slots[date_fl[k]]):
                                if sch_slot[(k,l)] < 2:
                                    if p == 0:
                                        if (k,l) in out_3:
                                            out_3[(k,l)].append(a)
                                            p = 1
                                            sch_slot[(k,l)] = sch_slot[(k,l)] + 1
                                        else:
                                            out_3[(k,l)] = [a]
                                            p = 1
                                            sch_slot[(k,l)] = sch_slot[(k,l)] + 1
                        if p == 0:
                            for l in sorted(night_slots[date_fl[k]]):
                                if sch_slot[(k,l)] < 3:
                                    if p == 0:
                                        if (k,l) in out_3:
                                            out_3[(k,l)].append(a)
                                            p = 1
                                            sch_slot[(k,l)] = sch_slot[(k,l)] + 1
                                        else:
                                            out_3[(k,l)] = [a]
                                            p = 1
                                            sch_slot[(k,l)] = sch_slot[(k,l)] + 1
                        if p == 0:
                            for l in sorted(night_slots[date_fl[k]]):
                                if sch_slot[(k,l)] <= 3:
                                    if p == 0:
                                        if (k,l) in out_3:
                                            out_3[(k,l)].append(a)
                                            p = 1
                                            sch_slot[(k,l)] = sch_slot[(k,l)] + 1
                                        else:
                                            out_3[(k,l)] = [a]
                                            p = 1
                                            sch_slot[(k,l)] = sch_slot[(k,l)] + 1
                if p == 0:
                    missed_ref.append(a)
    logger.info("Completed Scheduling time slots")     
    print(M1)
    logger.info("No.of times day model failed: "+ str(M1))
    print(M2)
    logger.info("No.of times shift model failed: "+ str(M2))
    data = {}
    #Printing Schedule with units
    out_file = open('SCHEDULE_DFW_UNITS.csv','w')
    out_file.write('Id'+','+'Scheduled_date'+','+'shift'+','+'units'+','+'sku'+','+'VRDD-ORDD'+','+'VRDD'+','+'UPT_type')
    out_file.write('\n')
    M3 = 0
    for i,j in sorted(out_2.keys()):
        for k in out_2[(i,j)]:
            out_file.write(str(k)+','+str(i)+','+str(j)+','+str(units_sku_obj[k][0])+','+str(units_sku_obj[k][1])+','+str(units_sku_obj[k][2])+','+str(dt[k][0]))
            out_file.write('\n')
    out_file.close()
    a = str(dtm.datetime.today())
    logger.info("Writing Output to a CSV file")    
    #Printing the output schedule                   
    out_file = open('SCHEDULE_DFW_'+str(date)+'_.csv','w')
    out_file.write('Reference_number'+','+'PO_number'+','+'Scheduled_date'+','+'Scheduled_time'+','+'units'+','+'sku'+','+'hj_rank'+','+'vendor'+','+'carrier'+','+'delete'+','+'ORDD'+','+'VRDD'+','+'vas_units'+','+'VNA'+','+'Reason')
    out_file.write('\n')
    for i,j in sorted(out_3.keys()):
        for k in out_3[(i,j)]:
            for l in ref_num[k]:
                if (vendor[k],j) in std_no:
                    out_file.write(str(k)+','+str(l)+','+str(i)+','+str(j)+','+str(po[(k,l)][0])+','+str(po[(k,l)][1])+','+str(hj_rank[k])+','+str(v_name[k])+','+str(csr[k])+','+'N'+','+str(ordd[l])+','+str(vrdd[k])+','+str(vas_units[k])+','+'419'+','+str(std_no[(vendor[k],j)]))
                    out_file.write('\n')
                    data[M3,l] = [a,k,l,i,j,po[(k,l)][0],po[(k,l)][1],v_name[k],cr_dt[k],'DFW1','Daily']
                else:
                    out_file.write(str(k)+','+str(l)+','+str(i)+','+str(j)+','+str(po[(k,l)][0])+','+str(po[(k,l)][1])+','+str(hj_rank[k])+','+str(v_name[k])+','+str(csr[k])+','+'N'+','+str(ordd[l])+','+str(vrdd[k])+','+str(vas_units[k]))
                    out_file.write('\n')
                    data[M3,l] = [a,k,l,i,j,po[(k,l)][0],po[(k,l)][1],v_name[k],cr_dt[k],'DFW1','Daily']
            M3 = M3+1
    df_out = pd.DataFrame(data = data.values())
    for i,j in rsch.keys():
        out_file.write(str(i)+','+str(j)+','+str(rsch[(i,j)][0])+','+str(rsch[(i,j)][1])+','+','+','+','+','+','+'Y')
        out_file.write('\n')
    out_file.close()
    logger.info("CSV file is created")
    logger.info("Writing data into Sandbox table")
    #Writing Exception day model
    if df_day.empty == False:
        df_day.columns = ['rt','portal_fc','po_fc','ref','po','vrdd','cr_dt','rc']
        for index,row in df_day.iterrows():
            cur.execute('INSERT INTO sandbox_supply_chain.iso_exception ("rundate","portal_fc","po_fc","Ref_no","PO_no","VRDD","created_dt","reason_code") VALUES (?,?,?,?,?,?,?,?)',
                        (row['rt'],row['portal_fc'],row['po_fc'],row['ref'],row['po'],row['vrdd'],row['cr_dt'],row['rc']))
    #Writng Exception Shift model
    if df_sh.empty == False:
        df_sh.columns = ['rt','portal_fc','po_fc','ref','po','vrdd','cr_dt','rc']
        for index,row in df_sh.iterrows():
            cur.execute('INSERT INTO sandbox_supply_chain.iso_exception ("rundate","portal_fc","po_fc","Ref_no","PO_no","VRDD","created_dt","reason_code") VALUES (?,?,?,?,?,?,?,?)',
                        (row['rt'],row['portal_fc'],row['po_fc'],row['ref'],row['po'],row['vrdd'],row['cr_dt'],row['rc']))
    #writing ISO output
    if df_out.empty == False:
        df_out.columns = ['rt','ref','po','dt','tm','units','sku','vendor','cr_dt','FC_nm','Batch']
        for index,row in df_out.iterrows():
            cur.execute('INSERT INTO sandbox_supply_chain.ISO_OUTPUT_NEW ("rundate","Reference_number","PO_number","Sch_date","Sch_time","Units","SKU","vendor","Created_dt","FC_nm","Batch") VALUES (?,?,?,?,?,?,?,?,?,?,?)',
                        (row['rt'],row['ref'],row['po'],row['dt'],row['tm'],row['units'],row['sku'],row['vendor'],row['cr_dt'],row['FC_nm'],row['Batch']))
    bulk_e = {}
    cnt = 0
    for i,j in sorted(out_3.keys()):
        dt = pd.to_datetime(i).date()
        tm = pd.to_datetime(j).time()
        combine = dtm.datetime.combine(dt,tm)
        est = pytz.timezone('US/Eastern')
        loc = est.localize(combine)
        utc = pytz.utc
        loc = loc.astimezone(utc)
        loc = loc.replace(tzinfo = None)
        for k in out_3[(i,j)]:
            for l in ref_num[k]:
                bulk_e[cnt] = [str(k),str(l),str(i),str(j),a,'0',a,'1',a,'1','DFW1',int(inc[k]),'419',str(loc)]
                cnt = cnt+1
    df_bulk = pd.DataFrame(data = bulk_e.values())
    if df_bulk.empty == False:
        df_bulk.columns = ['ref_no','po','date','time','csv_tm','csv_fl','hj_tm','hj_fl','bul_tm','bul_fl','FC_nm','inc_no','fr_type','gmt']
        for index,row in df_bulk.iterrows():
          cur.execute('INSERT INTO sandbox_supply_chain.iso_bulk_email ("reference_number","PO_number","Scheduled_date","Scheduled_time","csv_timestamp","csv_flag","HJ_timestamp","HJ_flag","bulk_mail_timestamp","bulk_mail_flag","FC_nm","Incident_NO","Freight_type","utc_time") VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)',
                      row['ref_no'],row['po'],row['date'],row['time'],row['csv_tm'],row['csv_fl'],row['hj_tm'],row['hj_fl'],row['bul_tm'],row['bul_fl'],row['FC_nm'],row['inc_no'],row['fr_type'],row['gmt'])
            
    logger.info("Completed writing data into sandbox table")
    sum1 = 0
    for i in out_1.keys():
        sum1 = sum1 + len(out_1[i])
    print(sum1)
    logger.info("No.of incidents in a day: "+ str(sum1))
    
    sum3 = 0
    for (i,j) in out_3.keys():
        sum3 = sum3 + len(out_3[(i,j)])
    print(sum3)
    logger.info("No.of incidents scheduled: "+ str(sum3))
    cnt = 0
    for i  in vas_flag.keys():
        if vas_flag[i] == '1':
            cnt = cnt+1
        else:
            pass
    print (cnt)
    end_time = time.time()
    execution = end_time-start_time
    print(execution)
    logger.info("Execution time: "+ str(execution)+" SECONDS")
 
    fromaddr = 'scsystems@chewy.com'
    toaddr = 'vmanohar@chewy.com'
    to = ', '.join(toaddr)
    msg = MIMEMultipart()
    msg['From'] = fromaddr
    msg['To'] = toaddr
    msg['Subject'] = "Algorithm Successfully ran for DFW1" 
    body = "Hello, \nNo.of times Day Model failed: "+str(M1)+"\nNo.of times Shift model failed: "+str(M2)+"\nNo.of Incidents Requested: "+str(sum1)+"\nNo.of Incidents Scheduled: "+str(sum3)+".\nThanks"
    msg.attach(MIMEText(body, 'plain'))
    server = smtplib.SMTP('smtp.chewymail.com', 25)
    text = msg.as_string()
    server.sendmail(fromaddr,toaddr.split(','), text)
    logger.info("Email was sent to the recipients: %s" %(toaddr))
    server.quit()
    print("Email was sent to the recipients: %s" %(toaddr))
    if M1 > 0 or M2 > 0:
        if M1 > 0 and M2==0:
            fromaddr = 'scsystems@chewy.com'
            toaddr = 'vmanohar@chewy.com,igonzalez1@chewy.com,EAlfonso@chewy.com,jxie@chewy.com'
            to = ', '.join(toaddr)
            file_list = ['day_model_DFW.ilp']
            msg = MIMEMultipart()
            msg['From'] = fromaddr
            msg['To'] = toaddr
            msg['Subject'] = "LP Model Failed for DFW1 at day level" 
            body = "Hello, \nModel Failed at day level for"+str(M1)+"times.\nThanks\nVenkatesh"
            msg.attach(MIMEText(body, 'plain'))
            for j in file_list:
                file_path = j
                attachment = open(file_path, "rb")
                part = MIMEBase('application', 'octet-stream')
                part.set_payload((attachment).read())
                encoders.encode_base64(part)
                part.add_header('Content-Disposition', "attachment; filename= %s" % j)
                msg.attach(part)
            server = smtplib.SMTP('smtp.chewymail.com', 25)
            text = msg.as_string()
            server.sendmail(fromaddr,toaddr.split(','), text)
            logger.info("Email was sent to the recipients: %s" %(toaddr))
            server.quit()
            print("Email was sent to the recipients: %s" %(toaddr))
        elif M1==0 and M2 > 0:
            fromaddr = 'scsystems@chewy.com'
            toaddr = 'vmanohar@chewy.com,igonzalez1@chewy.com,EAlfonso@chewy.com,jxie@chewy.com'
            to = ', '.join(toaddr)
            file_list = ['shift_model_DFW.ilp']
            msg = MIMEMultipart()
            msg['From'] = fromaddr
            msg['To'] = toaddr
            msg['Subject'] = "LP Model Failed for DFW1 at shift level" 
            body = "Hello, \nModel Failed at shift level"+str(M2)+"times.\nThanks\nVenkatesh"
            msg.attach(MIMEText(body, 'plain'))
            for j in file_list:
                file_path =  j
                attachment = open(file_path, "rb")
                part = MIMEBase('application', 'octet-stream')
                part.set_payload((attachment).read())
                encoders.encode_base64(part)
                part.add_header('Content-Disposition', "attachment; filename= %s" % j)
                msg.attach(part)
            server = smtplib.SMTP('smtp.chewymail.com', 25)
            text = msg.as_string()
            server.sendmail(fromaddr,toaddr.split(','), text)
            logger.info("Email was sent to the recipients: %s" %(toaddr))
            server.quit()
            print("Email was sent to the recipients: %s" %(toaddr))
        elif M1 > 0 and M2 > 0:
            fromaddr = 'scsystems@chewy.com'
            toaddr = 'vmanohar@chewy.com,igonzalez1@chewy.com,EAlfonso@chewy.com,jxie@chewy.com'
            to = ', '.join(toaddr)
            file_list = ['shift_model_DFW.ilp','day_model_DFW.ilp']
            msg = MIMEMultipart()
            msg['From'] = fromaddr
            msg['To'] = toaddr
            msg['Subject'] = "LP Model Failed for DFW1 at day level and  shift level" 
            body = "Hello, \nModel Failed at day level"+str(M1)+"times and shift level"+str(M2)+"times.\nThanks\nVenkatesh"
            msg.attach(MIMEText(body, 'plain'))
            for j in file_list:
                file_path = j
                attachment = open(file_path, "rb")
                part = MIMEBase('application', 'octet-stream')
                part.set_payload((attachment).read())
                encoders.encode_base64(part)
                part.add_header('Content-Disposition', "attachment; filename= %s" % j)
                msg.attach(part)
            server = smtplib.SMTP('smtp.chewymail.com', 25)
            text = msg.as_string()
            server.sendmail(fromaddr,toaddr.split(','), text)
            logger.info("Email was sent to the recipients: %s" %(toaddr))
            server.quit()
            print("Email was sent to the recipients: %s" %(toaddr))    
    cxn.close()
    logger.info("Vertica is Disconnected")
except Exception as e:
    print("Error Reported")
    logger.error("Error in the code: "+str(e))
    fromaddr = 'scsystems@chewy.com'
    toaddr = 'vmanohar@chewy.com,igonzalez1@chewy.com,EAlfonso@chewy.com,jxie@chewy.com'
    to = ', '.join(toaddr)
    msg = MIMEMultipart()
    msg['From'] = fromaddr
    msg['To'] = toaddr
    msg['Subject'] = "Algorithm did not run for DFW1" 
    body = "Hello, Algorithm failed for the following reason :"+str(e)+"\nThanks"
    msg.attach(MIMEText(body, 'plain'))
    server = smtplib.SMTP('smtp.chewymail.com', 25)
    text = msg.as_string()
    server.sendmail(fromaddr,toaddr.split(','), text)
    logger.info("Email was sent to the recipients: %s" %(toaddr))
    server.quit()
    print("Email was sent to the recipients: %s" %(toaddr))
    logger.info("Vertica is Disconnected")
    cxn.close()
rh.close()
