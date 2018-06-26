from pf import app
from pf import dbfunc as db
from pf import jwtdecodenoverify as jwtnoverify

from datetime import datetime, date, timedelta
from multiprocessing import Process
from multiprocessing import Pool
import json
import time

from flask import request, make_response, jsonify, Response, redirect
import psycopg2
import requests
import jwt
from dateutil import tz

@app.route('/dashfetchdata',methods=['POST','OPTIONS'])
def dashfetchdata():
#This is called by fund data fetch service
    if request.method=='OPTIONS':
        print("inside dashfetchdata options")
        return make_response(jsonify('inside dashfetchdata options'), 200)  

    elif request.method=='POST':
        print("inside dashfetchdata POST")
        print((request))        
        print(request.headers)
        userid,entityid=jwtnoverify.validatetoken(request)
        print(datetime.now().strftime('%Y-%m-%d %H:%M:%S'))
        print(userid,entityid)
    
        payload= request.get_json() 
        print(payload)

        pfid = payload.get('pfid', None)
        prodtyp = payload.get('prodtyp', None)
        fundid = payload.get('fundid', None)
        # data required for dashboard
        datarequired = payload.get('datareq', None)
        # pffull - PF details + Product summary
        # prodfull - funds under the Product 
        # fndsum - Fund transactions under the pf
		# fndfull - Fund transactions under the pf

        print('after')
        #time.sleep(1)

        pfsum_record = None
        pf_prodwise_record = None
        prod_det_record = None
        fund_record = None
        fund_det_record = None
        request_status = None
        failure_reason = None
        
		
		if datarequired == None:
            request_status = "failed"
            failure_reason = "No details on the data requirment provided by client"

        elif datarequired == "pffull": 
            #This is for the dashboard page
            pfsum_record, request_status, failure_reason = get_dashbord_data("pfsumrec",pfid,entityid)
			pf_prodwise_record, request_status, failure_reason  = get_dashbord_data("prodsumrec",pfid,entityid)

        elif datarequired == "prodfull":
            prod_det_record, request_status, failure_reason = get_dashbord_data("fundsumrec",pfid,prodtyp,fundid,fromdt,todt,entityid)
		
		elif datarequired == "fundfull":
			fund_det_record, request_status, failure_reason = get_dashbord_data("funddetrec",pfid,prodtyp,fundid,fromdt,todt,entityid)
		
		# Check for DB error and send user friendly error msg to front end
		if request_status == "dbfail":
			request_status = "failed"
            failure_reason = "Data base Error contact Adminstrator"

        records = {
            'pfsumrec'      :  {} if pfsum_record == None else pfsum_record,             # pffull
            'prodsumrec'    :  {} if pf_prodwise_record == None else pf_prodwise_record, # pffull (product summary for a PF)
            'fundsumrec'    :  {} if prod_det_record == None else prod_det_record,       # prodfull
            'funddetrec'    :  {} if fund_det_record == None else fund_det_record,       # fundfull
            'status'        :  "success" if request_status == None else request_status,
            'failreason'    :  "" if failure_reason == None else failure_reason
        }

    return make_response(jsonify(records), 200)
		
		
def get_dashbord_data(datareq,pfid,prodtyp,fundid,fromdt,todt,entityid,offset=0):
        con,cur=db.mydbopncon()
        print(con)
        print(cur)
		status = None
		failreason = None
		
        if datarequired == "pfsumrec": 
            #This is for the dashboard page

            #cur.execute("select row_to_json(art) from (select a.*, (select json_agg(b) from (select * from pfstklist where pfportfolioid = a.pfportfolioid ) as b) as pfstklist, (select json_agg(c) from (select * from pfmflist where pfportfolioid = a.pfportfolioid ) as c) as pfmflist from pfmaindetail as a where pfuserid =%s ) art",(userid,))
            #command = cur.mogrify("select row_to_json(art) from (select a.*,(select json_agg(b) from (select * from webapp.pfstklist where pfportfolioid = a.pfportfolioid ) as b) as pfstklist, (select json_agg(c) from (select c.*,(select json_agg(d) from (select * from webapp.pfmforlist where orormflistid = c.ormflistid AND ormffndstatus='INCART' AND entityid=%s) as d) as ormffundorderlists from webapp.pfmflist c where orportfolioid = a.pfportfolioid ) as c) as pfmflist from webapp.pfmaindetail as a where pfuserid =%s AND entityid=%s) art",(entityid,userid,entityid,))
            command = cur.mogrify(
                """
                SELECT row_to_json(b) FROM (
                    SELECT dpos_pfportfolioid AS pfid, sum(dpos_invamount) AS invamt,sum(dpos_curvalue) AS curval,sum(dpos_totalpnl) AS pnl, (sum(dpos_curvalue)/sum(dpos_invamount))*100 AS percentgr
                    FROM webapp.dailyposition WHERE dpos_pfportfolioid = %s AND dpos_entityid = %s
                    GROUP BY dpos_pfportfolioid
                ) b;
                """,(pfid,entityid,))
				
		elif datarequired == "prodsumrec": 
        #cur.execute("select row_to_json(art) from (select a.*, (select json_agg(b) from (select * from pfstklist where pfportfolioid = a.pfportfolioid ) as b) as pfstklist, (select json_agg(c) from (select * from pfmflist where pfportfolioid = a.pfportfolioid ) as c) as pfmflist from pfmaindetail as a where pfuserid =%s ) art",(userid,))
            #command = cur.mogrify("select row_to_json(art) from (select a.*,(select json_agg(b) from (select * from webapp.pfstklist where pfportfolioid = a.pfportfolioid ) as b) as pfstklist, (select json_agg(c) from (select c.*,(select json_agg(d) from (select * from webapp.pfmforlist where orormflistid = c.ormflistid AND ormffndstatus='INCART' AND entityid=%s) as d) as ormffundorderlists from webapp.pfmflist c where orportfolioid = a.pfportfolioid ) as c) as pfmflist from webapp.pfmaindetail as a where pfuserid =%s AND entityid=%s) art",(entityid,userid,entityid,))
            command = cur.mogrify(
                """
                SELECT json_agg(b) FROM (
                    SELECT 
                        dpos_producttype AS prodtyp,
                        CASE 
                            WHEN dpos_producttype = 'BSEMF' THEN 'MUTUAL FUNDS'
                            WHEN dpos_producttype = 'SGB' THEN 'SGB'
                            WHEN dpos_producttype = 'EQ' THEN 'Equity'
                            WHEN dpos_producttype = 'HEQ' THEN 'Home Equity'
                        END AS product,	
                        dpos_pfportfolioid AS pfid,
                        sum(dpos_invamount) AS invamt,sum(dpos_curvalue) AS curval,sum(dpos_totalpnl) AS pnl, (sum(dpos_curvalue)/sum(dpos_invamount))*100 AS percentgr
                    FROM webapp.dailyposition WHERE dpos_pfportfolioid = %s AND dpos_entityid = %s
                    GROUP BY dpos_producttype,dpos_pfportfolioid
                ) b;
                """,(pfid,entityid,))

        elif datarequired == "fundsumrec": 
            #This is for the dashboard details page
            command = cur.mogrify(
                """
                SELECT json_agg(b) FROM (
                    SELECT          
                        dpos_schemecd AS schemecode,
                        b.fnddisplayname as schemename,
                        dpos_producttype AS prodtyp,
                        CASE                         
                            WHEN dpos_producttype = 'BSEMF' THEN 'MUTUAL FUNDS'                        
                            WHEN dpos_producttype = 'SGB' THEN 'SGB'                        
                            WHEN dpos_producttype = 'EQ' THEN 'Equity'                        
                            WHEN dpos_producttype = 'HEQ' THEN 'Home Equity' 
                        END AS product,
                        dpos_pfportfolioid AS pfid,
                        sum(dpos_unit) AS units, sum(dpos_avgnav) AS avgnav, sum(dpos_curnav) AS curnav, sum(dpos_invamount) AS invamt,sum(dpos_curvalue) AS curval,sum(dpos_totalpnl) AS pnl, (sum(dpos_curvalue)/sum(dpos_invamount))*100 AS percentgr
                    FROM webapp.dailyposition a
                    LEFT JOIN webapp.fundmaster b ON b.fndschcdfrmbse = dpos_schemecd 
                    WHERE dpos_producttype = %s AND dpos_pfportfolioid = %s AND dpos_entityid = %s
                    GROUP BY dpos_schemecd,b.fnddisplayname,dpos_producttype,dpos_pfportfolioid
                ) b;
                """,(prodtyp,pfid,entityid,))		
				
        elif datarequired == "funddetrec": 
            #This is for the dashboard details page
            command = cur.mogrify(
                """
                SELECT json_agg(b) FROM (
                    SELECT          
                        tran_schemecd AS schemecode,
                        b.fnddisplayname as schemename,
                        tran_producttype AS prodtyp,
                        CASE                         
                            WHEN tran_producttype = 'BSEMF' THEN 'MUTUAL FUNDS'                        
                            WHEN tran_producttype = 'SGB' THEN 'SGB'                        
                            WHEN tran_producttype = 'EQ' THEN 'Equity'                        
                            WHEN tran_producttype = 'HEQ' THEN 'Home Equity' 
                        END AS product,
                        dpos_pfportfolioid AS pfid,
                        tran_unit AS units, tran_nav AS trannav, 
						sum(dpos_curnav) AS curnav, 
						sum(tran_invamount) AS invamt,sum(dpos_curvalue) AS curval,sum(dpos_totalpnl) AS pnl, (sum(dpos_curvalue)/sum(dpos_invamount))*100 AS percentgr
                    FROM webapp.dailyposition a
                    LEFT JOIN webapp.fundmaster b ON b.fndschcdfrmbse = dpos_schemecd 
                    WHERE dpos_producttype = %s AND dpos_pfportfolioid = %s AND dpos_entityid = %s
                    GROUP BY dpos_schemecd,b.fnddisplayname,dpos_producttype,dpos_pfportfolioid
					OFFSET %s
                ) b;
                """,(prodtyp,pfid,entityid,offset,))	
			
		print(command)
		cur, dbqerr = db.mydbfunc(con,cur,command)
		print(cur)
		print(dbqerr)
		print(type(dbqerr))
		print(dbqerr['natstatus'])

		if cur.closed == True:
			if(dbqerr['natstatus'] == "error" or dbqerr['natstatus'] == "warning"):
				status = "dbfail"
				failreason = "pf product wise fetch failed with DB error"
				print(status)
				
		if cur.rowcount > 1:
			status = "dbfail"
			failreason = "pf product wise fetch returned more rows"
			print(status)

		record = None
		if cur.rowcount == 1:
			record = cur.fetchall()
		
		return record[0][0], status, failreason

@app.route('/dashchart',methods=['POST','OPTIONS'])
def dashchart():
#This is called by fund data fetch service
    if request.method=='OPTIONS':
        print("inside dashchart options")
        return make_response(jsonify('inside dashchart options'), 200)  

    elif request.method=='POST':
        print("inside dashchart POST")
        print((request))        
        print(request.headers)
        userid,entityid=jwtnoverify.validatetoken(request)
        print(datetime.now().strftime('%Y-%m-%d %H:%M:%S'))
        print(userid,entityid)
    
        payload= request.get_json()
        print(payload)
        data = payload
        print(data)
        
        pfid = data.get('pfid', None)
        prodtyp = data.get('prodtyp', None)
        fundid = data.get('fundid', None)
        fromdt = None
        todt = None
        # data required for dashboard
        datarequired = payload.get('datareq', None)
        # pffull - PF full data
        # prodfull - funds under the Product 
        # pffull - Fund transactions under the pf

        pffulldata = None
        prodfulldata = None
		fundfulldata = None
        request_status = None
        failure_reason = None

        if datarequired == None:
            request_status = "failed"
            failure_reason = "No details on the data requirment provided by client"

        elif datarequired == 'pffull':
            pffulldata, request_status, failure_reason = get_char_data("pffulldata",pfid,prodtyp,fundid,fromdt,todt,entityid)

        elif datarequired == 'prodfull':
            prodfulldata, request_status, failure_reason = get_char_data("prodfulldata",pfid,prodtyp,fundid,fromdt,todt,entityid)
			
		elif datarequired == 'fundfull':
            fundfulldata, request_status, failure_reason = get_char_data("fundfulldata",pfid,prodtyp,fundid,fromdt,todt,entityid)

		# Check for DB error and send user friendly error msg to front end
		if request_status == "dbfail":
			request_status = "failed"
            failure_reason = "Data base Error contact Adminstrator"
			
    chart_data = {
        'pffulldata'  :     [] if pffulldata == None else pffulldata,
        'prodfulldata':     [] if prodfulldata == None else prodfulldata,
		'fundfulldata':     [] if fundfulldata == None else fundfulldata,
        'status'      :     'success' if request_status == None else request_status,
        'failreason'  :     '' if failure_reason == None else failure_reason
    }

    return make_response(jsonify(chart_data), 200)


def get_char_data(datareq,pfid,prodtyp,fundid,fromdt,todt,entityid):
		status, failreason = None *2
        con,cur=db.mydbopncon()        
        print(con)
        print(cur)
        pf_record = None

        if datareq == "pffulldata":
            command = cur.mogrify(
            """
            SELECT json_agg(info)
            FROM (
                SELECT json_build_array("Date","investment","Growth") AS info
                FROM 
                    (
                            SELECT dpos_date AS Date,SUM(dpos_invamount) AS investment, SUM(dpos_curvalue) AS Growth from webapp.dailyposition_hist
                            WHERE dpos_pfportfolioid = %s AND dpos_entityid = %s
                            GROUP BY dpos_date
                            UNION ALL
                            SELECT dpos_date AS Date,SUM(dpos_invamount) AS investment, SUM(dpos_curvalue) AS Growth from webapp.dailyposition
                            WHERE dpos_pfportfolioid = %s AND dpos_entityid = %s
                            GROUP BY dpos_date
                    ) AS x("Date","investment","Growth")
            ) as t;
            """,(pfid,entityid,pfid,entityid,))
			
		if datareq == "prodfulldata":
            command = cur.mogrify(
            """
            SELECT json_agg(info)
            FROM (
                SELECT json_build_array("Date","investment","Growth") AS info
                FROM 
                    (
                            SELECT dpos_date AS Date,SUM(dpos_invamount) AS investment, SUM(dpos_curvalue) AS Growth FROM webapp.dailyposition_hist
                            WHERE dpos_producttype = %s AND dpos_pfportfolioid = %s AND dpos_entityid = %s
                            GROUP BY dpos_date                            
                            UNION ALL
                            SELECT dpos_date AS Date,SUM(dpos_invamount) AS investment, SUM(dpos_curvalue) AS Growth FROM webapp.dailyposition
                            WHERE dpos_producttype = %s AND dpos_pfportfolioid = %s AND dpos_entityid = %s
                            GROUP BY dpos_date
                    ) AS x("Date","investment","Growth")
            ) as t;
            """,(prodtyp,pfid,entityid,prodtyp,pfid,entityid,))
		
		if datareq == "fundfulldata":
            command = cur.mogrify(
            """
            SELECT json_agg(info)
            FROM (
                SELECT json_build_array("Date","investment","Growth") AS info
                FROM 
                    (
                            SELECT dpos_date AS Date,SUM(dpos_invamount) AS investment, SUM(dpos_curvalue) AS Growth FROM webapp.dailyposition_hist
                            WHERE dpos_schemecd = %s AND dpos_producttype = %s AND dpos_pfportfolioid = %s AND dpos_entityid = %s
                            GROUP BY dpos_date                            
                            UNION ALL
                            SELECT dpos_date AS Date,SUM(dpos_invamount) AS investment, SUM(dpos_curvalue) AS Growth FROM webapp.dailyposition
                            WHERE dpos_schemecd = %s AND dpos_producttype = %s AND dpos_pfportfolioid = %s AND dpos_entityid = %s
                            GROUP BY dpos_date
                    ) AS x("Date","investment","Growth")
            ) as t;
            """,(fundid,prodtyp,pfid,entityid,fundid,prodtyp,pfid,entityid,))
			
		print(command)
		cur, dbqerr = db.mydbfunc(con,cur,command)
		print("#########################################3")

		print(cur)
		print(dbqerr)
		print(type(dbqerr))
		print(dbqerr['natstatus'])

		if cur.closed == True:
			if(dbqerr['natstatus'] == "error" or dbqerr['natstatus'] == "warning"):
				status = "dbfail"
				failreason = "pf product wise fetch failed with DB error"
				print(status,failreason)
				
		if cur.rowcount > 1:
			status = "dbfail"
			failreason = "pf product wise fetch returned more rows"
			print(status,failreason)

		if cur.rowcount == 1:
			pf_record = cur.fetchall()[0][0]
			pf_record.insert(0,['Date', 'Investment', 'Growth'])
			print(pf_record)               

	   
		return record[0][0], status, failreason
