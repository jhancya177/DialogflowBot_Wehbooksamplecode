from __future__ import print_function
# from future.standard_library import install_aliases
# install_aliases()

from urllib.parse import urlparse, urlencode
from urllib.request import urlopen, Request
from urllib.error import HTTPError

import pandas as pd
import json
from pandas.io.json import json_normalize

import json
import os
import random

from flask import Flask
from flask import request
from flask import make_response

# Flask app should start in global layout
app = Flask(__name__)


@app.route('/webhook', methods=['POST'])
def webhook():
    req = request.get_json(silent=True, force=True)
    res = processRequest(req)
    res = json.dumps(res, indent=4)
    r = make_response(res)
    r.headers['Content-Type'] = 'application/json'
    return r


def processRequest(req):

    if req.get("queryResult").get("action") != "query":
        print("Please check your action name in DialogFlow...")
        return {}

    result = req.get("queryResult")
    parameters = result.get("parameters")
    dateduration = parameters.get("date-period")
    #print(dateduration)
    #from datetime import date
    #date_today = date.today().strftime('%Y-%m-%d')

    category = parameters['Category']
    name = parameters['Name']
    metric = parameters ['Metric']
    startdate = ""
    enddate = ""
    if dateduration != "":
        enddatee = dateduration['endDate']
        startdatee = dateduration['startDate']
        enddate = enddatee.split('T')[0]
        startdate = startdatee.split('T')[0]
        res = makeWebhookResult(category , name , startdate ,enddate , metric)
        return res


def makeWebhookResult(category , name , startdate , enddate , metric):
    import pyodbc
    import re
    from datetime import datetime
    import requests
    #import pandas as pd
    #import json
    #from pandas.io.json import json_normalize
    server = 'tcp:gcpdatabasee.database.windows.net'
    database = 'gcpdatabase'
    username = 'gcpdatabase'
    password = 'Ctli_12345678'
    driver= '{ODBC Driver 17 for SQL Server}'
    cnxn = pyodbc.connect('DRIVER='+driver+';SERVER='+server+';PORT=1433;DATABASE='+database+';UID='+username+';PWD='+password+';Encrypt=yes;TrustServerCertificate=no;')
    cursor = cnxn.cursor()
    #date_comp = date.today().strftime('%Y-%m-%d')
    if category == "we":
         if metric == 'high':
            sql = """SELECT max(%s) FROM ExpenseDetails  WHERE name in ('wife' , 'husband') and month between '%s' and '%s'""" % (category,startdate,enddate)
        elif metric == 'low':
            sql = """SELECT min(%s) FROM ExpenseDetails  WHERE name in ('wife' , 'husband') and month between '%s' and '%s'""" % (category,startdate,enddate)
        else:
            sql = """SELECT avg(%s) FROM ExpenseDetails  WHERE name like ('wife' , 'husband') and month between '%s' and '%s'""" % (category,startdate,enddate)
    else:    
        if metric == 'high': 
            sql = """SELECT max(%s) FROM ExpenseDetails  WHERE name like '%s%%' and month between '%s' and '%s'""" % (category,name,startdate,enddate)
        elif metric == 'low':
            sql = """SELECT min(%s) FROM ExpenseDetails  WHERE name like '%s%%' and month between '%s' and '%s'""" % (category,name,startdate,enddate)
        else:
            sql = """SELECT avg(%s) FROM ExpenseDetails  WHERE name like '%s%%' and month between '%s' and '%s'""" % (category,name,startdate,enddate)
    print(sql)
    cursor.execute(sql)
    row_value = cursor.fetchone()
    expense = row_value
    #print(row_value)
    match = re.search(r'\d{4}-\d{2}-\d{2}', startdate)
    date_result = datetime.strptime(match.group(), '%Y-%m-%d').date()    

    speech = [" for "+ str(category ) + str(name) +" spent "+ str(expense)+" on "+str(date_result),
                      str(name)+" spent " + str(expense) +" on " + str(category ) ,
                    "on " + str(date_result) +" " +str(name) +" spent "+str (expense) +" on "+  str (category)]
    
    return {

            "fulfillmentText":  str(random.choice(speech)),
            "source": "azure database"
                }


if __name__ == '__main__':
    port = int(os.getenv('PORT', 5003))

    print("Starting app on port %d" % port)

    app.run(debug=True, port=port, host='0.0.0.0')

