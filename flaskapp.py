from flask import Flask, request, render_template, session, redirect, url_for, make_response
import os
import MySQLdb, hashlib, os
from werkzeug.utils import secure_filename
from datetime import datetime
import base64
from nltk.corpus import wordnet
import pandas as pd
import pymysql
import time
import random
from pymemcache.client.base import Client


from sqlalchemy import create_engine
from math import sin, cos, sqrt, atan2, radians
app = Flask(__name__)
app.secret_key = "RANDOM"

#Home Page
@app.route('/')
def HomePage():
    return render_template("login.html")

Uploadpath = "/home/ubuntu/Upload"
Downloadpath = "/home/ubuntu/Download"

connection = MySQLdb.connect("sql-db-instance","Username","password","Db-Name")
engine= create_engine("mysql+pymysql://Username:Password@AWS-Db-Instance/dbName")
memc=  Client(('AWS-elasticache-endpoint',11211))

#Login Page
@app.route('/login',methods=['POST','GET'])
def UserLogin():
    if 'username' in session:
        return render_template('index.html', username = session['username'])
    if request.method == 'POST':
        cursor = connection.cursor()
        username = request.form['username']
        if(username == '' ):
            return render_template('login.html',resultText="Invalid UserName ")
        sql = "select Username from Users where Username = '"+username+"'"
        cursor.execute(sql)
        if cursor.rowcount == 1:
            results = cursor.fetchall()
            for row in results:
                session['username'] = username
                return render_template('index.html', username = session['username'],resultObject=results)
        else:
            return render_template('login.html',resultText="Invalid User. Please try again")
    else:
        return render_template('register.html' , resultText ="User Not Present . Please Register")


#Register New USer Page
@app.route('/register',methods=['POST','GET'])
def NavigateToRegister():
    return  render_template('register.html')
#Register New User
@app.route('/registerUser',methods=['POST','GET'])
def RegisterUser():
    if 'username' in session:
        return render_template('index.html', username = session['username'])

    if request.method == 'POST':
        cursor = connection.cursor()
        username = request.form['username']
        if(username == '' ):
            return render_template('register.html',resultText="UserName  empty")
        sql = "select Username from Users where Username='"+username+"'"
        cursor.execute(sql)
        if cursor.rowcount == 1:
            return render_template('register.html', resultText="Username Already Present. Please try a different Name ")

        sql = "insert into Users (Username) values ('"+username+"')"
        cursor.execute(sql)
        connection.commit()
        cursor.close()
        return render_template('login.html', resultText="User Registered Successfully . please Login")
    else:
        return render_template('register.html',resultText="Something went Wrong! Please try again")

@app.route('/uploader',methods=['POST','GET'])
def uploader():
    if 'username' not in session:
        return render_template('login.html')
    if request.method == 'POST':
        file = request.files['file']
        filename = secure_filename(file.filename)
        file.save(os.path.join(Uploadpath, filename))
        con=engine.connect()
        df = pd.read_csv(os.path.join(Uploadpath, filename))
        #df= cleanDF(df)
        fileNameWithoutext= str(filename.split('.')[0])
        start_time = time.time()
        df.to_sql(name = fileNameWithoutext,con = con,if_exists='replace',index=True, index_label='rowNumber')
        end_time = time.time()
        total_time = str(end_time - start_time)
        con.close()
    return render_template('index.html', resultText="File Uploaded successfully , Total SQL time "+ total_time +" Seconds")

@app.route('/queries',methods=['POST','GET'])
def queryExecute():
    if 'username' not in session:
        return render_template('login.html')
    cursor = connection.cursor()
    sql ='SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_TYPE = "BASE TABLE" AND TABLE_SCHEMA="AWSStrorageDB" '
    cursor.execute(sql)
    results = cursor.fetchall()
    data=[]
    for row in results:
            data.append(row[0])
    print('##########################')
    print(data)
    print('#########################')
    if cursor.rowcount >= 1:
            return render_template('queries.html',resultTables = data)
    return render_template('queries.html')

@app.route('/runRandQueries',methods=['POST','GET'])
def runRandQueries():
    if 'username' not in session:
        return render_template('login.html')
    if request.method == 'POST':
        rangeRand=request.form['randQueryCount']
        rangeRand=int(rangeRand)
        dbTable = request.form['Dbtable']
        cursor = connection.cursor()
        startTime = time.time()
        for num in range(1, rangeRand):
            randomNumber = random.randint(0, rangeRand)
            dynamicQuery = "SELECT * FROM {0} where rowNumber = {1};".format(dbTable,randomNumber)
            cursor.execute(dynamicQuery)
            rows = cursor.fetchall()
            print("******************")
            print (rows)
        end_time = time.time()
        total_time = str(end_time - startTime)
        for i in range(1, rangeRand):
            query_m = "select * from {0} ;".format(dbTable)
            cursor.execute(query_m)
            mqhash = hashlib.sha256(query_m).hexdigest()
            start_time = time.time()
            result = memc.get(mqhash)
            if not result:
                value = cursor.execute(query_m)
                memc.set(mqhash, value)
        end_time = time.time()
        total_time2 = str((end_time - start_time) )
        # print(' time taken', total_time)
        cursor.close()
        return render_template('queries.html', resultText="Time to run "+ str(rangeRand)+ " queries in RDS "+ total_time + " seconds" +"\n" + "Time to run "+ str(rangeRand)+ " queries in memcache "+ total_time2 +  " seconds")


@app.route('/Locations',methods=['POST','GET'])
def LocationQuery():
    if 'username' not in session:
        return render_template('login.html')
    if request.method=='POST':
        location=request.form['Location']
        dbTable = 'significant_month'
        cursor = connection.cursor()
        startTime=time.time()
        sql="SELECT * FROM {0} where place like '%{1}%';".format(dbTable,location)
        cursor.execute(sql)
        end_time = time.time()
        total_time = str(end_time - startTime)
        data=[]
        results = cursor.fetchall()
        for row in results:
            rowdetails={}
            rowdetails['time']=row[1]
            rowdetails['latitude']=row[2]
            rowdetails['longitude']=row[3]
            rowdetails['depth']=row[4]
            rowdetails['mag']=row[5]
            rowdetails['place']=row[14]
            print("################# row details###########3")
            print(rowdetails)
            data.append(rowdetails)
        return render_template('queries.html',LocationQueryTime="Time to run this query in RDS "+ total_time + " seconds" , resultText1=data, resultTextTotalRecords=len(data))



@app.route('/LocationsDist',methods=['POST','GET'])
def distbwLocations():
    if 'username' not in session:
        return render_template('login.html')
    if request.method=='POST':
        LocationLatitude=request.form['LocationLatitude']
        print(LocationLatitude)
        LocationLatitude2=request.form['LocationLatitude2']
        LocationLongitude=request.form['LocationLongitude']
        LocationLongitude2=request.form['LocationLongitude2']
        print(LocationLongitude)
        dbTable = 'Starbucks'
        print(dbTable)
        cursor = connection.cursor()
        startTime=time.time()
        sql="SELECT * FROM Starbucks where (Latitude >= {1} or Latitude <= {2}) and (Longitude >={3} or Longitude <={4} ) ;".format(dbTable, float(LocationLatitude),float(LocationLatitude2),float(LocationLongitude),float(LocationLongitude2))
        #sql="SELECT * FROM Starbucks where (Latitude >= 100 or Latitude <= 120) and (Longitude >=40 or Longitude <=80 ) ;"
        cursor.execute(sql)
        results = cursor.fetchall()
        print("Results")
        print(results)
        data=[]
        for row in results:
            rowdetails={}

             #rowdetails['time']=row[1]
            rowdetails['id']=row[2]
            rowdetails['Name']=row[3]
            rowdetails['StoreNumber']=row[4]
            rowdetails['latitude']=row[14]
            rowdetails['longitude']=row[15]
            rowdetails['Name']=''.join([x for x in rowdetails['Name'] if ord(x) < 128])
            #rowdetails['distance']=computeDistance(LocationLatitude,LocationLongitude,row[14],row[15])
            print("$$$$$$$$$$$$$$")
            print(rowdetails)
            data.append(rowdetails)
        end_time = time.time()
        total_time = str(end_time - startTime)
        return render_template('queries.html',LocationLatLongTime="Time to run this query in RDS "+ total_time + " seconds" , resultText2=data, resultTextTotalRecords2=len(data))



@app.route('/MagnitudeRange',methods=['POST','GET'])
def ComputeMagnitude():
    if 'username' not in session:
        return render_template('login.html')
    if request.method=='POST':
        RangeFrom=request.form['RangeFrom']
        RangeTo=request.form['RangeTo']
        City=request.form['City']
        cursor = connection.cursor()
        startTime=time.time()
        #sql="SELECT * FROM Education where SAT_AVG >= 500 OR SAT_AVG <=2000 and CITY ='Marion'  LIMIT 3  ;"
        sql="SELECT * FROM AWSStrorageDB.Education where SAT_AVG<={0} or SAT_AVG>={1} and CITY='{3}' LIMIT 3;".format(int(RangeFrom),int(RangeTo),str(City))
        sql2 ="SELECT * FROM Starbucks where CITY ='{0}' LIMIT 8;".format(str(City))
        cursor.execute(sql)
        end_time = time.time()

        results = cursor.fetchall()
        cursor.execute(sql2)
        total_time = str(end_time - startTime)
        results2=cursor.fetchall()
        data=[]
        for row in results:
            rowdetails={}
            rowdetails['Name']=row[4]
            rowdetails['City']=City

            print("################# row details###########3")
            print(rowdetails)
            data.append(rowdetails)
        for row in results2:
            rowdetails={}
            rowdetails['SName']=row[3]
            rowdetails['StoreNumber']=row[4]
            rowdetails['SName']=''.join([x for x in rowdetails['SName'] if ord(x) < 128])
            data.append(rowdetails)

        return render_template('queries.html',MagnitudeTime="Time to run this query in RDS "+ total_time + " seconds" , resultText3=data, resultTextTotalRecords3=len(data))





def computeDistance(lat1,long1,lat2,long2):
    R = 6373.0
    lat1 = radians(float(lat1))
    lon1 = radians(float(long1))
    lat2 = radians(float(lat2))
    lon2 = radians(float(long2))
    dlon = lon2 - lon1
    dlat = lat2 - lat1
    a = sin(dlat / 2)**2 + cos(lat1) * cos(lat2) * sin(dlon / 2)**2
    c = 2 * atan2(sqrt(a), sqrt(1 - a))
    distance = R * c
    return (distance)

def cleanDF(df):
    columns=list(df)
    df=df.dropna(axis=0,how='any')
    df= df.dropna(axis=1, how='all')
    return df




#Logout User
@app.route('/logout', methods=['POST','GET'])
def logout():
    if 'username' in session:
        session.pop('username', None)
    return render_template('login.html')




if __name__ == '__main__':
  app.run()
