import requests
import json
import os
import mysql.connector as mc

ID="bee5729a-d0ff-41e7-8372-9ef189529540"
SECRET="FexO4d/+kzyIQGJ4i47tdSAuJZyNIwS3dTpcyZX54jw="

API_AUTH="https://api.dataservices.doverfs.com:8496/appaccess/authorize"

headers = {
    'Content-Type': 'application/json',
}
data = {'clientId':'bee5729a-d0ff-41e7-8372-9ef189529540','secret':'FexO4d/+kzyIQGJ4i47tdSAuJZyNIwS3dTpcyZX54jw='}

r = requests.post(API_AUTH,  headers=headers, data=json.dumps(data))

Token=r.json()
Token=Token['accessToken']

site_id = [
'69a2b1e1-a8ee-44da-83ef-54b44ec841b2',
'0428555e-bf25-4f68-94e3-74df90a20a27'
]

mydb = mc.connect(
    host='sitedetails.mysql.database.azure.com',
    user='dbadmin@sitedetails',
    passwd='aB8626ajit123',
    database='poc'
)
tablename = "sitedetails"
mycursor = mydb.cursor()

for i in (site_id):
    API_JSON="https://api.dataservices.doverfs.com:8863/sites/"+i+"?api-version=2017-06-15"
    headers = {
            'Authorization': 'Bearer '+Token,
            }
    response = requests.get(API_JSON, headers=headers)
    res=response.json()

    name = (res['name'])
    cust_id = str(res['customer']['id'])
    cust_name = str(res['customer']['name'])
    site_status = str(res['siteActivityStatus'])

    insert="INSERT INTO sitedetails (name,customer_Id, customer_name, siteActivityStatus) VALUES (%s,%s,%s,%s)"
    val= (unicode(name).encode("utf8"),unicode(cust_id).encode("utf8"), unicode(cust_name).encode("utf8"), unicode(site_status))
    mycursor.execute(insert,val)
    mydb.commit()

#devices
    for k in res['devices'][:]:
        id = str(k['id'])
        deviceType = str(k['deviceType'])
        pumpState   = str(k['pumpState'])
        isActive    = str(k['isActive'])

        insert="INSERT INTO sitedetails (name,customer_Id, customer_name, siteActivityStatus,devices_id,devices_deviceType,devices_pumpState,devices_IsActive) VALUES (%s,%s,%s,%s,%s,%s,%s,%s)"
        val= (unicode(name).encode("utf8"),unicode(cust_id).encode("utf8"), unicode(cust_name).encode("utf8"), unicode(site_status).encode("utf8"),unicode(id).encode("utf8"),unicode(deviceType).encode("utf8"),unicode(pumpState).encode("utf8"),unicode(isActive).encode("utf8"))
        mycursor.execute(insert,val)
        mydb.commit()


        if "fuelingPoints" in k:
            for m in (k["fuelingPoints"][:]):
                if m["deviceId"]:
                   deviceId = str(m['deviceId'])
                if m["name"]:
                   fname = str(m['name'])
                insert="INSERT INTO sitedetails (name,customer_Id, customer_name, siteActivityStatus,devices_id,devices_deviceType,devices_pumpState,devices_IsActive,devices_fuelingPoints_DeviceId, devices_fuelingPoints_Name) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"
                val= (unicode(name).encode("utf8"),unicode(cust_id).encode("utf8"), unicode(cust_name).encode("utf8"), unicode(site_status).encode("utf8"),unicode(id).encode("utf8"),unicode(deviceType).encode("utf8"),unicode(pumpState).encode("utf8"),unicode(isActive).encode("utf8"),unicode(deviceId).encode("utf8"),unicode(fname).encode("utf8") )
                mycursor.execute(insert,val)
                mydb.commit()


