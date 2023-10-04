import json
from decimal import Decimal

import mysql.connector

import datetime

import pysftp

mydb = mysql.connector.connect(
  host="51.68.93.61",
  port=4922,
  user="root",
  password="Sfax_2018",
  database="anavid_prod"
)


mycursor = mydb.cursor()

mycursor.execute("SELECT * FROM espaces where id_client = 48;")

myresult = mycursor.fetchall()

yesterday = datetime.datetime.now() - datetime.timedelta(1)
date_reporting = datetime.datetime.strftime(yesterday, '%Y-%m-%d')

for x in myresult:

  mycursor = mydb.cursor()
  sql_req="SELECT id,name  FROM cameras where id_espace ="+str(x[0])+" ;"
  mycursor.execute(sql_req)
  myresult1 = mycursor.fetchall()
  print("-------------")
  filename=str(x[2]).replace(" ","")+"_"+date_reporting+".json"
  f = open(filename, "w")

  print('Espace ' + str(x[2]))
  data_espace = []
  for y in myresult1:
    sql_req1 = "SELECT count(*) as total_cnt, count(CASE WHEN age='0-9' THEN 1 END), count(CASE WHEN age='10-17' THEN 1 END), count(CASE WHEN age='18-29' THEN 1 END), count(CASE WHEN age='30-39' THEN 1 END), count(CASE WHEN age='40-49' THEN 1 END), count(CASE WHEN age='50-59' THEN 1 END), count(CASE WHEN age='60-100' THEN 1 END), count(CASE WHEN gender='Male' THEN 1 END) as male_cnt, count(CASE WHEN gender='Female' THEN 1 END) as female_cnt FROM entry WHERE DATE(visit_date) = %s AND id_camera = %s;"
    mycursor.execute(sql_req1, (date_reporting, y[0]))
    myresult2 = mycursor.fetchall()
    #

    sql_req2 = "SELECT DATE_FORMAT(e.visit_date, '%Hh') AS hour, COUNT(*) as count, SUM(CASE WHEN e.gender = 'Male' THEN 1 ELSE 0 END) as gender_men, SUM(CASE WHEN e.gender = 'Female' THEN 1 ELSE 0 END) as gender_women, SUM(CASE WHEN e.age = '0-9' THEN 1 ELSE 0 END) as age_0_9, SUM(CASE WHEN e.age = '10-17' THEN 1 ELSE 0 END) as age_10_17, SUM(CASE WHEN e.age = '18-29' THEN 1 ELSE 0 END) as age_18_29, SUM(CASE WHEN e.age = '30-39' THEN 1 ELSE 0 END) as age_30_39, SUM(CASE WHEN e.age = '40-49' THEN 1 ELSE 0 END) as age_40_49, SUM(CASE WHEN e.age = '50-59' THEN 1 ELSE 0 END) as age_50_59, SUM(CASE WHEN e.age = '60-100' THEN 1 ELSE 0 END) as age_60_100 FROM entry AS e INNER JOIN cameras AS c ON e.id_camera = c.id WHERE DATE(e.visit_date) = %s and e.id_camera = %s GROUP BY DATE_FORMAT(e.visit_date, '%Hh') ORDER BY hour;"
    mycursor.execute(sql_req2, (date_reporting, y[0]))
    myresult3 = mycursor.fetchall()

    column_names = [i[0] for i in mycursor.description]

    rows_as_dict = [dict(zip(column_names, row)) for row in myresult3]

    for row in rows_as_dict:
      for key, value in row.items():
        if isinstance(value, Decimal):
          row[key] = int(value)

    for z in myresult2:
      xx = {
        "camera": y[1],
        "date": date_reporting,
        "visit_Count": z[0],
        "Age":
          {
          "0-9": z[1],
          "10-17": z[2],
          "18-29": z[3],
          "30-39": z[4],
          "40-49": z[5],
          "50-59": z[6],
          "60-100": z[7]
          },
          "Gender": {
          "Homme": z[8],
          "Femme": z[9],

          },
          "detailsByHour":rows_as_dict
          }
      data_espace.append(xx)
  print(str(data_espace))
  f.write(str(data_espace))
  f.close()
  cnopts = pysftp.CnOpts()
  cnopts.hostkeys = None

  #with pysftp.Connection('141.95.232.21', username='ftpanavid', password='anavid**2023', cnopts=cnopts) as sftp:
  #    sftp.chdir('zen')
  #    sftp.put(filename)