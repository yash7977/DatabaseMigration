import mysql.connector
import json
from pymongo.database import DBRef
#from bson.dbref import DBRef
mydb = mysql.connector.connect(
  host="localhost",
  user="yash",
  passwd="Ultra9911",
  database="db"
)

n=int(input("Enter number of tables: "))
for i in range(0,n):
    name = str(input("Enter name of table: "))
    mycursor = mydb.cursor()
    #mycursor.execute("SELECT * FROM Iris")
    #myresult = mycursor.fetchall()
    #for x in myresult:
    #    print(x)
    curr = mydb.cursor()
    curr.execute("SELECT * FROM {}".format(name))
    data = curr.fetchall()
    # print(json.dumps(data, default=json_serial))

    data_json = []
    header = [j[0] for j in curr.description]
    # data = mycursor.fetchall()
    for k in data:
        data_json.append(dict(zip(header, k)))
    # print(data_json)

    with open('{}.json'.format(name), 'w') as fp:
        json.dump(data_json, fp)

    from pymongo import MongoClient
    client = MongoClient('localhost', 27017)
    db = client['test']
    #db.col.insert(data_json)
    db[name].insert(data_json)
    #col.insert(data_json)

#####################find child unique values########################
if n >= 2:
    dbref = str(input("Use DBRef?"))
    if dbref == "yes":
        child = str(input("Enter name of child table:"))
        parent = str(input("Enter name of Parent table:"))
    else:
        exit()
child_name = str(input("Enter child target column name:"))
child_curr = mydb.cursor()
child_curr.execute("SELECT DISTINCT {0} FROM {1}".format(child_name,child))
child_data = child_curr.fetchall()
print(child_data)
child_find=[]
print(len(child_data))
for i in range(0,len(child_data)):
    child_find.append(child_data[i][0])
    print(child_find[i])
#####################find parent unique values##################
parent_name = str(input("Enter parent target column name:"))
parent_curr = mydb.cursor()
parent_curr.execute("SELECT DISTINCT {0} FROM {1}".format(parent_name,parent))
parent_data = child_curr.fetchall()
print(type(parent_data[0][0]))
parent_find=[]
print(len(parent_data))
for l in range(0,len(parent_data)):
    parent_find.append(parent_data[l][0])
    print(parent_find[l])

###################Mapping DBRef###########################
mapp = {}
for i in range(0,len(child_find)):
    value = input('Enter mapping for {}'.format(child_find[i]))
    mapp[child_find[i]]=value
print(mapp)
mapp_key = list(mapp.keys())
mapp_value = list(mapp.values())
print(mapp_key)
print(mapp_value)
######################Mongodb dbref#####################################

from pymongo import MongoClient

client = MongoClient('localhost', 27017)
db = client['test']
child_curr = []
for i in child_find:
    child_curr.append(db[child].find_one({child_name:i}))

print(child_curr)

parent_curr = []
for i in parent_find:
    parent_curr.append(db[parent].find({parent_name:i}))

print(child_curr[1]['_id'])

#for i in parent_curr:
#    print(child_curr[i]['_id'])
'''for i in parent_curr:
    for j in i:
        j[parent_name] = DBRef(collection = child, id="{}".format(child_curr[i['_id']]))
        db[parent].save(j)'''


for i in parent_curr:
    for j in i:
        if str(j[parent_name]) in mapp_value:
            index = mapp_value.index(str(j[parent_name]))
            j[parent_name] = DBRef(collection=child, id="{}".format(child_curr[index]['_id']))
            db[parent].save(j)

#for i in parent_curr:
#    for j in i:
#        if j['Species'] in mapp_value == True:
#            print("true")
#        else:
#            print("flase")

'''d['Species'] = DBRef(collection = "title",id = setosa_find['_id'])
    db.iris.save(d)'''