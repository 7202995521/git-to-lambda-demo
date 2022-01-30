import boto3
import os
from boto3.dynamodb.conditions import Key
import json
from decimal import Decimal

dynamodb = boto3.resource('dynamodb')
table = dynamodb.Table(os.environ['TableName'])

client = boto3.client('iot-data', region_name='us-east-1')

# create a class DecimalEncoder that casts Decimals into floats and then displays them in desired format.

class DecimalEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, Decimal):
            return float(obj)
        return json.JSONEncoder.default(self, obj)

def lambda_handler(event, context):

  if event["id"] == "1":
    
    # add new item in database
    
    data = table.put_item(
      Item={
          'student_name': 'Student9',
          'subject_title': 'Scun55hhvvhh'
      }
    )
    print("Item succesfully added in database")
    
  elif event["id"] == "2":
    
    # read a item by it's primary key from database
    
    data = table.get_item(
            Key={
                'student_name' : 'Student2',
                'subject_title': 'Maths'
                }
        )
                
    if 'Item' in data:
        print(json.dumps(data['Item'],cls=DecimalEncoder,indent = 4))
    else:
        print("Requested data is not found in database also here")
  
  elif event["id"] == "3":
    
    # update a item in database
    
    data = table.update_item(
            Key={
                'student_name': 'Student3',
                'subject_title': 'Science'
                },
            UpdateExpression="set age = :g",
            ExpressionAttributeValues={
                ':g': 11
                },
            ReturnValues="UPDATED_NEW"
            )
    print(json.dumps(data['Item'],cls=DecimalEncoder,indent = 4))
  
  elif event["id"] == "4":
    
    # delete a item from database
    
    data = table.delete_item(
            Key={
              'student_name': 'Student3',
              'subject_title': 'Science'
            }
      )
    print("Item succesfully deleted from database")
    
  elif event["id"] == "5":
    
    # scan database data using secondary index
    
      data = table.scan(
          IndexName = 'grade-index'
      )
    
    # scan database data by using ProjectionExpression
    # here we just scanning for all student_name & subject_title in the database
    
    # data = table.scan(
    #    ProjectionExpression="student_name, subject_title"
    #  )
    
      print(json.dumps(data['Items'],cls=DecimalEncoder,indent = 4))
    
  elif event["id"] == "6":
    
    # query database data using global secondary index
    
    data = table.query(
        IndexName='grade-index',
        KeyConditionExpression=Key('grade').eq('C')
    )
    
    print(json.dumps(data['Items'],cls=DecimalEncoder,indent = 4))
    
  elif event["id"] == "7":
    
    # write a data using batch write in database
    
    with table.batch_writer() as batch:
      
      batch.put_item(
        
        Item = {
          'student_name': 'Student7',
          'subject_title': 'Hindi'
        }
      )
      
      batch.put_item(
        
        Item = {
          'student_name': 'Student8',
          'subject_title': 'Urdu'
        }
      )
    print("Items added succesfully using batch let's have a look")
    
  elif event["id"] == "8":
    
    # update a device shadow
    
      data = {"state" : { "desired" : { "temp_rate" : 80 }}}
      mypayload = json.dumps(data, indent = 2).encode('utf-8')
      response = client.update_thing_shadow(
          thingName = 'demodevice',
          shadowName='Classic Shadow',
          payload = mypayload
      )
      print("Shadow update succesfully")

  elif event["id"] == "9":
    
    # get a thing device shadow
    
      response = client.get_thing_shadow(
          thingName = 'demodevice',
          shadowName='Classic Shadow'
      )
      print(response)

   elif event["id"] == "10":
    
    # delete thing device shadow
    
      response = client.delete_thing_shadow(
          thingName = 'demodevice',
          shadowName='Classic Shadow'
      )
      print("Shadow update succesfully")