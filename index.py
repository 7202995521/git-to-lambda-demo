import boto3
import os
from boto3.dynamodb.conditions import Key
import json
from decimal import Decimal

dynamodb = boto3.resource('dynamodb')
table = dynamodb.Table(os.environ['TableName'])

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
          'student_name': 'Student6',
          'subject_title': 'English(F.L)',
          'grade': 'B',
          'age': 16
      }
    )
    print("Item succesfully added in database Have yoe look or not")
    
  elif event["id"] == "2":
    
    # read a item by it's primary key from database
    
    data = table.get_item(
            Key={
                'student_name' : 'Student1',
                'subject_title': 'Maths'
            }
        )
                
    if 'Item' in data:
        print(json.dumps(data['Item'],cls=DecimalEncoder,indent = 4))
    else:
        print("Requested data is not found in database")
  
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
          'subject_title': 'Hindi',
          'grade': 'F',
          'age': 14
        }
      )
      
      batch.put_item(
        
        Item = {
          'student_name': 'Student8',
          'subject_title': 'Urdu',
          'grade': 'F',
          'age': 20
        }
      )
    print("Items added succesfully using batch")