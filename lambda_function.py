
######################################################################################################################
 #  Copyright 2020 Amazon.com, Inc. or its affiliates. All Rights Reserved.                                           #
 #                                                                                                                    #
 #  Licensed under the Apache License, Version 2.0 (the License). You may not use this file except in compliance    #
 #  with the License. A copy of the License is located at                                                             #
 #                                                                                                                    #
 #      http://www.apache.org/licenses/LICENSE-2.0                                                                    #
 #                                                                                                                    #
 #  or in the 'license' file accompanying this file. This file is distributed on an 'AS IS' BASIS, WITHOUT WARRANTIES #
 #  OR CONDITIONS OF ANY KIND, express or implied. See the License for the specific language governing permissions    #
 #  and limitations under the License.                                                                                #
 #####################################################################################################################

import json
import os
import boto3
import time
from helper import AwsHelper
from og import OutputGenerator

def getJobResults(api, jobId):

    pages = []

    client = AwsHelper().getClient('textract', 'us-east-1')
    response = client.get_document_analysis(JobId=jobId)
    pages.append(response)
    print("Resultset page recieved: {}".format(len(pages)))
    nextToken = None
    if('NextToken' in response):
        nextToken = response['NextToken']
        print("Next token: {}".format(nextToken))

    while(nextToken):
        try:
            if(api == "StartDocumentTextDetection"):
                response = client.get_document_text_detection(JobId=jobId, NextToken=nextToken)
            else:
                response = client.get_document_analysis(JobId=jobId, NextToken=nextToken)

            pages.append(response)
            print("Resultset page recieved: {}".format(len(pages)))
            nextToken = None
            if('NextToken' in response):
                nextToken = response['NextToken']
                print("Next token: {}".format(nextToken))

        except Exception as e:
            if(e.__class__.__name__ == 'ProvisionedThroughputExceededException'):
                print("ProvisionedThroughputExceededException.")
                print("Waiting for few seconds...")
                time.sleep(5)
                print("Waking up...")


    return pages

def processRequest(request):

    output = ""

    print("Request : {}".format(request))

    jobId = request['jobId']
    documentId = request['jobTag']
    jobStatus = request['jobStatus']
    jobAPI = request['jobAPI']
    bucketName = request['bucketName']
    outputBucketName = request['outputBucketName']
    objectName = request['objectName']
    
    
    directory = objectName.split('/')

    upload_path = ''
    for subdirectory in directory:
        if subdirectory != directory[-1]:
            upload_path += (subdirectory+'/')
            
    file_name = directory[-1]
    
    file_name_no_ext = file_name.rsplit( ".", 1 )[ 0 ]
    
    upload_path = upload_path + file_name_no_ext + '/'
    
    pages = getJobResults(jobAPI, jobId)

    print("Result pages recieved: {}".format(len(pages)))

    detectForms = False
    detectTables = False
    if(jobAPI == "StartDocumentAnalysis"):
        detectForms = True
        detectTables = True


    # outputPath = '{}{}/{}'.format(PUBLIC_PATH_S3_PREFIX,documentId,SERVICE_OUTPUT_PATH_S3_PREFIX)
    print("Generating output for DocumentId: {} and storing in {}".format(file_name,upload_path))

    opg = OutputGenerator(documentId, pages, outputBucketName, objectName, file_name_no_ext, detectForms, detectTables, upload_path)
    opg_output = opg.run()


    output = "Processed -> Document: {}, Object: {}/{} processed.".format(documentId, bucketName, objectName)
    
   
    return {
        'statusCode': 200,
        'body': output
    }

def lambda_handler(event, context):

    print("event: {}".format(event))
    
    for record in event['Records']:
        message = json.loads(record['Sns']['Message'])
        print("Message: {}".format(message))

        request = {}
    
        request["jobId"] = message['JobId']
        request["jobTag"] = message['JobTag']
        request["jobStatus"] = message['Status']
        request["jobAPI"] = message['API']
        request["bucketName"] = message['DocumentLocation']['S3Bucket']
        request["objectName"] = message['DocumentLocation']['S3ObjectName']
        request["outputBucketName"] = os.environ['OUTPUT_BUCKET']
    
        processRequest(request)

