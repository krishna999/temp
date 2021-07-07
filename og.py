import json
from tabulate import tabulate
from helper import FileHelper, S3Helper
from ta import TextAnalyzer, TextMedicalAnalyzer, TextTranslater
from trp import *
import numpy as np
# Import writer class from csv module
from csv import writer
import pandas as pd
import os
import uuid
import boto3
from decimal import Decimal
import time 
import hashlib

UNSUPPORTED_DATE_FORMAT = "UNSUPPORTED_DATE_FORMAT"
DOCTEXT = "docText"
KVPAIRS = "KVPairs"
PUBLIC_PATH_S3_PREFIX= "public/"
SERVICE_OUTPUT_PATH_S3_PREFIX = "output/"
TEXTRACT_PATH_S3_PREFIX = "textract/"
COMPREHEND_PATH_S3_PREFIX = "comprehend/"
TABLES_PATH_S3_PREFIX = "tables/"

s3 = boto3.resource('s3')


class OutputGenerator:
    def __init__(self, documentId, response, bucketName, objectName, fileName, forms, tables,outputPath):
        self.documentId = documentId
        self.response = response
        self.bucketName = bucketName
        self.objectName = objectName
        self.forms = forms
        self.tables = tables
        self.outputPath = outputPath
        self.fileName = fileName
        
        self.document = Document(self.response)

    def _findNearest(self, array, value):
        return min(array, key=lambda x:abs(x-value))

    def _getClosestLine(self, lines, table_width, table_top_val, table_left_val):
        line_arr = []

        for line in lines:
            if(line.geometry.boundingBox.top < table_top_val):
                if(line.geometry.boundingBox.left > table_left_val and (line.geometry.boundingBox.left < table_left_val + (table_width / 4))):
                    line_arr.append(line.geometry.boundingBox.top)
                    
        if (len(line_arr) > 0):
            return self._findNearest(line_arr, table_top_val)
        else:
            return None

    def _outputWords(self, page, p):
        csvData = []
        for line in page.lines:
            for word in line.words:
                csvItem = []
                csvItem.append(word.id)
                if (word.text):
                    csvItem.append(word.text)
                else:
                    csvItem.append("")
                csvData.append(csvItem)
        csvFieldNames = ['Word-Id', 'Word-Text']
        FileHelper.writeCSV("{}-page-{}-words.csv".format(self.fileName, p),
                            csvFieldNames, csvData)

    def _outputText(self, page, p):
        text = page.text
        FileHelper.writeToFile("{}-page-{}-text.txt".format(self.fileName, p),
                               text)

        textInReadingOrder = page.getTextInReadingOrder()
        FileHelper.writeToFile(
            "{}-page-{}-text-inreadingorder.txt".format(self.fileName, p),
            textInReadingOrder)

    def _outputForm(self, page, p):
        csvData = []
        key_value_pairs = {}
        csvFieldNames = ['Key', 'Value']
        csvData.append(csvFieldNames)
        for field in page.form.fields:
            csvItem = []
            if(field.key):
                csvItem.append(field.key.text)
            else:
                csvItem.append("")
            if(field.value):
                csvItem.append(field.value.text)
            else:
                csvItem.append("")
            csvData.append(csvItem)
            if ":" in csvItem[0]:
                csv_key = csvItem[0].split(":")[0]
            else:
                csv_key = csvItem[0]
            key_value_pairs[csv_key] = csvItem[1]
            
        
        FileHelper.writeCSVRaw("/tmp/key-values.csv", csvData)
        df = pd.read_csv("/tmp/key-values.csv", delimiter=',',error_bad_lines=False)
        
        
        opath = "{}{}page-{}-forms.csv".format(self.outputPath,TEXTRACT_PATH_S3_PREFIX, p)
        S3Helper.writeCSV(csvFieldNames, csvData, self.bucketName, opath)
        
        # Write to DynamoDB table
        
        api = self.document.pages[0].form.searchFieldsByKey('UWI')[0].value
        if len(page.form.searchFieldsByKey('Report Date')) > 0:
            report_date = page.form.searchFieldsByKey('Report Date')[0].value
        else:
            report_date = 'Missing'
            
        df['Id'] = [uuid.uuid4() for _ in range(len(df.index))]
        df['API/UWI'] = api
        df['Report Date'] = report_date
        df['Operator'] = 'From S3 Metadata'
        
        # Get the service resource.
        dynamodb = boto3.resource('dynamodb')
        dynamodb_client = boto3.client('dynamodb')
        
        dd_table_name = "key-values".replace(" ", "-").replace("(", "-").replace(")", "-").replace("&", "-").replace(",", " ").replace(":", "-")

        print("DynamoDB table name is {}".format(dd_table_name))
    
        # Create the DynamoDB table.
        try:
    
            existing_tables = dynamodb_client.list_tables()['TableNames']
    
            if dd_table_name not in existing_tables:
                table = dynamodb.create_table(
                    TableName=dd_table_name,
                    KeySchema=[
                        {
                            'AttributeName': 'Id',
                            'KeyType': 'HASH'
                        }
                    ],
                    AttributeDefinitions=[
                        {
                            'AttributeName': 'Id',
                            'AttributeType': 'S'
                        },
                    ],
                    ProvisionedThroughput={
                            'ReadCapacityUnits': 5,
                            'WriteCapacityUnits': 5
                    }
                )
                # Wait until the table exists, this will take a minute or so
                table.meta.client.get_waiter('table_exists').wait(TableName=dd_table_name)
                # Print out some data about the table.
                print("Table successfully created. Item count is: " + str(table.item_count))
        #except dynamodb_client.exceptions.ResourceInUseException:
        except:
            # do something here as you require
            print('Table {} already exists'.format(dd_table_name))
            pass
        
        try:
            table = dynamodb.Table(dd_table_name)
            df = df.astype(str)
            for index, row in df.iterrows():
                data=row.to_dict()
                table.put_item(Item=data)
            print('Finished adding rows to dynamodb table {}'.format(dd_table_name))
        except:
            print('Exception occured when adding attributes to dynamodb')
            pass
    
        return key_value_pairs

    def _outputTable(self, page, p, tables):  
        api = self.document.pages[0].form.searchFieldsByKey('UWI')[0].value.text
        reportdate_matching = [s for s in page.textlines if "Report Date:" in s]
        if len(page.form.searchFieldsByKey('Report Date')) > 0:
            report_date = page.form.searchFieldsByKey('Report Date')[0].value
        elif len(reportdate_matching) > 0:
            report_date = reportdate_matching[0].split(':')[1]
            print('!!!! Report Date found in Lines !!!!')
        else:
            report_date = 'Missing'
        t = 1
               
        for table in page.tables:
            # Find the table name
            if (self._getClosestLine(page.lines, table.geometry.boundingBox.width, table.geometry.boundingBox.top, table.geometry.boundingBox.left) is None):
                break
            else:
                line_top_val = self._getClosestLine(page.lines, table.geometry.boundingBox.width, table.geometry.boundingBox.top, table.geometry.boundingBox.left)
                
            table_name = ""
            dd_table_name = ""
            for line in page.lines:
                if line.geometry.boundingBox.top == line_top_val:
                    table_name = line.text + '.csv'
                    dd_table_name = line.text
                    print('table name is {} with header top value {} and header left value{}'.format(line.text, line.geometry.boundingBox.top, line.geometry.boundingBox.left))
                    
            
            table_name = table_name.replace('/', '--')

            from pathlib import Path
            
            objectname_hash = str(int(hashlib.sha256(self.objectName.encode('utf-8')).hexdigest(), 16) % 10**8)
            
            table_file = Path('/tmp/' + objectname_hash + table_name) 
            # /tmp/Time Log.csv
            csvData = []
            
            print("Current set of tables for {} are {}".format(self.objectName, tables))
            if table_name in tables:
                item_num = 0
                for item in tables[table_name]:
                    csvData.append(item)
                
                for row in table.rows:
                    item_num = item_num + 1
                    csvRow = []
                    # Header row
                    if (item_num > 1):
                        csvRow.append(uuid.uuid4())
                        csvRow.append(api)
                        csvRow.append(report_date)
                        csvRow.append(self.objectName)
                        csvRow.append('Page #{}'.format(page.pagenum))

                        for cell in row.cells:
                            csvRow.append(cell.text)
                        csvData.append(csvRow)
            else:
                row_num = 0
                for row in table.rows:
                    row_num = row_num + 1
                    csvRow = []
                    # Header row
                    if (row_num == 1):
                        csvRow.append('Id')
                        csvRow.append('API/UWI')
                        csvRow.append('Report Date')
                        csvRow.append('File Name')
                        csvRow.append('Page Number')
                    else:
                        csvRow.append(uuid.uuid4())
                        csvRow.append(api)
                        csvRow.append(report_date)
                        csvRow.append(self.objectName)
                        csvRow.append('Page #{}'.format(page.pagenum))
                
                    for cell in row.cells:
                        csvRow.append(cell.text)
                    
                    csvData.append(csvRow)
            
            tables[table_name] = csvData
            
            if(table_file.exists()):
                os.remove(table_file)
            
            # write to file
            FileHelper.writeCSVRaw(table_file, csvData)

            bucket = s3.Bucket(self.bucketName)
            key = "{}{}{}".format(self.outputPath, TABLES_PATH_S3_PREFIX, table_name)
            bucket.upload_file(str(table_file), key)
            
            # Write to DynamoDB table
            df = pd.read_csv(table_file, error_bad_lines=False)

            # Get the service resource.
            dynamodb = boto3.resource('dynamodb')
            dynamodb_client = boto3.client('dynamodb')
            
            dd_table_name = dd_table_name.replace(" ", "-").replace("(", "-").replace(")", "-").replace("&", "-").replace(",", " ").replace(":", "-").replace('/', '--')

            print("DynamoDB table name is {}".format(dd_table_name))

            # Create the DynamoDB table.
            try:

                existing_tables = dynamodb_client.list_tables()['TableNames']

                if dd_table_name not in existing_tables:
                    table = dynamodb.create_table(
                        TableName=dd_table_name,
                        KeySchema=[
                            {
                                'AttributeName': 'Id',
                                'KeyType': 'HASH'
                            }
                        ],
                        AttributeDefinitions=[
                            {
                                'AttributeName': 'Id',
                                'AttributeType': 'S'
                            },
                        ],
                        ProvisionedThroughput={
                                'ReadCapacityUnits': 5,
                                'WriteCapacityUnits': 5
                        }
                    )
                    # Wait until the table exists, this will take a minute or so
                    table.meta.client.get_waiter('table_exists').wait(TableName=dd_table_name)
                    # Print out some data about the table.
                    print("Table successfully created. Item count is: " + str(table.item_count))
            # except dynamodb_client.exceptions.ResourceInUseException:
            except:
                # do something here as you require
                print('Exception occured!')
                pass
            
            try:
                table = dynamodb.Table(dd_table_name)
                df = df.astype(str)
                for index, row in df.iterrows():
                    data=row.to_dict()
                    table.put_item(Item=data)
            except:
                print("Exception occured when adding rows to dynamodb during Table generation")
                pass

            t = t + 1

    def _convertFloatToDecimal(s): return decimal.Decimal(str(round(float(s), 2)))
    
    def _outputTablePretty(self, page, p, table_format='github'):
        for table_number, table in enumerate(page.tables):
            rows_list = list()
            for row in table.rows:
                one_row = list()
                for cell in row.cells:
                    one_row = one_row + [cell.text]
                rows_list.append(one_row)
            pretty_table = tabulate(rows_list, tablefmt=table_format)
            FileHelper.writeToFile(
                "{}-page-{}-table-{}-tables-pretty.txt".format(
                    self.fileName, p, table_number), pretty_table)

    def run(self):

        if (not self.document.pages):
            return

        #FileHelper.writeToFile("{}-response.json".format(self.fileName),
        #                       json.dumps(self.response))

        print("Total Pages in Document: {}".format(len(self.document.pages)))

        p = 1
        tables = {}
        for page in self.document.pages:
            file = "{}-page-{}-response.json".format(self.fileName, p)
            local_file = "/tmp/" + file
            FileHelper.writeToFile(
                local_file,
                json.dumps(page.blocks))
                
            bucket = s3.Bucket(self.bucketName)
            key = "{}{}{}".format(self.outputPath, TEXTRACT_PATH_S3_PREFIX, file)
            bucket.upload_file(local_file, key)

            #self._outputWords(page, p)

            #self._outputText(page, p)

            if (self.forms):
                self._outputForm(page, p)

            if (self.tables):
                self._outputTable(page, p, tables)
                # self._outputTablePretty(page, p)
                
            #self.generateInsights( True, False, False,'us-east-1')
                         
            p = p + 1
            

    def _insights(self, start, subText, sentiment, syntax, entities,
                  keyPhrases, ta):
        # Sentiment
        sentiment = ta.getSentiment(subText)

        # Syntax
        #dsyntax = ta.getSyntax(subText)
        #for dst in dsyntax['SyntaxTokens']:
        #    dsyntaxRow = []
        ##    dsyntaxRow.append(dst["PartOfSpeech"]["Tag"])
        #    dsyntaxRow.append(dst["PartOfSpeech"]["Score"])
        #    dsyntaxRow.append(dst["Text"])
        #    dsyntaxRow.append(int(dst["BeginOffset"]) + start)
        #    dsyntaxRow.append(int(dst["EndOffset"]) + start)
        #    syntax.append(dsyntaxRow)

        # Entities
        dentities_obj = ta.getEntities(subText)
        dentities = json.dumps(dentities_obj)

        # Key Phrases
        dkeyPhrases_obj = ta.getKeyPhrases(subText)
        dkeyPhrases = json.dumps(dkeyPhrases_obj)
        
        return dentities, dkeyPhrases, sentiment

    def _medicalInsights(self, start, subText, medicalEntities, phi, tma):
        # Entities
        dentities = tma.getMedicalEntities(subText)
        for dent in dentities['Entities']:
            dentitiesRow = []
            dentitiesRow.append(dent["Text"])
            dentitiesRow.append(dent["Type"])
            dentitiesRow.append(dent["Category"])
            dentitiesRow.append(dent["Score"])
            dentitiesRow.append(int(dent["BeginOffset"]) + start)
            dentitiesRow.append(int(dent["EndOffset"]) + start)
            medicalEntities.append(dentitiesRow)

        phi.extend(tma.getPhi(subText))

    def _generateInsightsPerDocument(self, page, p, insights, medicalInsights,
                                     translate, ta, tma, tt):

        maxLen = 2000
        api = self.document.pages[0].form.searchFieldsByKey('UWI')[0].value.text
        if len(page.form.searchFieldsByKey('Report Date')) > 0:
            report_date = page.form.searchFieldsByKey('Report Date')[0].value.text
        else:
            report_date = 'Missing'
            
        for table in page.tables:
            if (self._getClosestLine(page.lines, table.geometry.boundingBox.top, table.geometry.boundingBox.left) is None):
                break
            else:
                line_top_val = self._getClosestLine(page.lines, table.geometry.boundingBox.top, table.geometry.boundingBox.left)
                
            table_name = ""
            dd_table_name = ""
            for line in page.lines:
                if line.geometry.boundingBox.top == line_top_val:
                    table_name = line.text
            
            if ('Time Log' in table_name or 'Daily Operation' in table_name):
                all_insights = []
                row_insights = {}
                row_num = 0
                for row in table.rows:
                    row_num = row_num + 1
                    text_for_analysis = ""
                    for cell in row.cells:
                        text_for_analysis = text_for_analysis + cell.text
                    
                    text = text_for_analysis  
                    start = 0
                    sl = len(text)
                    sentiment = []
                    syntax = []
                    entities = []
                    keyPhrases = []
                    medicalEntities = []
                    phi = []
                    translation = ""
                    
                    sub_insights = []
                    while (start < sl):
                        end = start + maxLen
                        if (end > sl):
                            end = sl
                    
                        subText = text[start:end]
                    
                        if (insights):
                            entities, keyPhrases, sentiment = self._insights(start, text, sentiment, syntax, entities,
                                           keyPhrases, ta)
                            
                            sub_insights.append(entities)
                            sub_insights.append(keyPhrases)
                            
                        start = end
                    
                    row_insights['Id'] = str(uuid.uuid4())
                    row_insights['API UWI'] = api
                    row_insights['Report Date'] = report_date
                    row_insights['Text'] = text
                    row_insights['Entites-KeyPhrases'] = str(sub_insights)
                    row_insights['SentimentDetails'] = str(sentiment)
                    row_insights['Sentiment'] = sentiment['Sentiment']
                    row_insights['File Name'] = self.fileName
                    all_insights.append(row_insights)
                    

                bucket = s3.Bucket(self.bucketName)
                if (insights):
                    file = "{}-page-{}-{}-insights.json".format(self.fileName, p, table_name)
                    file_fullpath = "/tmp/" + file
                    with open(file_fullpath, 'w') as fp:
                        json.dump(all_insights, fp)
 
                    key = "{}{}{}".format(self.outputPath, TEXTRACT_PATH_S3_PREFIX, file)
                    bucket.upload_file(file_fullpath, key)
                    
                    dd_table_name = 'insights'
                    
                    # Get the service resource.
                    dynamodb = boto3.resource('dynamodb')
                    dynamodb_client = boto3.client('dynamodb')
                    
                    dd_table_name = dd_table_name.replace(" ", "-").replace("(", "-").replace(")", "-").replace("&", "-").replace(",", " ").replace(":", "-").replace('/', '--')
        
                    print("DynamoDB table name is {}".format(dd_table_name))
        
                    # Create the DynamoDB table.
                    try:
        
                        existing_tables = dynamodb_client.list_tables()['TableNames']
        
                        if dd_table_name not in existing_tables:
                            table = dynamodb.create_table(
                                TableName=dd_table_name,
                                KeySchema=[
                                    {
                                        'AttributeName': 'Id',
                                        'KeyType': 'HASH'
                                    }
                                ],
                                AttributeDefinitions=[
                                    {
                                        'AttributeName': 'Id',
                                        'AttributeType': 'S'
                                    },
                                ],
                                ProvisionedThroughput={
                                        'ReadCapacityUnits': 5,
                                        'WriteCapacityUnits': 5
                                }
                            )
                            # Wait until the table exists, this will take a minute or so
                            table.meta.client.get_waiter('table_exists').wait(TableName=dd_table_name)
                            # Print out some data about the table.
                            print("Table successfully created. Item count is: " + str(table.item_count))
                    except dynamodb_client.exceptions.ResourceInUseException:
                        # do something here as you require
                        print('Exception occured!')
                        pass
                    
                    table = dynamodb.Table(dd_table_name)
                    for item in all_insights:
                        time.sleep(2.4)
                        table.put_item(Item=item)
                    
                    

    def generateInsights(self, insights, medicalInsights, translate, awsRegion):

        print("Generating insights...")

        if (not self.document.pages):
            return

        ta = TextAnalyzer('en', awsRegion)
        tma = TextMedicalAnalyzer(awsRegion)

        tt = None
        if (translate):
            tt = TextTranslater('auto', translate, awsRegion)

        p = 1
        for page in self.document.pages:
            self._generateInsightsPerDocument(page, p, insights,
                                              medicalInsights, translate, ta,
                                              tma, tt)
            p = p + 1
