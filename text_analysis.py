import boto3
from pprint import pprint
from csv import reader
import pandas as pd
import os,sys,uuid
import time
from urllib.parse import unquote_plus

def split_list(datatitle):
    half = len(datatitle)//2
    list_1 = datatitle[:half]
    list_2 = datatitle[half:]
    return list_1,list_2

def lambda_handler(event, context):
    
    try:
        s3_client = boto3.client('s3')
        bucket_name = "output-reddit-data"
        s3_file_name = "output.csv"

        resp = s3_client.get_object(Bucket=bucket_name, Key=s3_file_name)
        df_s3_data = pd.read_csv(resp['Body'], sep=',')
        title = list(df_s3_data['title'])
        
        analysis_list_1,analysis_list_2 = split_list(title)
        analysis_string_1 = ' '.join(analysis_list_1)
        analysis_string_2 = ' '.join(analysis_list_2)
        
        comprehend = boto3.client("comprehend")
        
        #Extracting entities using comprehend
        entities_1 = comprehend.detect_entities(Text = analysis_string_1, LanguageCode = "en")
        entities_2 = comprehend.detect_entities(Text = analysis_string_2, LanguageCode = "en")

        #fetching only the entities with organization type        
        entity_text = []
        for entities in entities_1['Entities']:
            if entities['Type'] == 'ORGANIZATION' and entities['Score'] >= 0.95:
                entity_text.append(entities['Text'])
                
        for entities in entities_2['Entities']:
            if entities['Type'] == 'ORGANIZATION' and entities['Score'] >= 0.95:
                entity_text.append(entities['Text'])
        
        # print(entity_text)
        # print('unique entities are ',set(entity_text))
        Final_Entity = []
        Final_results =athena_connection(set(entity_text),Final_Entity)
        
        #Extracting keyphrase using comprehend
        final_sentiment = []
        keyphrase_1 = comprehend.detect_sentiment(Text = analysis_string_1, LanguageCode = "en")
        keyphrase_2 = comprehend.detect_sentiment(Text = analysis_string_2, LanguageCode = "en")
        
        final_sentiment.append(keyphrase_1['Sentiment'])
        final_sentiment.append(keyphrase_2['Sentiment'])
                
        result_response1 = "Here are the stocks redditors are talking about.."
        result_response2 = " " + str(set(Final_results))
        
        if final_sentiment[0] == final_sentiment[1]:
            result_response3 = "Sentiment of the overall Reddit Conversation seems to be: " + str(final_sentiment[0])
        else:
            result_response3 = " Sentiment of the overall Reddit Conversation seems to be: " + "Mixed"

        combined_text = result_response1 + result_response2 + result_response3
    
    except Exception as err:
        print(err)
      
    result = {
               "sessionAttributes": {},
                  "dialogAction": {
                     "type": "Close",
                      "fulfillmentState": "Fulfilled",
                         "message": {
                             "contentType": "PlainText",
                             "content": combined_text
                         }
                     }
                 }
            
    return result
    
    
# Check if Entity  is valid Entity  from athena query 
      
       
def athena_connection(list,Final_Entity):
    # athena constant
    DATABASE = 'reddit_stock_data'
    TABLE = 'data_dictionary_ticker'
    # S3 constant
    S3_OUTPUT = 's3://output-athena-results/'
    S3_BUCKET = 'output-athena-results'

    # number of retries
    RETRY_COUNT = 20
            

    COLUMN1,COLUMN2 = 'symbol','name'

    for list_each in list :
        # print(list_each)
        # print(type(list_each))
        if list_each.isalpha():
            # created query AND 'symbol' like % %  OR 'Name' like % %"
            query = "SELECT count(*) FROM %s.%s where %s like '%s'  OR %s like '%s';" % (DATABASE, TABLE,COLUMN1,'%'+list_each+'%',COLUMN2,'%'+list_each+'%')
            # print(query)
       
            # athena client
            client = boto3.client('athena')
    
            # Execution
            response = client.start_query_execution(
                QueryString=query,
                QueryExecutionContext={
                    'Database': DATABASE
                                        },
                        ResultConfiguration={
                    'OutputLocation': S3_OUTPUT,
                    }
                )
            
              
            # get query execution id
            query_execution_id = response['QueryExecutionId']
            # print('query_execution_id is',query_execution_id)
          
    
            # get execution status
            for i in range(1, 1 + RETRY_COUNT):
    
                # get query execution
                query_status = client.get_query_execution(QueryExecutionId=query_execution_id)
                query_execution_status = query_status['QueryExecution']['Status']['State']
                # print(query_execution_status)
                # print(query_status)
    
                if query_execution_status == 'SUCCEEDED':
                    print("STATUS:" + query_execution_status)
                    results = client.get_query_results(QueryExecutionId=query_execution_id)
                    final_result = (results['ResultSet']['Rows'])
                    final_exe = int(final_result[1]['Data'][0]['VarCharValue'])
                    # print(final_exe)
                    # print(results)
                    #print(type(results))
                    if (final_exe) >= 1:
                        Final_Entity.append(str(list_each))
                        #print('Final Entity is',Final_Entity)
                        # print(list_each)
                    break
         
                    if query_execution_status == 'FAILED':
                        raise Exception("STATUS:" + query_execution_status)
                    
                    else:
                        print("STATUS:" + query_execution_status)
                        time.sleep(i)
            # else:
            #     client.stop_query_execution(QueryExecutionId=query_execution_id)
            #     raise Exception('TIME OVER')
            # break
    
    return Final_Entity
    
       
        
        
