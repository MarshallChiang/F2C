import google_api_utilities
from dateutil.parser import parse
import datetime
import pandas as pd 
import pymysql
import boto3 
import json
import sys
import os

s3 = boto3.client('s3')
config = json.loads(s3.get_object(Bucket='shopbackconfig', Key='F2C_config.json')['Body'].read().decode('utf-8'))
lambda_resource = boto3.client('lambda', region_name='ap-northeast-1')
database = pymysql.connect(host=os.environ['mysql_domain'], port=3306, user='Ops', password=os.environ['mysql_password'], database='ShopBack', charset='utf8', autocommit=True)

class file_handler :

    def __init__(self, id, file) :
        self.config = config[id]
        self.file = file 
        
    def to_df(self) :      
        selector = []
        df = pd.read_csv(self.file, encoding='utf-8-sig')
        if len(df) == 0 :
            self.df = pd.DataFrame()
            return 
        df.columns = map(str.lower, df.columns)
        if self.config['sheet_definition'] :
            df.query(self.config['sheet_definition'], inplace=True)
        for c in self.config['column_definition'] :
            old, new = c['old'], c['new']
            applier = eval(c['applier']) if c['applier'] else lambda x : x
            df[new] = df[old].apply(applier)
            selector.append(new)
        self.df = df[selector]

    def gen_dataset(self, offset=100) :
        output = self.df.to_dict('r')
        i = 0
        while True :
            if i * offset > len(output) : 
                break
            yield output[i*offset : (i+1)*offset]
            i += 1
        
def main(even) :
    print('[%s] Start Processing Stream %s (%s)'%(datetime.datetime.now().strftime('%Y-%m-%d'), even['id'], config[even['id']]['name']))
    cursor = database.cursor(pymysql.cursors.DictCursor)
    cursor.execute('''SELECT max(send_at) AS s FROM Log_ReportProcess WHERE offer_id = "%s"'''%even['id'])
    epoch_after = cursor.fetchone()['s']
    epoch_after = (epoch_after + datetime.timedelta(hours=9)).strftime('%Y-%m-%d %H:%M:%S') if epoch_after else epoch_after
    mail_service = google_api_utilities.gmailHandler('./credentials/token/mail_token.json')
    messages = mail_service.filter_Mails(from_user=config[even['id']]['from'], epoch_after=epoch_after)
    if messages :
        for m in messages : #Get message ids from config filter.
            for f in mail_service.get_attachment(m['id'], filename_regex=config[even['id']]['filename_regex'], store_dir='./tmp/') : #Get attatchments from each id.
                source = file_handler(even['id'], f['file_path'])
                try :
                    source.to_df()
                except Exception as e :
                    raise ValueError(e)
                for offset in source.gen_dataset() :
                    data = lambda_resource.invoke(
                    FunctionName = 'arn:aws:lambda:ap-northeast-1:286841818544:function:ShopBack_F2C_Worker',
                    InvocationType = 'Event',
                    Payload = json.dumps({
                        'data': offset,
                        'id' : even['id'],
                        'send_at' : f['internalDate']
                        })
                    )
                    print('[%s] Lambda Process %s Units '%(datetime.datetime.now().strftime('%Y-%m-%d'), len(offset)))
    print('[%s] Finish Processing Stream %s (%s)'%(datetime.datetime.now().strftime('%Y-%m-%d'), even['id'], config[even['id']]['name']))

if __name__ == '__main__' : 
    stream = sys.argv[1]
    main({'id':stream})
