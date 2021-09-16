import pymysql
import requests
import datetime
import boto3
import json
import os

s3 = boto3.client('s3')
config = json.loads(s3.get_object(Bucket='YourBucketName', Key='F2C_config.json')['Body'].read().decode('utf-8'))
database = pymysql.connect(host=os.environ['mysql_domain'], port=3306, user='Ops', password=os.environ['mysql_password'], database='ShopBack', charset='utf8', autocommit=True)


def lambda_handler_data_call(even, context) :
    process_at = datetime.datetime.now()
    failed_list = []
    params_set = even['data']
    if even['id'] in ('2792', '2793') :
        params_set = taobao_tw_handler(params_set)
        print('[INFO] Taobao Pre-Parse Finished.')
    print('[INFO] Start Processing.')
    if params_set :
        for p in params_set :
            r = requests.get(os.environ['api_url'], params=p)
            if r.text != os.environ['success_message'] :
                p['error_message'] = r.text
                failed_list.append(p)
        print('%s \nFailed Counts : %s\nFailed List : %s'%(config[even['id']]['name'], len(failed_list), failed_list))
        finished_at  = datetime.datetime.now()
        cursor = database.cursor(pymysql.cursors.DictCursor)
        cursor.execute('''INSERT INTO Log_ReportProcess 
        (offer_id, offer_name, send_at, process_time, process_counts, accurate_rate)
        VALUES
        ('%s', '%s', '%s', '%s', '%s', '%s') '''%(
            even['id'], config[even['id']]['name'], datetime.datetime.fromtimestamp(int(even['send_at'][:-3])), (finished_at-process_at).seconds, len(params_set), round((1 - len(failed_list)/len(params_set)),2))
            )
        print('[INFO] Finish MYSQL Insertion.')

def taobao_tw_handler(params) :
    cursor = database.cursor(pymysql.cursors.DictCursor)
    output = list()
    for p in params :
        print(p)
        c = cursor.execute(''' INSERT INTO taobao_taiwan_conversions (order_id, report_status, datetime)
        VALUES ('%s', '%s', '%s') ON DUPLICATE KEY UPDATE 
        order_id = '%s', report_status = '%s'
        '''%(p['adv_sub'], p['status'], p['datetime'], p['adv_sub'], p['status']))
        print(c)
        if c == 1 :
            output.append(p)
    print('[INFO] Eligible Taobao Taiwan Orders Counts : %s'%len(output))
    return output