from datetime import datetime, timedelta
import time
import sys
import pandas as pd
import numpy as np
import psycopg2
import requests
import json
from lib import red

today = datetime.today()
currentDate = today.strftime('%Y-%m-%d')
first = today.replace(day=1)
lastMonth = first - timedelta(days=1)
loading_month = datetime(lastMonth.year, lastMonth.month,1).strftime('%Y-%m-%d')
datem = datetime(lastMonth.year, lastMonth.month, 1).strftime('%Y_%m') #yyyy_mm

dbname = 'billing'                                              
host = 'billing.cn8tpncpyezz.eu-west-1.redshift.amazonaws.com'  
port = 5439                                                     
user = red.config['user']                                    
password = red.config['password']                            

con = psycopg2.connect(dbname= dbname, host=host, 
                       port= port, user= user, password= password)

def main(table_name, ingestion_date, month=loading_month, message="Task completed."):
    """This function loads and checks premium subscriptions, then sends a notification about the job status."""

    # Load premium subscriptions
    f = open('/reporting_sys/scripts/monthly_premium_subscriptions', 'r')
    query = f.read()
    query = query.format(loading_month = month)

    try: 
        cur = con.cursor()
        cur.execute(query)

    except:
        cur.execute('rollback')
        raise TypeError('Query failed.')

    # check load status 

    query = "select count(*) from {table_name} where date(inserted_on) = '{ingestion_date}';".format(table_name=table_name,ingestion_date=ingestion_date)

    try: 
        cur = con.cursor()
        cur.execute(query)

    except:
        print('Query failure')
        cur.execute('rollback')

    res = cur.fetchall()
    if res[0][0] == 0:
        raise TypeError('Ingested 0 rows.')
    else:
        print("Ingested {num} of rows into table {table_name} on '{ingestion_date}'.".format(num=res[0][0],table_name=table_name,ingestion_date=ingestion_date))

    # check number 
    def outlier_flag(delta, upper, lower):
        if delta > upper:
            return 'flag'
        elif delta < lower:
            return 'flag'
        else:
            return 'no flag'

    query = "select report_month,\
       country_code,\
       sum(gross_revenue_usd) as gross_revenue_usd,\
       sum(net_revenue_usd) as net_revenue_usd\
    from dwh.monthly_premium_subscriptions\
    where true\
    and report_month >= current_date - interval '13 months'\
    and country_code in (select country_code from public.ref_country where is_mena is TRUE)\
    group by 1,2;"

    try: 
        cur = con.cursor()
        cur.execute(query)
        result = cur.fetchall()
        colnames = [desc[0] for desc in cur.description]
        df = pd.DataFrame(result,columns=colnames)

        if df.shape[0] <= 10:
            raise ValueError('Subscriptions data is incomplete.')
        else:
            pass 
    except:
        cur.execute('rollback')
        raise TypeError('Query failed.')

    # transformations 
    df.sort_values(by=['report_month','country_code'], ascending=True,inplace=True)
    df['gross_revenue_usd_prev_month'] = df.sort_values(by=['report_month'],ascending =True).groupby(['country_code'])['gross_revenue_usd'].shift(1)
    df = df[df['gross_revenue_usd_prev_month'].isna() != True]
    df['gross_revenue_delta'] = df['gross_revenue_usd']/df['gross_revenue_usd_prev_month'] - 1

    outlier_df = pd.DataFrame(columns=['country_code','ub','lb'])
        
    for i in df['country_code'].unique():

        delta_mom = df[df['country_code']==i]['gross_revenue_delta'].astype(float)
        delta_mom = np.sort(delta_mom)
        q1 = np.percentile(delta_mom, 25, interpolation = 'midpoint')
        q3 = np.percentile(delta_mom, 75, interpolation = 'midpoint')
        iqr = q3 -  q1
        lower_bound = q1 - 1.5 * iqr
        upper_bound = q3 + 1.5 * iqr

        row = {'country_code':i,'ub':upper_bound,'lb':lower_bound}

        outlier_df = outlier_df.append(row, ignore_index=True)

    df = df.merge(outlier_df,how='inner',on='country_code')

    df['is_outlier'] = df.apply(lambda x:outlier_flag(x['gross_revenue_delta'],x['ub'],x['lb']),axis=1)


    if df.query("is_outlier=='flag' and report_month=='{loading_month}'".format(loading_month=month)).shape[0] > 0:
        raise ValueError('Spike detected.')
    else:
        print('Revenue check passed.') 


    # send notification
    headers = {
        'Content-Type': 'application/json',
    }
    params = (
        ('access_token', 'DQVJ1MkdsNHB5TTExa01SZAk5nXzJ1aUhiQmRJakdZAOVljZA2VfeTlsU3lzYWJsSjhOVnlhelU5azM5M0JsRllHaG44UGdTNTA2RGs4OUpzNW84N0l5aWNzWVFQRlYwdjZA0RzFKdXNjYTFvalY3S2JoZAnAyQ2ZAmbzc4TWZAEUkttS0RlVHdfZAmx0TkFnN1JNX0JTQmw4em1mdEFmei1vT2wxZA01xdjcybjRLcmlDWnRHb25PelBibWVIbzk3bU50eXNoUWsxNEd3'),
    )
    message = str(message)[:2000].replace('"', "'").replace('\n', '')

    data = f'{{"recipient":{{"thread_key":100070351712439}},"message":{{"text":"{message}"}}}}'

    response = requests.post('https://graph.facebook.com/v3.0/me/messages', headers=headers, params=params, data=data)
    
    if response.status_code == 400:
        message = message[:200]
        data = f'{{ "recipient":{{"id":"100070351712439" }}, "message":{{ "text":"{message}" }}}}'
        response = requests.post(
            'https://graph.facebook.com/v2.6/me/messages', headers=headers, params=params, data=data)    

    return response.status_code

if __name__ == "__main__":
    main(table_name = 'public.temp_li_monthly_premium_subscriptions', 
        ingestion_date = currentDate, 
        month=loading_month, 
        message="Premium subsription task completed.")

