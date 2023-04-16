def hello_pubsub(event, context):
    """
    Background Cloud Function to be triggered by Pub/Sub.
    Required dependencies : bigquery
    Configure environment variables while creating and deploying cloud function
    for bigquery dataset and table
    dataset: customer_db_entry
    table:customer_urls
    
    """
    import base64
    import json
    import os,sys
    import pandas as pd
    import re
    import numpy as np
    from google.cloud import bigquery

    if 'data' in event:
        log_data= base64.b64decode(event['data']).decode('utf-8')

        log_data_str = str(log_data)

        customer_id = re.findall(r'C_...', log_data_str)
        for ids in customer_id:
          ids=ids

        link_regex = re.compile('((https?):((//)|(\\\\))+([\w\d:#@%/;$()~_?\+-=\\\.&](#!)?)*)', re.DOTALL)
        links = re.findall(link_regex, log_data_str)
        for lnk in links:
          lnk=lnk

        data = {'customer_id': ids,
                'url': lnk[0]
                }

        df = pd.DataFrame([data])

        # Load client
        client = bigquery.Client(project='bq-db-383212')

        # Define table name, in format dataset.table_name
        table = 'customer_db_entry.customer_urls'

        # Load data to BQ
        job = client.load_table_from_dataframe(df, table)
        
    else:
        print('Hello World')
