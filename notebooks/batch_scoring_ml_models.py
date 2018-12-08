# Databricks notebook source
# MAGIC %md # Batch Scoring ML Model with Spark Pandas UDF
# MAGIC 
# MAGIC After you train your ML model, how do you use it to perform batch scoring of a very large dataset?  How would you do this in parallel to minimize scoring time?.
# MAGIC If you trained your model with Spark ML then this is not a problem as Spark ML model is designed to score Spark distributed data objects. However, if Spark ML is not what you used due to its limitation and your model happens to be SKlearn or a Tensorflow model or  is in the form of published web service (your own model or a cognitive API) then there's no straight forward way to do this.
# MAGIC  
# MAGIC In this post, I'll show two examples of how batch scoring can be applied using the relatively new Pandas UDF function in Spark 2.x:
# MAGIC Batch scoring from cognitive API (or your own ML model published as API)
# MAGIC Batch scoring from persisted SKlearn model

# COMMAND ----------

# MAGIC %md ## Scoring from persisted sklearn model

# COMMAND ----------

#This is the example to load a sklearn model from pkl file and use it to score mini batches of data from Spark Streaming.
#But the dataset does not need to be streaming, it can be any Spark dataset
df_stream = spark.readStream.format("delta").table("events")
df_stream.withWatermark("aiv_epoch_start", "10 minutes").registerTempTable("amazon_msess_events")
from pyspark.sql.types import *
from pyspark.sql.functions import pandas_udf, PandasUDFType,window
import datetime

import pandas as pd
import numpy as np
from sklearn.externals import joblib
import pandas.errors

jdbcHostname = 'DBhost'
jdbcUsername =''
jdbcPassword = ''
table = ''

jdbcDatabase = "DBname"
jdbcPort = 1433
jdbcUrl = "jdbc:sqlserver://{0}:{1};database={2}".format(jdbcHostname, jdbcPort, jdbcDatabase)
print(jdbcUrl)
connectionProperties = {
  "user" : jdbcUsername,
  "password" : jdbcPassword,
  "driver" : "com.microsoft.sqlserver.jdbc.SQLServerDriver"
}



req_eval = spark.sql("select ng_start_time, ng_stop_time, cast(ng_stop_time as long) as start_time,cast(ng_stop_time as long) as stop_time, (cast(ng_stop_time as long) - cast(ng_start_time as long)) as duration, total_count, manifest_requests, avg_request_time,avg_tdwait_time,max_request,max_tdwait,  max_mbps, min_mbps,avg_mbps,avg_rtt,bytes,total_2xx,total_3xx,total_4xx,total_5xx,td_lte_1s_count,tc_lte_1s_count,	td_gt_1s_lte_2s_count,	tc_gt_1s_lte_2s_count,	td_gt_2s_lte_4s_count,	tc_gt_2s_lte_4s_count,	td_gt_4s_lte_6s_count,	tc_gt_4s_lte_6s_count,	td_gt_6s_lte_8s_count,	tc_gt_6s_lte_8s_count,	td_gt_8s_lte_10s_count,	tc_gt_8s_lte_10s_count,	td_gt_10s_lte_30s_count,	tc_gt_10s_lte_30s_count,	td_gt_30s_lte_60s_count,	tc_gt_30s_lte_60s_count,	td_gt_60s_count	tc_gt_60s_count,asn, cc, state,cast(max_err as string) as max_err, (case when aiv_num_rebuffers >0 then 1 else 0 end) as aiv_num_rebuffers  from amazon_msess_events")


schema = StructType([
    StructField("ng_start_time", TimestampType()),
    StructField("ng_stop_time", TimestampType()),
    StructField("cc", StringType()),
    StructField("state", StringType()),
    StructField("asn", DoubleType()),

    StructField("predicted_buffering", DoubleType()),
    StructField("actual_buffering", DoubleType()),

])

@pandas_udf(schema, functionType=PandasUDFType.GROUPED_MAP)


def predict_buffering(panda_buffer):
  from sklearn.externals import joblib
  model = joblib.load('/dbfs/mnt/regr.joblib') 

  #if datasize is not long enough, return default result
  #score threshold whether to score as positive
  threshold = 0.45
  states=['SC','AZ','LA','MN','NJ','DC','OR','unknown','VA','RI','KY','WY','NH','MI','NV','WI','ID','CA','CT','NE','MT','NC','VT','MD','DE','MO','IL','ME','WA','ND','MS','AL','IN','OH','TN','IA','NM','PA','SD','NY','TX','WV','GA','MA','KS','CO','FL','AK','AR','OK','UT','HI']
  countries =['cr','pr','us','vi','cl','il','ro','jp','ag','vn','pl','vg','za','sk','mu','pt','ke','ni','sg','ae','iq','hk','be','qa','bz','gb','me','ec','sa','co','tr','de','is','tt','lu','br', 'im', 'gt', 'bb', 'jo', 'es', 'hr', 'eu', 'dj', 'kr', 'it', 'uy', 'af', 'pe', 'vc', 'ar', 'sv', 'jm', 'ph', 'nl', 'bo', 'gp', 'hn', 'hu', 'ca', 'al', 'bm', 've', 'gu','ee','nz','si','gr','aw','ru','mt','th','cw','ch','ma','gh','do','lt','ht','pa','no','bg','cy','at','cz','ua','dm','mx','bs','ai','ky','fr','se','ie','dk','gd','id','bh','gg','gy','fi']

  errors =[412, 206, 500, 504, 502, 503, 400, 403, 404, 591, 408, 200, 260, 499]
  ori_states = panda_buffer['state'].values.tolist()
  ori_cc=panda_buffer['cc'].values.tolist()
  panda_buffer['ng_start_time'] = pd.to_datetime(panda_buffer['start_time'], unit='ms', utc=True)
  panda_buffer['ng_stop_time'] = pd.to_datetime(panda_buffer['stop_time'],unit='ms', utc=True)
  start_time = panda_buffer['ng_start_time'].values.tolist()
  stop_time = panda_buffer['ng_stop_time'].values.tolist()
  del panda_buffer['ng_start_time']
  del panda_buffer['ng_stop_time']
  del panda_buffer['start_time']
  del panda_buffer['stop_time']
#   del panda_buffer['window']
  panda_buffer['state']= panda_buffer['state'].astype('category',categories=states)
  temp = pd.get_dummies(panda_buffer['state'], prefix='state')
  panda_buffer = pd.concat([panda_buffer, temp], axis = 1)
  del panda_buffer['state'], temp

  panda_buffer['cc']= panda_buffer['cc'].astype('category',categories=countries)
  temp = pd.get_dummies(panda_buffer['cc'], prefix='cc')
  panda_buffer = pd.concat([panda_buffer, temp], axis = 1)
  del panda_buffer['cc'], temp

  panda_buffer['max_err']= panda_buffer['max_err'].astype('category',categories=errors)
  temp = pd.get_dummies(panda_buffer['max_err'], prefix='max_err')
  panda_buffer = pd.concat([panda_buffer, temp], axis = 1)
  del panda_buffer['max_err'], temp

  # panda_buffer['asn']= panda_buffer['asn'].astype('category',categories=asns)
  # temp = pd.get_dummies(panda_buffer['asn'], prefix='asn')
  # panda_buffer = pd.concat([panda_buffer, temp], axis = 1)
  # del panda_buffer['asn'], temp
  asn =panda_buffer['asn'].values.tolist()
  y = panda_buffer['aiv_num_rebuffers'].values.tolist()
  X_dataset=  panda_buffer.copy()
  del X_dataset['aiv_num_rebuffers']
  X = X_dataset.values

  y_pred= model.predict(X).tolist()
  def score(y_pred, threshold=0.5):
    out = []
    for item in y_pred:
      if item[1] >=threshold:
        out.append(1)
      else:
        out.append(0)
    return out
  out_pred = model.predict_proba(X).tolist()
  y_pred = score(out_pred, threshold)


  return pd.DataFrame({'ng_start_time': start_time,'ng_stop_time': stop_time, 'cc':ori_cc,'state':ori_states,'asn':asn,  'predicted_buffering': y_pred, 'actual_buffering': y})







agg= req_eval.groupby(window("ng_start_time", "5 minutes", "10 seconds"))
output =agg.apply(predict_buffering)

#here you can set the mode = "overwrite" (inside JDBC) if you only want to see latest data in output table. Otherwise can you set the mode ='append', then in the query of client tool, you need to select latest record sort by start_time, end_time
#side note: if you want to ouput a regular Dataframe, not stream dataframe to SQL table, use syntax: yourDF.write.jdbc(url=jdbcUrl, table=table, mode="append", properties=connectionProperties)
#IMPORTANT: Please make sure you create output table in advance in SQL Server. There's a bug for non-numeric column to be included as colum index if you let the driver create table automatically.
output.registerTempTable("prediction_out")

  

# COMMAND ----------

# MAGIC %md ## Batch Scoring from ML rest API 

# COMMAND ----------


#This Example use the Azure's Anomaly Finder API to score against Spark data in a batch fashion.
#By doing this, we can score records in parallel for supposed to be real time API.

#This function has input of a spark data frame which include mulitple variables aligned in the same timeline to test for anomalies simultaneously
# @columns: This is the list of numeric column to detect anomaly
# @cat_columns: this the list of categorical columns to include for query purpose
# @Groupby_cols: the list of columns for grouping by
def anomaly_df_finder(df,columns, cat_columns,groupby_cols,max_ratio=0.25,sens=95, rare_as_exception =False,higher_value_better_cols=None ):
  from pyspark.sql.types import StructType,StructField,StringType,BooleanType, TimestampType,DoubleType
  from pyspark.sql.functions import pandas_udf, PandasUDFType
  import datetime
  from pyspark.sql.functions import window

  import pandas as pd
  import json
  import requests
  import numpy as np
  import time

  schema = StructType([
      StructField("timestamp", TimestampType()),
      StructField("col_anomaly_count", DoubleType()),

  ])
  for cat_column in cat_columns:
    schema.add(cat_column,StringType() )

  for column in columns:
    schema.add(column+'_IsAnomaly_Pos',BooleanType() )
    schema.add(column+'_IsAnomaly_Neg',BooleanType() )
    schema.add(column+'_is_anomaly',BooleanType() )
    schema.add(column+'_upper_value',DoubleType() )
    schema.add(column+'_lower_value',DoubleType() )
    schema.add(column+'_expected_value',DoubleType() )
    schema.add(column+'_value',DoubleType() )

  @pandas_udf(schema, functionType=PandasUDFType.GROUPED_MAP)


  def detect_anomaly(df):
    MaxAnomalyRatio=max_ratio
    Sensitivity=sens

  #   columns = ['avg_rtt', 'avg_tdwait']
    output_dict = {'timestamp': df['Timestamp']}
    output_dict['col_anomaly_count']=0
    for cat_column in cat_columns:
      output_dict[cat_column]= df[cat_column]

    #if datasize is not long enough, return default result
    if df.shape[0] <12:
      for column in columns:
        output_dict[column+'_IsAnomaly_Pos']=rare_as_exception
        output_dict[column+'_IsAnomaly_Neg']=False
        output_dict[column+'_is_anomaly']=rare_as_exception
        output_dict[column+'_upper_value']=-999
        output_dict[column+'_lower_value']=-999
        output_dict[column+'_value']=df[column]
        output_dict[column+'_expected_value']=df[column]
      output_dict['col_anomaly_count']=output_dict['col_anomaly_count']+np.array(output_dict[column+'_IsAnomaly_Pos'])

      return pd.DataFrame(data=output_dict)




    endpoint = 'https://westus2.api.cognitive.microsoft.com/anomalyfinder/v2.0/timeseries/entire/detect'
    subscription_key = '' #Key for version 2

    def detect(endpoint, subscription_key, request_data):
      headers = {'Content-Type': 'application/json', 'Ocp-Apim-Subscription-Key': subscription_key}
      response = requests.post(endpoint, data=json.dumps(request_data), headers=headers)
      #Dealing with threshold exceeding exception, retry util we can call the api
      while response.status_code == 429:
        time.sleep(1)
        response = requests.post(endpoint, data=json.dumps(request_data), headers=headers)

      if response.status_code == 200:
          return json.loads(response.content.decode("utf-8"))

      else:
  #         print(response.status_code)
          raise Exception(str(response.status_code)+":" +response.text + json.dumps(request_data))

    #Loop for each column in 
#     df.sort_values(by= 'Timestamp', inplace=True)
    
    for column in columns:
      df_out = df[['Timestamp', column]].copy()
      df_out["Value"] = df_out[column]
      del df_out[column]
      df_out.Timestamp = pd.to_datetime(df.Timestamp, unit ='ms',utc =True)
      json_data = df_out.to_json(orient='records',date_format ='iso',date_unit ='s')
      json_loaded = json.loads(json_data)
      json_loaded = {"Granularity":"minutely", "CustomInterval":5,"MaxAnomalyRatio": MaxAnomalyRatio, "Sensitivity": Sensitivity, "Series":json_loaded }
    #   json_loaded = {"Period": None, "Points":json_loaded }

      try:
        result = detect(endpoint, subscription_key, json_loaded)
      except Exception as e:
        output_dict[column+'_IsAnomaly_Pos']=rare_as_exception
        output_dict[column+'_IsAnomaly_Neg']=False
        output_dict[column+'_is_anomaly']=rare_as_exception
        output_dict[column+'_upper_value']=-999
        output_dict[column+'_lower_value']=-999
        output_dict[column+'_value']=df[column]
        output_dict[column+'_expected_value']=df[column]
        continue


      output_dict[column+'_IsAnomaly_Pos']=result['IsPositiveAnomaly']
      output_dict[column+'_IsAnomaly_Neg']=result['IsNegativeAnomaly']
      output_dict[column+'_is_anomaly']=result['IsAnomaly']
      if higher_value_better_cols and column in higher_value_better_cols:
        output_dict['col_anomaly_count']=output_dict['col_anomaly_count']+np.array(result['IsNegativeAnomaly'])
      else:
        output_dict['col_anomaly_count']=output_dict['col_anomaly_count']+np.array(result['IsPositiveAnomaly'])

#       output_dict['col_anomaly_count']=output_dict['col_anomaly_count']+np.array(result['IsAnomaly'])

      output_dict[column+'_upper_value']=result['UpperMargins']
      output_dict[column+'_lower_value']=result['LowerMargins']
      output_dict[column+'_value']=df[column]
      output_dict[column+'_expected_value']=result['ExpectedValues']
#       output_dict[column+'_upper_value'] = np.array(result['ExpectedValues'])+(100-Sensitivity)*np.array(result['UpperMargins'])
#       output_dict[column+'_lower_value'] = np.array(result['ExpectedValues'])-(100-Sensitivity)*np.array(result['LowerMargins']) 
      output_dict[column+'_upper_value'] = np.array(result['UpperMargins'])
      output_dict[column+'_lower_value'] = np.array(result['LowerMargins'])  

    output = pd.DataFrame(data=output_dict)

    return output
  agg=df.groupby(groupby_cols)
  df_output =agg.apply(detect_anomaly)

  return df_output