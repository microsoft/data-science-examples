// Databricks notebook source
// MAGIC %md ## Parameterized Job for running encryption

// COMMAND ----------

// MAGIC %md 
// MAGIC This notebook is a template to illustrate how encryption at scale can be parameterized so that it can be run with  data factory scheduling or Databricks job scheduling. The main use is to read json param file that contain mapping between dataset name and its schema definition & encryption definition so that we can automate: 
// MAGIC 1. Reading datasets of different source types: flat file (csv and other), streaming from eventhubs and decode from raw into structured format
// MAGIC 2. Automatically pick up fields to encrypt
// MAGIC 3. Write to a designated delta table
// MAGIC Note:
// MAGIC - The structurization of data sources is currently done in a simplistic way, e.g. support string, numeric and date column. For other complex types like timestamp, reformat of date etc...future effort is needed to enhance. The main focus of this is to get the structure out in string and apply encryption
// MAGIC - For parallel running of multiple notebook like this where each notebook process one dataset/table, one can either define parallel execution steps in ADF or use databricks workflow to run notebooks conrrently (https://docs.databricks.com/user-guide/notebooks/notebook-workflows.html#run-multiple-notebooks-concurrently). In that case, the step to load json file should be placed at the master notebook and this notebook is used as worker notebook to run a specific dataset
// MAGIC 
// MAGIC Parameters for the job/notebook
// MAGIC These are parameters for the notebook that can be set at run time either by Devop engineer or by another master notebook/job
// MAGIC 1. schema_mapping_json_path: Path to the Json file that contain mapping between dataset names, fields, whether or not it needs encryption, data type. See below for an example of a json file
// MAGIC 2. Dataset name: name of the dataset that will be used to look up for schema and encryption mapping in the json file
// MAGIC 3. Ingestion type: Streaming or Batch. Streaming is used when the source is EventHub. Batch is used when raw data is copied to a landing folder in ADLS Gen 2 and this notebook picks up from there
// MAGIC 4. Input data folder: For batch processing from landing zone. This can be set with wildcat to copy data recursively
// MAGIC 5. Output data folder: In case you let the job create the delta table automatically in batch mode, set the output folder so that encrypted data is created there. For batch mode, this may be needed as there can be multiple checkpoint location before final append to the target table. In case of streaming, the final table can be used directly as Streaming supports checkpoint location
// MAGIC 6. Table name: output table name. Can be an existing table name
// MAGIC 7. Eventhub Account: the account name of the eventhub in case a streaming dataset is used
// MAGIC 8. Secret scope/secret: name of secret scope and key to retrieve EH's key 
// MAGIC 9. EH topic: topic to read data from. As a deviation from original design due to the use of Delta table, it's recommended to write streaming encryption output to a delta table instead of EH's topic. The reason is EH delta table support change capture. So the next job can just subscribe to a Delta table to read new changes. This may be faster than reading from a EH's topic.

// COMMAND ----------

import org.apache.spark.sql.types._
import scala.collection.JavaConversions._

// import json
// from json import JSONDecoder
// from collections import OrderedDict

dbutils.widgets.text("schema_name", "", "Schema name")

dbutils.widgets.text("eh_topic", "", "EH Raw topic") //Devops will provide
dbutils.widgets.text("eh_en_topic", "", "EH Encrypted topic") //Devops will provide

dbutils.widgets.text("secret_scope", "", "EH Secret scope") //Scope will be same
dbutils.widgets.text("secret_key", "", "EH Raw Secret key") //Devops will provide
// dbutils.widgets.text("access_policy_key", "", "EH Raw Access Policy key") //Devops will provide
// dbutils.widgets.text("en_access_policy_key", "", "EH Encrypted Access Policy key") //Devops will provide

dbutils.widgets.text("en_secret_key", "", "EH Encrypted Secret key")//Devops will provide
dbutils.widgets.text("eh_account", "", "Event Hub Account") //Devops will provide


dbutils.widgets.text("checkpointLocation", "dbfs:/mnt/cp/testcp", "Check point Location")

// #Fetch data from parameters

val schema_name = dbutils.widgets.get("schema_name") //Devops will provide used for both ADF and EH

val ingestion_type = dbutils.widgets.get("ingestion_type") //EH =streaming or ADF = batch 
val checkpointLocation = dbutils.widgets.get("checkpointLocation") //where do we defined this ? 

//Error table path to capture bad data 
val error_logtbl_path="dbfs:/mnt/tbl/error_tbl"

// #Getting detail for Streaming
val eh_account = dbutils.widgets.get("eh_account")
val eh_topic=dbutils.widgets.get("eh_topic")
val eh_en_topic =dbutils.widgets.get("eh_en_topic")

val secret_scope=dbutils.widgets.get("secret_scope")
val secret_key=dbutils.widgets.get("secret_key")
val en_secret_key=dbutils.widgets.get("en_secret_key")

val eh_key = dbutils.secrets.get(secret_scope,secret_key)
val access_policy_name ="SendListenSharedAccesskey"
val en_access_policy_name ="EncryptedSendListenSharedAccesskey"

val eh_en_key =dbutils.secrets.get(secret_scope,en_secret_key)

//Look up the schema detail from 
val jdbc_secret_password_key ="edsschemadb-password"
val jdbc_secret_username_key = "edsschemadb-username"
val jdbc_secret_hostname_key= "edsschemadb-hostname"
val jdbcUsername = dbutils.secrets.get(secret_scope,jdbc_secret_username_key)

val jdbcPassword =dbutils.secrets.get(secret_scope,jdbc_secret_password_key)
val jdbcHostname=  dbutils.secrets.get(secret_scope,jdbc_secret_hostname_key)
val jdbcPort = 1433
val jdbcDatabase ="edsschemadb"

val jdbcUrl = s"jdbc:sqlserver://${jdbcHostname}:${jdbcPort};database=${jdbcDatabase}"

// Create a Properties() object to hold the parameters.
import java.util.Properties
val connectionProperties = new Properties()

connectionProperties.put("user", s"${jdbcUsername}")
connectionProperties.put("password", s"${jdbcPassword}")

val driverClass = "com.microsoft.sqlserver.jdbc.SQLServerDriver"
connectionProperties.setProperty("Driver", driverClass)

val schema_df = spark.read.jdbc(url=jdbcUrl,table="(select schema_content from schema_registry where schema_name='"+schema_name+"') as tbl", properties=connectionProperties)

val field_encryption_df = spark.read.jdbc(url=jdbcUrl, table="(select attribute_name, encr_key from sensitive_field_mst A, schema_registry B where A.schema_id = B.schema_id and B.schema_name='"+schema_name+"') as tbl", properties=connectionProperties)

//HOW DO WE COME UP WITH SCHEMA BASED on JSON 

val schema = DataType.fromJson(schema_df.take(1)(0)(0).toString).asInstanceOf[StructType]
val field_encryption_list = field_encryption_df.collectAsList()
val field_encryption_map = field_encryption_list.map(_.toSeq).map({ a => a.get(0) -> a.get(1) }).toMap


// print(ingestion_type)
// print(eh_account)
// print(eh_topic)
// print(secret_scope)
// print(secret_key)


// COMMAND ----------

// #Below, To fix bad data, I will use something like this
// #select CASE WHEN Description rlike "^[a-zA-Z ]+$" THEN Description ELSE NULL END logic_column from chicago_crimes_delta
// #To identify 
// #This is the regex map in the form of encryption_key: regex_expression
// // # regex_map ={
// // #   'name2':'^[A-Z]',
// // #   'ssn':'[A-Z!@#$*<>]',
// // #   'dob':'[A-Za-z]'
// // # #  'govt_id':[govt_id: [!"#()$%&*+,./:<=>?@^_a-z\\[\]{|}~;]
  
// // # }

//For special characters Scala require \\\\ in the regex

val regex_map =Map(
  "name2" ->"[^a-zA-Z\\\\d:\\\\s]",
  
  "ssn" -> "[A-Z!@#$*<>]",
  "dob" -> "[A-Za-z]"
  
)

def fullFlattenSchema(schema: StructType): Seq[String] = {
  def helper(schema: StructType, prefix: String): Seq[String] = {
    val fullName: String => String = name => if (prefix.isEmpty) name else s"$prefix.$name"
    schema.fields.flatMap {
      case StructField(name, inner: StructType, _, _) =>
        fullName(name) +: helper(inner, fullName(name))
      case StructField(name, _, _, _) => Seq(fullName(name))
    }
  }

  helper(schema, "")
}
val schema_fields = fullFlattenSchema(schema)

// COMMAND ----------

// #This part is to catch all 'bad' data records and store into an error log table
import org.apache.spark.sql.functions._

// 
// #Batch copy and encryption
val encr_invalid_tbl_name = "encr_invalid_tbl" //used for processing data that passed invalidation criteria

val encrypt_func="encrypt"
// #Prepare for consuming from  eventHub - create listener
val BOOTSTRAP_SERVERS = eh_account+".servicebus.windows.net:9093"
val EH_SASL = "kafkashaded.org.apache.kafka.common.security.plain.PlainLoginModule required username=\"$ConnectionString\" password=\"Endpoint=sb://"+eh_account+".servicebus.windows.net;SharedAccessKeyName="+access_policy_name+";SharedAccessKey="+eh_key+"\" ;"
// print(EH_SASL)
//print(EH_SASL2)

val GROUP_ID_BAD_DATA = "$BAD_DATA"
val encr_invalid_df = spark 
.readStream 
.format("kafka") 
.option("kafka.bootstrap.servers", BOOTSTRAP_SERVERS) 
.option("subscribe", eh_topic) 
.option("kafka.sasl.mechanism","PLAIN") 
.option("kafka.security.protocol","SASL_SSL") 
.option("kafka.sasl.jaas.config", EH_SASL ) 
.option("startingOffsets", "earliest")
.option("kafka.request.timeout.ms", "60000") 
.option("kafka.session.timeout.ms", "60000") 
.option("kafka.group.id", GROUP_ID_BAD_DATA) 
.option("failOnDataLoss", "false") 
.load() 
.select($"timestamp", $"value".cast("STRING").alias("value")) 
.select($"timestamp", from_json($"value", schema).as("json"))
  
// #temporary tables to process sql stmts 

encr_invalid_df.createOrReplaceTempView(encr_invalid_tbl_name)

// #Constructing the SQL statement for good data
var encr_invalid_sql_statement:String = ""
var where_statement: String =" where "

for (field <- schema_fields) {
  if (field_encryption_map.keys.contains((field.split("\\.").last.toLowerCase()))){ //this is handling for sensitive fields
    val encr_key = field_encryption_map.getOrElse(field.split("\\.").last.toLowerCase(),"").toString
//     print(encr_key)
    val regex_exp = regex_map.getOrElse(encr_key,"").toString
//       #EH
//       #filter good data based on invalidation criteria provided by infosec team and encrypt it
    encr_invalid_sql_statement = encr_invalid_sql_statement + "json."+field+" "   +field.split("\\.").last+ ";" 
    where_statement = where_statement +"json."+field+" rlike '"+regex_exp+"' or "
  }
  else //this is handling for non-sensitive fields
    encr_invalid_sql_statement = encr_invalid_sql_statement+"json."+ field+";"
}


//print(encr_invalid_sql_statement)

//This is the part to build a struct SQL statement (hierchary) so that we can convert back to hierarchical Json format
var after_struct="struct("
var before_struct="struct("
var singles=""
for (word <- encr_invalid_sql_statement.split(";")) {
  if (word.contains("after")){
    after_struct=after_struct+word+","
  }else if(word.contains("before")){
    before_struct=before_struct+word+"," 
  }else{
    singles = singles+word+","
  }
}


after_struct = after_struct.slice(0, after_struct.length - 1) +") as after"
before_struct = before_struct.slice(0, before_struct.length - 1) +") as before,"
where_statement = where_statement.slice(0, where_statement.length - 4)
encr_invalid_sql_statement = "Select CAST(json.current_ts AS STRING) as Key, struct("+singles+before_struct +after_struct+ ") AS value from " + encr_invalid_tbl_name + where_statement
// print(encr_invalid_sql_statement)

val encr_invalid_dfout = spark.sql(encr_invalid_sql_statement)
// #write invalid data to a delta table
// # encr_invalid_dfout.writeStream.option("checkpointLocation", checkpointLocation+"/error").option("path",error_logtbl_path).table(error_logtbl)
encr_invalid_dfout.writeStream.format("delta").option("checkpointLocation", checkpointLocation+"/error").option("path",error_logtbl_path).start()

// #this option is to write invalid data to a EH topic
// # EH_ENC_SASL = "kafkashaded.org.apache.kafka.common.security.plain.PlainLoginModule required username='$ConnectionString' password='Endpoint=sb://"+eh_account+".servicebus.windows.net;SharedAccessKeyName="+en_access_policy_name+";SharedAccessKey="+eh_en_key+"';"

// # encr_invalid_dfout_df =encr_invalid_dfout.writeStream \
// #     .format("kafka") \
// #     .option("kafka.bootstrap.servers", BOOTSTRAP_SERVERS) \
// #     .option("topic", "error_topic_here") \
// #     .option("kafka.sasl.mechanism","PLAIN") \
// #     .option("kafka.security.protocol","SASL_SSL") \
// #     .option("kafka.sasl.jaas.config", EH_ENC_SASL ) \
// #     .option("checkpointLocation", checkpointLocation) \
// #     .start() 


// COMMAND ----------

display(spark.readStream.format("delta").load(error_logtbl_path))
//This is to display if any bad records are caught


// COMMAND ----------

// #This part is to use regex_replace to replace invalid characters in columns that are supposed to be encrypted before passing to encryption function to minimize problem with encryption due to valid format.
import org.apache.spark.sql.functions._

// dbutils.fs.rm(checkpointLocation, True)
val encr_valid_tbl_name = "encr_valid_tbl" //#used for processing data that passed invalidation criteria

val encrypt_func="encrypt"
 //Prepare for consuming from  eventHub - create listener
val BOOTSTRAP_SERVERS = eh_account+".servicebus.windows.net:9093"
val EH_SASL = "kafkashaded.org.apache.kafka.common.security.plain.PlainLoginModule required username=\"$ConnectionString\" password=\"Endpoint=sb://"+eh_account+".servicebus.windows.net;SharedAccessKeyName="+access_policy_name+";SharedAccessKey="+eh_key+"\" ;"
// print(EH_SASL)

//Consumer group for taking care of valid senstive data
val GROUP_ID_valid_DATA = "$GOOD_DATA"
val encr_valid_df = spark 
.readStream 
.format("kafka") 
.option("kafka.bootstrap.servers", BOOTSTRAP_SERVERS) 
.option("subscribe", eh_topic) 
.option("kafka.sasl.mechanism","PLAIN") 
.option("kafka.security.protocol","SASL_SSL") 
.option("kafka.sasl.jaas.config", EH_SASL ) 
.option("kafka.request.timeout.ms", "60000") 
.option("kafka.session.timeout.ms", "60000") 
.option("startingOffsets", "earliest")
.option("kafka.group.id", GROUP_ID_valid_DATA) 
.option("failOnDataLoss", "false") 
.load() 
.select(col("timestamp"), col("value").cast("STRING").alias("value")) 
.select(col("timestamp"), from_json(col("value"), schema).alias("json"))
  
//#temporary tables to process sql stmts 
encr_valid_df.createOrReplaceTempView(encr_valid_tbl_name)

//#Constructing the SQL statement for good data
var encr_valid_sql_statement: String = ""
//# #Where statement to remove null value object
//#James: not where but it should be regexp_replace((column, pattern, replacement)) to remove/replace special character before apply encrypt_func
//# 
for (field <- schema_fields) {
  if (field_encryption_map.keys.contains((field.split("\\.").last.toLowerCase()))){
    //#this is handling for sensitive fields
  //  #in case of streaming above, the field need to be accessed as json.field_name
    //  #EH
      //#filter good data based on validation criteria provided by infosec team and encrypt it
//#       encr_valid_sql_statement = encr_valid_sql_statement +"(CASE WHEN json."+field+" IS NOT NULL AND LENGTH(json."+field+") <> 0 THEN " +encrypt_func +"(json."+field+",'"+ field_encryption_map.keys.contains((field.split("\\.").last.toLowerCase()))+"')  ELSE json."+field+" END) "   +field.split("\\.").last+ ";" 
      //#To apply regex to remove special characters, you can do it as follow
    val encr_key = field_encryption_map.getOrElse(field.split("\\.").last.toLowerCase(),"").toString
    val regex_exp = regex_map.getOrElse(encr_key,"").toString

     encr_valid_sql_statement = encr_valid_sql_statement +"(CASE WHEN json."+field+" IS NOT NULL AND LENGTH(json."+field+") <> 0 AND " +"json."+field+" !rlike '"+regex_exp+"' THEN " +encrypt_func +"(json."+field+",'"+encr_key+"')  ELSE NULL END) "   +field.split("\\.").last+ ";"

  } else { 
    //#this is handling for non-sensitive fields
     encr_valid_sql_statement = encr_valid_sql_statement+"json."+ field+";"
  }
}

// print (encr_valid_sql_statement)
//#This is the part to build a struct SQL statement (hierchary) so that we can convert back to hierarchical Json format
var after_struct="struct("
var before_struct="struct("
var singles=""
for (word <- encr_valid_sql_statement.split(";")) {
  if (word.contains("after")){
    after_struct=after_struct+word+","
  }else if(word.contains("before")){
    before_struct=before_struct+word+"," 
  }else{
    singles = singles+word+","
  }
}


after_struct = after_struct.slice(0, after_struct.length - 1) +") as after"
before_struct = before_struct.slice(0, before_struct.length - 1) +") as before,"

encr_valid_sql_statement = "Select CAST(json.current_ts AS STRING) as Key, to_json(struct("+singles+before_struct +after_struct+ ")) AS value from " + encr_valid_tbl_name 

val encr_valid_dfout = spark.sql(encr_valid_sql_statement)

// display(encr_valid_dfout)
//#Stream this data to Databasew

//# #Write out the encrypted dataset
val EH_ENC_SASL = "kafkashaded.org.apache.kafka.common.security.plain.PlainLoginModule required username='$ConnectionString' password='Endpoint=sb://"+eh_account+".servicebus.windows.net;SharedAccessKeyName="+en_access_policy_name+";SharedAccessKey="+eh_en_key+"';"

val encr_valid_dfout_df =encr_valid_dfout.writeStream 
    .format("kafka") 
    .option("kafka.bootstrap.servers", BOOTSTRAP_SERVERS) 
    .option("topic", eh_en_topic) 
    .option("kafka.sasl.mechanism","PLAIN") 
    .option("kafka.security.protocol","SASL_SSL") 
    .option("kafka.sasl.jaas.config", EH_ENC_SASL ) 
    .option("checkpointLocation", checkpointLocation+"/main_process") 
    .start() 