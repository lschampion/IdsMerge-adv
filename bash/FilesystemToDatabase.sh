dataSourceConfigPath="/data/merge/conf/test.properties"
hdfsInputPath="/user/test/tmp_data/result/"


spark-submit \
--master yarn \
--name idunion \
--driver-memory 8G \
--executor-memory 20G \
--num-executors 20 \
--executor-cores 2 \
--jars /data/merge/lib/jars/ \
--conf spark.serializer=org.apache.spark.serializer.KryoSerializer \
--class com.test.postProcess.FilesystemToDatabase /data/merge/lib/IDsMerge-adv-1.0.jar ${dataSourceConfigPath} ${hdfsInputPath}


hdfs dfs -rm -r "${hdfsInputPath}history/"
hdfs dfs -cp "${hdfsInputPath}new/*" "${hdfsInputPath}history/"