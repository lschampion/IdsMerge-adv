dataSourceConfigPath="/data/merge/conf/test.properties"
outPath="/user/test/tmp_data/preProcess/"
hdfs dfs -rm -r ${outPath}

spark-submit \
--master yarn \
--name preprocess \
--driver-memory 8G \
--executor-memory 20G \
--num-executors 20 \
--executor-cores 2 \
--jars /data/merge/lib/jars/ \
--conf spark.serializer=org.apache.spark.serializer.KryoSerializer \
--class com.test.preProcess.DatabaseToFilesystem /data/merge/lib/IDsMerge-adv-1.0.jar ${dataSourceConfigPath} ${outPath}
