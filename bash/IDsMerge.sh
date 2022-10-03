preprocePath="/user/test/tmp_data/preProcess/"
mergePath="/user/test/tmp_data/merge/"
resultPath="/user/test/tmp_data/result/"
idcount=1000

hdfs dfs -rm -r ${mergePath}


spark-submit \
--master yarn \
--name idsMerge \
--driver-memory 8G \
--executor-memory 20G \
--num-executors 20 \
--executor-cores 2 \
--jars /data/merge/lib/jars/ \
--conf spark.serializer=org.apache.spark.serializer.KryoSerializer \
--class com.test.IDsMergeInOne.IDsMergeEntry /data/merge/lib/IDsMerge-adb-1.0.jar ${preprocePath} ${mergePath} ${resultPath} ${idcount}
