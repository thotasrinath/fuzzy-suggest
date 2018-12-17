package com.dbs;

import co.cask.cdap.api.spark.JavaSparkExecutionContext;
import co.cask.cdap.api.spark.JavaSparkMain;

public class FuzzySpark implements JavaSparkMain {
    @Override
    public void run(JavaSparkExecutionContext javaSparkExecutionContext) throws Exception {

        javaSparkExecutionContext.execute(new SparkTx());


    }


}
