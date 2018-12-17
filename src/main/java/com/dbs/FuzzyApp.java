package com.dbs;

import co.cask.cdap.api.Resources;
import co.cask.cdap.api.app.AbstractApplication;
import co.cask.cdap.api.spark.AbstractSpark;

public class FuzzyApp extends AbstractApplication {

    @Override
    public void configure() {

        addSpark(new PageRankSpark());

    }

    public static final class PageRankSpark extends AbstractSpark {

        @Override
        public void configure() {
            setDescription("Spark page rank program");
            setMainClass(FuzzySpark.class);
            setDriverResources(new Resources(1024));
            setExecutorResources(new Resources(1024));
        }
    }
}
