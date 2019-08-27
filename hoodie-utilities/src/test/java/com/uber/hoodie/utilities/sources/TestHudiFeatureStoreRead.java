package com.uber.hoodie.utilities.sources;

import com.uber.hoodie.DataSourceReadOptions;
import io.hops.util.Hops;
import org.apache.spark.SparkConf;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.util.HashMap;
import java.util.Map;
import java.util.logging.Logger;


public class TestHudiFeatureStoreRead {

  static Logger logger = Logger.getLogger(TestHudiFeatureStoreRead.class.getName());


  public static void main(String[] args) {

    SparkConf sparkConf = new SparkConf();
    SparkSession spark = SparkSession
        .builder()
        .appName(Hops.getJobName())
        .config(sparkConf)
        .enableHiveSupport()
        .getOrCreate();

    logger.info("Hello Feature Store! version:" + spark.version());

    Map<String, String> hudiArgs = new HashMap<String, String>();
    hudiArgs.put(DataSourceReadOptions.VIEW_TYPE_OPT_KEY(), "incremental");
    hudiArgs.put(DataSourceReadOptions.BEGIN_INSTANTTIME_OPT_KEY(), "20190516114809");
    
    try {
      Dataset<Row> df = Hops.getFeaturegroup("Stock").setSpark(spark).setFeaturestore(Hops.getProjectFeaturestore().read())
        .setVersion(1).setHudi(true).setHudiArgs(hudiArgs).setHudiTableBasePath("/Projects/Hudi/Output/Stock").read();
    } catch(Exception ex) {
      logger.severe("Error when reading from feature store");
    }

    //logger.info("Dataframe from hudi featurestore: " + df);
    logger.info("Hudi featurestore read succesful! ");
    //Stop spark session
    spark.stop();

  }

}







