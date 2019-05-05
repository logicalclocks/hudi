package com.uber.hoodie.utilities.deltastreamer;

import com.beust.jcommander.JCommander;
import com.uber.hoodie.common.model.HoodieRecord;
import com.uber.hoodie.utilities.UtilHelpers;
import java.util.Optional;
import org.apache.avro.generic.GenericRecord;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

public class Main {

  private static volatile Logger log = LogManager.getLogger(Main.class);

  public static void main(String[] args) throws Exception {
    final HoodieDeltaStreamer.Config cfg = new HoodieDeltaStreamer.Config();
    JCommander cmd = new JCommander(cfg, args);
    if (cfg.help || args.length == 0) {
      cmd.usage();
      System.exit(1);
    }
    SparkConf sparkConf = UtilHelpers.buildSparkConf("delta-streamer-" + cfg.targetTableName, cfg.sparkMaster);
    JavaSparkContext jssc = UtilHelpers.buildSparkContextHiveSupport(sparkConf);
    HoodieDeltaStreamer deltaStreamer = new HoodieDeltaStreamer(cfg, jssc);

    final Optional<JavaRDD<GenericRecord>> avroRDDOptional = deltaStreamer.createRDD();

    if ((!avroRDDOptional.isPresent()) || (avroRDDOptional.get().isEmpty())) {
      log.info("No new data, nothing to commit.. ");
      return;
    }

    if (cfg.enableFeatureStore) {

      deltaStreamer.createFeatureGroup(avroRDDOptional);
      return;
    }

    JavaRDD<HoodieRecord> records = deltaStreamer.createPayload(avroRDDOptional);
    records = deltaStreamer.filterDuplicateRecords(records);

    if (records.isEmpty()) {
      log.info("No new data, nothing to commit.. ");
      return;
    }

    deltaStreamer.writeIntoHoodieTable(records);


  }
}
