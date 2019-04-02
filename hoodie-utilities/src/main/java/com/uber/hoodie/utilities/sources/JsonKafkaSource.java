/*
 *  Copyright (c) 2017 Uber Technologies, Inc. (hoodie-dev-group@uber.com)
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *           http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 *
 */

package com.uber.hoodie.utilities.sources;

import com.uber.hoodie.common.util.TypedProperties;
import com.uber.hoodie.utilities.schema.SchemaProvider;
import com.uber.hoodie.utilities.sources.helpers.KafkaOffsetGen;
import com.uber.hoodie.utilities.sources.helpers.KafkaOffsetGen.CheckpointUtils;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Optional;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.streaming.kafka010.KafkaUtils;
import org.apache.spark.streaming.kafka010.LocationStrategies;
import org.apache.spark.streaming.kafka010.OffsetRange;
import scala.Tuple2;

/**
 * Read json kafka data
 */
public class JsonKafkaSource extends JsonSource {

  private static Logger log = LogManager.getLogger(JsonKafkaSource.class);

  private final KafkaOffsetGen offsetGen;

  public JsonKafkaSource(TypedProperties properties, JavaSparkContext sparkContext, SparkSession sparkSession,
                         SchemaProvider schemaProvider) {
    super(properties, sparkContext, sparkSession, schemaProvider);
    offsetGen = new KafkaOffsetGen(properties);
    log.info("Source class is ---------Json Kafka Source");
  }

  @Override
  protected InputBatch<JavaRDD<String>> fetchNewData(Optional<String> lastCheckpointStr,
                                                     long sourceLimit) {
    OffsetRange[] offsetRanges = offsetGen.getNextOffsetRanges(lastCheckpointStr, sourceLimit);
    long totalNewMsgs = CheckpointUtils.totalNewMessages(offsetRanges);
    if (totalNewMsgs <= 0) {
      return new InputBatch<>(Optional.empty(),
              lastCheckpointStr.isPresent() ? lastCheckpointStr.get() : "");
    }
    log.info("About to read " + totalNewMsgs + " from Kafka for topic :" + offsetGen.getTopicName());
    JavaRDD<String> newDataRDD = toRDD(offsetRanges);
    return new InputBatch<>(Optional.of(newDataRDD), CheckpointUtils.offsetsToStr(offsetRanges));
  }

  /*private JavaRDD<String> toRDD(OffsetRange[] offsetRanges) {
    return KafkaUtils.createRDD(sparkContext, String.class, String.class, StringDecoder.class, StringDecoder.class,
        offsetGen.getKafkaParams(), offsetRanges).values();
  } */

  private JavaRDD<String> toRDD(OffsetRange[] offsetRanges) {

   // return KafkaUtils.createRDD(sparkContext, offsetGen.getKafkaParams(), offsetRanges, LocationStrategies.PreferConsistent()).map( s -> s.value().toString());

  /*  Function<ConsumerRecord<String, String>, String> handler= new Function<ConsumerRecord<String, String>, String>(){
      @Override
      public String call(ConsumerRecord<String, String> r){
        return r.value();
      }
    };
    return  KafkaUtils.<String, String>createRDD(sparkContext, offsetGen.getKafkaParams(), offsetRanges, LocationStrategies.PreferConsistent()).map(handler);
  */

  JavaRDD<ConsumerRecord<String, String>> consumerRecord = KafkaUtils.createRDD(sparkContext, offsetGen.getKafkaParams(), offsetRanges, LocationStrategies.PreferConsistent());
    JavaPairRDD<String, String> pair = consumerRecord.mapToPair(new PairFunction<ConsumerRecord<String, String>, String, String>() {
      public Tuple2<String, String> call(ConsumerRecord<String, String> record) {
        log.info(" record key: " + record.key());
        log.info(" record value: " + record.value());
        return new Tuple2<>(record.key(), record.value());
      }
    });
    return pair.values();

  }
}