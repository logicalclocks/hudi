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

package com.uber.hoodie.utilities.deltastreamer;

import static com.uber.hoodie.utilities.schema.RowBasedSchemaProvider.HOODIE_RECORD_NAMESPACE;
import static com.uber.hoodie.utilities.schema.RowBasedSchemaProvider.HOODIE_RECORD_STRUCT_NAME;

import com.beust.jcommander.IStringConverter;
//import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.beust.jcommander.ParameterException;
import com.codahale.metrics.Timer;
import com.uber.hoodie.*;
import com.uber.hoodie.common.model.HoodieCommitMetadata;
import com.uber.hoodie.common.model.HoodieRecord;
import com.uber.hoodie.common.model.HoodieRecordPayload;
import com.uber.hoodie.common.model.HoodieTableType;
import com.uber.hoodie.common.table.HoodieTableMetaClient;
import com.uber.hoodie.common.table.HoodieTimeline;
import com.uber.hoodie.common.table.timeline.HoodieInstant;
import com.uber.hoodie.common.util.FSUtils;
import com.uber.hoodie.common.util.TypedProperties;
import com.uber.hoodie.config.HoodieCompactionConfig;
import com.uber.hoodie.config.HoodieIndexConfig;
import com.uber.hoodie.config.HoodieWriteConfig;
import com.uber.hoodie.hive.HiveSyncConfig;
import com.uber.hoodie.hive.HiveSyncTool;
import com.uber.hoodie.index.HoodieIndex;
import com.uber.hoodie.utilities.HiveIncrementalPuller;
import com.uber.hoodie.utilities.UtilHelpers;
import com.uber.hoodie.utilities.exception.HoodieDeltaStreamerException;
import com.uber.hoodie.utilities.schema.RowBasedSchemaProvider;
import com.uber.hoodie.utilities.schema.SchemaProvider;
import com.uber.hoodie.utilities.sources.InputBatch;
import com.uber.hoodie.utilities.sources.JsonDFSSource;
import com.uber.hoodie.utilities.transform.Transformer;
import io.hops.util.Hops;
import java.io.IOException;
import java.io.Serializable;
import java.util.*;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
//import scala.Tuple2;
import scala.collection.JavaConversions;

/**
 * An Utility which can incrementally take the output from {@link HiveIncrementalPuller} and apply
 * it to the target dataset. Does not maintain any state, queries at runtime to see how far behind
 * the target dataset is from the source dataset. This can be overriden to force sync from a
 * timestamp.
 */
public class HoodieDeltaStreamer implements Serializable {

  private static volatile Logger log = LogManager.getLogger(HoodieDeltaStreamer.class);

  public static String CHECKPOINT_KEY = "_deltastreamer.checkpoint.key";

  private final Config cfg;

  /**
   * Source to pull deltas from
   */
  private transient SourceFormatAdapter formatAdapter;

  /**
   * Schema provider that supplies the command for reading the input and writing out the target
   * table.
   */
  private transient SchemaProvider schemaProvider;

  /**
   * Allows transforming source to target dataset before writing
   */
  private transient Transformer transformer;

  /**
   * Extract the key for the target dataset
   */
  private KeyGenerator keyGenerator;

  /**
   * Filesystem used
   */
  private transient FileSystem fs;

  /**
   * Timeline with completed commits
   */
  private transient Optional<HoodieTimeline> commitTimelineOpt;

  /**
   * Spark context
   */
  private transient JavaSparkContext jssc;

  /**
   * Spark Session
   */
  private transient SparkSession sparkSession;

  /**
   * Hive Config
   */
  private transient HiveConf hiveConf;

  private String checkpoint;

  /**
   * Bag of properties with source, hoodie client, key generator etc.
   */
  TypedProperties props;

  private transient HoodieDeltaStreamerMetrics metrics;

  private transient Timer.Context overallTimerContext;


  public HoodieDeltaStreamer(Config cfg, JavaSparkContext jssc) throws IOException {
    this(cfg, jssc, FSUtils.getFs(cfg.targetBasePath, jssc.hadoopConfiguration()),
        getDefaultHiveConf(jssc.hadoopConfiguration()));
  }

  public HoodieDeltaStreamer(Config cfg, JavaSparkContext jssc, FileSystem fs, HiveConf hiveConf) throws IOException {
    this.cfg = cfg;
    this.jssc = jssc;
    this.sparkSession = SparkSession.builder().config(jssc.getConf()).getOrCreate();
    this.fs = FSUtils.getFs(cfg.targetBasePath, jssc.hadoopConfiguration());

    if (fs.exists(new Path(cfg.targetBasePath))) {
      HoodieTableMetaClient meta = new HoodieTableMetaClient(fs.getConf(), cfg.targetBasePath);
      this.commitTimelineOpt = Optional.of(meta.getActiveTimeline().getCommitsTimeline()
          .filterCompletedInstants());
    } else {
      this.commitTimelineOpt = Optional.empty();
    }

    this.props = UtilHelpers.readConfig(fs, new Path(cfg.propsFilePath), cfg.configs).getConfig();
    log.info("Creating delta streamer with configs : " + props.toString());
    this.schemaProvider = UtilHelpers.createSchemaProvider(cfg.schemaProviderClassName, props, jssc);
    this.transformer = UtilHelpers.createTransformer(cfg.transformerClassName);
    this.keyGenerator = DataSourceUtils.createKeyGenerator(cfg.keyGeneratorClass, props);

    this.formatAdapter =
        new SourceFormatAdapter(UtilHelpers.createSource(cfg.sourceClassName, props, jssc, sparkSession,
            schemaProvider));

    this.hiveConf = hiveConf;
  }

  private static HiveConf getDefaultHiveConf(Configuration cfg) {
    HiveConf hiveConf = new HiveConf(true);
    hiveConf.addResource(cfg);
    return hiveConf;
  }

  public Optional<JavaRDD<GenericRecord>> createRDD() throws Exception {
    metrics = new HoodieDeltaStreamerMetrics(getHoodieClientConfig(null));
    overallTimerContext = metrics.getOverallTimerContext();

    // Retrieve the previous round checkpoints, if any
    Optional<String> resumeCheckpointStr = Optional.empty();
    if (commitTimelineOpt.isPresent()) {
      Optional<HoodieInstant> lastCommit = commitTimelineOpt.get().lastInstant();
      if (lastCommit.isPresent()) {
        HoodieCommitMetadata commitMetadata = HoodieCommitMetadata.fromBytes(
            commitTimelineOpt.get().getInstantDetails(lastCommit.get()).get(), HoodieCommitMetadata.class);
        if (commitMetadata.getMetadata(CHECKPOINT_KEY) != null) {
          resumeCheckpointStr = Optional.of(commitMetadata.getMetadata(CHECKPOINT_KEY));
        } else {
          throw new HoodieDeltaStreamerException(
              "Unable to find previous checkpoint. Please double check if this table "
                  + "was indeed built via delta streamer ");
        }
      }
    } else {
      HoodieTableMetaClient.initTableType(jssc.hadoopConfiguration(), cfg.targetBasePath,
          cfg.storageType, cfg.targetTableName, "archived");
    }
    log.info("Checkpoint to resume from : " + resumeCheckpointStr);

    final Optional<JavaRDD<GenericRecord>> avroRDDOptional;
    final String checkpointStr;
    final SchemaProvider schemaProvider;
    if (transformer != null) {
      // Transformation is needed. Fetch New rows in Row Format, apply transformation and then convert them
      // to generic records for writing
      InputBatch<Dataset<Row>> dataAndCheckpoint = formatAdapter.fetchNewDataInRowFormat(
          resumeCheckpointStr, cfg.sourceLimit);

      Optional<Dataset<Row>> transformed =
          dataAndCheckpoint.getBatch().map(data -> transformer.apply(jssc, sparkSession, data, props));
      checkpointStr = dataAndCheckpoint.getCheckpointForNextBatch();
      checkpoint = checkpointStr;
      avroRDDOptional = transformed.map(t ->
          AvroConversionUtils.createRdd(t, HOODIE_RECORD_STRUCT_NAME, HOODIE_RECORD_NAMESPACE).toJavaRDD()
      );
      // Use Transformed Row's schema if not overridden
      schemaProvider =
          this.schemaProvider == null ? transformed.map(r -> (SchemaProvider) new RowBasedSchemaProvider(r.schema()))
              .orElse(dataAndCheckpoint.getSchemaProvider()) : this.schemaProvider;

      return avroRDDOptional;
    } else {
      // Pull the data from the source & prepare the write
      InputBatch<JavaRDD<GenericRecord>> dataAndCheckpoint =
          formatAdapter.fetchNewDataInAvroFormat(resumeCheckpointStr, cfg.sourceLimit);
      avroRDDOptional = dataAndCheckpoint.getBatch();
      checkpointStr = dataAndCheckpoint.getCheckpointForNextBatch();
      checkpoint = checkpointStr;
      schemaProvider = dataAndCheckpoint.getSchemaProvider();

      return avroRDDOptional;
    }

  }

  public JavaRDD<HoodieRecord> createPayload(Optional<JavaRDD<GenericRecord>> avroRDDOptional) throws Exception {

    registerAvroSchemas(schemaProvider);

    JavaRDD<GenericRecord> avroRDD = avroRDDOptional.get();
    JavaRDD<HoodieRecord> records = avroRDD.map(gr -> {
      HoodieRecordPayload payload = DataSourceUtils.createPayload(cfg.payloadClassName, gr,
          (Comparable) gr.get(cfg.sourceOrderingField));
      return new HoodieRecord<>(keyGenerator.getKey(gr), payload);
    });

    return records;
  }


  public JavaRDD<HoodieRecord> filterDuplicateRecords(JavaRDD<HoodieRecord> records) throws Exception {

    // filter dupes if needed
    HoodieWriteConfig hoodieCfg = getHoodieClientConfig(schemaProvider);
    if (cfg.filterDupes) {
      // turn upserts to insert
      cfg.operation = cfg.operation == Operation.UPSERT ? Operation.INSERT : cfg.operation;
      records = DataSourceUtils.dropDuplicates(jssc, records, hoodieCfg);

      return records;

    }
    return records;
  }

  public void writeIntoHoodieTable(JavaRDD<HoodieRecord> records) throws Exception {

    // Perform the write
    HoodieWriteConfig hoodieCfg = getHoodieClientConfig(schemaProvider);
    HoodieWriteClient client = new HoodieWriteClient<>(jssc, hoodieCfg, true);
    String commitTime = client.startCommit();
    log.info("Starting commit  : " + commitTime);


    JavaRDD<WriteStatus> writeStatusRDD;
    if (cfg.operation == Operation.INSERT) {
      writeStatusRDD = client.insert(records, commitTime);
    } else if (cfg.operation == Operation.UPSERT) {
      writeStatusRDD = client.upsert(records, commitTime);
    } else if (cfg.operation == Operation.BULK_INSERT) {
      writeStatusRDD = client.bulkInsert(records, commitTime);
    } else {
      throw new HoodieDeltaStreamerException("Unknown operation :" + cfg.operation);
    }

    long totalErrorRecords = writeStatusRDD.mapToDouble(ws -> ws.getTotalErrorRecords()).sum().longValue();
    long totalRecords = writeStatusRDD.mapToDouble(ws -> ws.getTotalRecords()).sum().longValue();
    boolean hasErrors = totalErrorRecords > 0;
    long hiveSyncTimeMs = 0;
    if (!hasErrors || cfg.commitOnErrors) {
      HashMap<String, String> checkpointCommitMetadata = new HashMap<>();
      checkpointCommitMetadata.put(CHECKPOINT_KEY, checkpoint);

      if (hasErrors) {
        log.warn("Some records failed to be merged but forcing commit since commitOnErrors set. Errors/Total="
            + totalErrorRecords + "/" + totalRecords);
      }

      boolean success = client.commit(commitTime, writeStatusRDD,
          Optional.of(checkpointCommitMetadata));
      if (success) {
        log.info("Commit " + commitTime + " successful!");
        // Sync to hive if enabled
        Timer.Context hiveSyncContext = metrics.getHiveSyncTimerContext();
        syncHive();
        hiveSyncTimeMs = hiveSyncContext != null ? hiveSyncContext.stop() : 0;
      } else {
        log.info("Commit " + commitTime + " failed!");
      }
    } else {
      log.error("There are errors when ingesting records. Errors/Total="
          + totalErrorRecords + "/" + totalRecords);
      log.error("Printing out the top 10 errors");
      writeStatusRDD.filter(ws -> ws.hasErrors()).take(10).forEach(ws -> {
        log.error("Global error :", ws.getGlobalError());
        if (ws.getErrors().size() > 0) {
          ws.getErrors().entrySet().forEach(r ->
              log.trace("Error for key:" + r.getKey() + " is " + r.getValue()));
        }
      });
    }

    client.close();
    long overallTimeMs = overallTimerContext != null ? overallTimerContext.stop() : 0;

    // Send DeltaStreamer Metrics
    metrics.updateDeltaStreamerMetrics(overallTimeMs, hiveSyncTimeMs);
  }

  public void syncHive() {
    if (cfg.enableHiveSync) {
      HiveSyncConfig hiveSyncConfig = DataSourceUtils.buildHiveSyncConfig(props, cfg.targetBasePath);
      log.info("Syncing target hoodie table with hive table(" + hiveSyncConfig.tableName
          + "). Hive metastore URL :" + hiveSyncConfig.jdbcUrl + ", basePath :" + cfg.targetBasePath);

      new HiveSyncTool(hiveSyncConfig, hiveConf, fs).syncHoodieTable();
    }
  }

  public void createFeatureGroup(Optional<JavaRDD<GenericRecord>> avroRDDOptional) throws Exception {

    registerAvroSchemas(schemaProvider);
    JavaRDD<GenericRecord> avroRDD = avroRDDOptional.get();

    String featureGroupName = cfg.targetTableName;
    Map<String, String> hudiArgs = new HashMap<String, String>();
    hudiArgs.put(DataSourceWriteOptions.STORAGE_TYPE_OPT_KEY(), cfg.storageType);
    hudiArgs.put(DataSourceWriteOptions.OPERATION_OPT_KEY(), props.getProperty(DataSourceWriteOptions.OPERATION_OPT_KEY()));
    hudiArgs.put(DataSourceWriteOptions.RECORDKEY_FIELD_OPT_KEY(), props.getProperty(DataSourceWriteOptions.RECORDKEY_FIELD_OPT_KEY()));
    hudiArgs.put(DataSourceWriteOptions.PARTITIONPATH_FIELD_OPT_KEY(), props.getProperty(DataSourceWriteOptions.PARTITIONPATH_FIELD_OPT_KEY()));
    hudiArgs.put(DataSourceWriteOptions.PRECOMBINE_FIELD_OPT_KEY(), props.getProperty(DataSourceWriteOptions.PRECOMBINE_FIELD_OPT_KEY()));
    hudiArgs.put(DataSourceWriteOptions.HIVE_USER_OPT_KEY(), props.getProperty(DataSourceWriteOptions.HIVE_USER_OPT_KEY()));
    hudiArgs.put(DataSourceWriteOptions.HIVE_PASS_OPT_KEY(), props.getProperty(DataSourceWriteOptions.HIVE_PASS_OPT_KEY()));
    hudiArgs.put(DataSourceWriteOptions.HIVE_URL_OPT_KEY(), props.getProperty(DataSourceWriteOptions.HIVE_URL_OPT_KEY()));
    hudiArgs.put(DataSourceWriteOptions.HIVE_PARTITION_FIELDS_OPT_KEY(), props.getProperty(DataSourceWriteOptions.HIVE_PARTITION_FIELDS_OPT_KEY()));
    hudiArgs.put(HoodieWriteConfig.INSERT_PARALLELISM, props.getProperty(HoodieWriteConfig.INSERT_PARALLELISM));
    hudiArgs.put(HoodieWriteConfig.UPSERT_PARALLELISM, props.getProperty(HoodieWriteConfig.UPSERT_PARALLELISM));
    hudiArgs.put(HoodieWriteConfig.BULKINSERT_PARALLELISM, props.getProperty(HoodieWriteConfig.BULKINSERT_PARALLELISM));
    hudiArgs.put(CHECKPOINT_KEY, checkpoint);


    Dataset<Row> df = AvroConversionUtils.createDataFrame(avroRDD.rdd(), schemaProvider.getSourceSchema().toString(), sparkSession);

    Hops.createFeaturegroup(featureGroupName).setDataframe(df).setDescription("Feature Group of ")
        .setDescriptiveStats(false).setFeatureCorr(false).setFeatureHistograms(false).setClusterAnalysis(false)
        .setHudi(true).setHudiArgs(hudiArgs).setHudiTableBasePath(cfg.targetBasePath).write();

    log.info("Succesfully created Featuregroup: " + cfg.targetTableName);

  }

  /**
   * Register Avro Schemas
   *
   * @param schemaProvider Schema Provider
   */
  private void registerAvroSchemas(SchemaProvider schemaProvider) {
    // register the schemas, so that shuffle does not serialize the full schemas
    if (null != schemaProvider) {
      List<Schema> schemas = Arrays.asList(schemaProvider.getSourceSchema(), schemaProvider.getTargetSchema());
      log.info("Registering Schema :" + schemas);
      jssc.sc().getConf().registerAvroSchemas(JavaConversions.asScalaBuffer(schemas).toList());
    }
  }

  private HoodieWriteConfig getHoodieClientConfig(SchemaProvider schemaProvider) {
    HoodieWriteConfig.Builder builder =
        HoodieWriteConfig.newBuilder().withPath(cfg.targetBasePath)
            .withAutoCommit(false).combineInput(cfg.filterDupes, true)
            .withCompactionConfig(HoodieCompactionConfig.newBuilder()
                .withPayloadClass(cfg.payloadClassName)
                // turn on inline compaction by default, for MOR tables
                .withInlineCompaction(HoodieTableType.valueOf(cfg.storageType) == HoodieTableType.MERGE_ON_READ)
                .build())
            .forTable(cfg.targetTableName)
            .withIndexConfig(HoodieIndexConfig.newBuilder().withIndexType(HoodieIndex.IndexType.BLOOM).build())
            .withProps(props);
    if (null != schemaProvider) {
      builder = builder.withSchema(schemaProvider.getTargetSchema().toString());
    }

    return builder.build();
  }

  public enum Operation {
    UPSERT, INSERT, BULK_INSERT
  }

  private static class OperationConvertor implements IStringConverter<Operation> {
    @Override
    public Operation convert(String value) throws ParameterException {
      return Operation.valueOf(value);
    }
  }

  public static class Config implements Serializable {

    @Parameter(names = {"--target-base-path"}, description = "base path for the target hoodie dataset. "
        + "(Will be created if did not exist first time around. If exists, expected to be a hoodie dataset)",
        required = true)
    public String targetBasePath;

    // TODO: How to obtain hive configs to register?
    @Parameter(names = {"--target-table"}, description = "name of the target table in Hive", required = true)
    public String targetTableName;

    @Parameter(names = {"--storage-type"}, description = "Type of Storage. "
        + "COPY_ON_WRITE (or) MERGE_ON_READ", required = true)
    public String storageType;

    @Parameter(names = {"--props"}, description = "path to properties file on localfs or dfs, with configurations for "
        + "hoodie client, schema provider, key generator and data source. For hoodie client props, sane defaults are "
        + "used, but recommend use to provide basic things like metrics endpoints, hive configs etc. For sources, refer"
        + "to individual classes, for supported properties.")
    public String propsFilePath =
        "file://" + System.getProperty("user.dir") + "/src/test/resources/delta-streamer-config/dfs-source.properties";

    @Parameter(names = {"--hoodie-conf"}, description = "Any configuration that can be set in the properties file "
        + "(using the CLI parameter \"--propsFilePath\") can also be passed command line using this parameter")
    public List<String> configs = new ArrayList<>();

    @Parameter(names = {"--source-class"}, description = "Subclass of com.uber.hoodie.utilities.sources to read data. "
        + "Built-in options: com.uber.hoodie.utilities.sources.{JsonDFSSource (default), AvroDFSSource, "
        + "JsonKafkaSource, AvroKafkaSource, HiveIncrPullSource}")
    public String sourceClassName = JsonDFSSource.class.getName();

    @Parameter(names = {"--source-ordering-field"}, description = "Field within source record to decide how"
        + " to break ties between records with same key in input data. Default: 'ts' holding unix timestamp of record")
    public String sourceOrderingField = "ts";

    @Parameter(names = {"--key-generator-class"}, description = "Subclass of com.uber.hoodie.KeyGenerator "
        + "to generate a HoodieKey from the given avro record. Built in: SimpleKeyGenerator (uses "
        + "provided field names as recordkey & partitionpath. Nested fields specified via dot notation, e.g: a.b.c)")
    public String keyGeneratorClass = SimpleKeyGenerator.class.getName();

    @Parameter(names = {"--payload-class"}, description = "subclass of HoodieRecordPayload, that works off "
        + "a GenericRecord. Implement your own, if you want to do something other than overwriting existing value")
    public String payloadClassName = OverwriteWithLatestAvroPayload.class.getName();

    @Parameter(names = {"--schemaprovider-class"}, description = "subclass of com.uber.hoodie.utilities.schema"
        + ".SchemaProvider to attach schemas to input & target table data, built in options: "
        + "com.uber.hoodie.utilities.schema.FilebasedSchemaProvider."
        + "Source (See com.uber.hoodie.utilities.sources.Source) implementation can implement their own SchemaProvider."
        + " For Sources that return Dataset<Row>, the schema is obtained implicitly. "
        + "However, this CLI option allows overriding the schemaprovider returned by Source.")
    public String schemaProviderClassName = null;

    @Parameter(names = {"--transformer-class"},
        description = "subclass of com.uber.hoodie.utilities.transform.Transformer"
            + ". Allows transforming raw source dataset to a target dataset (conforming to target schema) before writing."
            + " Default : Not set. E:g - com.uber.hoodie.utilities.transform.SqlQueryBasedTransformer (which allows"
            + "a SQL query templated to be passed as a transformation function)")
    public String transformerClassName = null;

    @Parameter(names = {"--source-limit"}, description = "Maximum amount of data to read from source. "
        + "Default: No limit For e.g: DFS-Source => max bytes to read, Kafka-Source => max events to read")
    public long sourceLimit = Long.MAX_VALUE;

    @Parameter(names = {"--op"}, description = "Takes one of these values : UPSERT (default), INSERT (use when input "
        + "is purely new data/inserts to gain speed)",
        converter = OperationConvertor.class)
    public Operation operation = Operation.UPSERT;

    @Parameter(names = {"--filter-dupes"}, description = "Should duplicate records from source be dropped/filtered out"
        + "before insert/bulk-insert")
    public Boolean filterDupes = false;

    @Parameter(names = {"--enable-hive-sync"}, description = "Enable syncing to hive")
    public Boolean enableHiveSync = false;

    @Parameter(names = {"--enable-feature-store"}, description = "Enable creation of feature store")
    public Boolean enableFeatureStore = false;

    @Parameter(names = {"--spark-master"}, description = "spark master to use.")
    public String sparkMaster = "yarn";

    @Parameter(names = {"--commit-on-errors"}, description = "Commit even when some records failed to be written")
    public Boolean commitOnErrors = false;

    @Parameter(names = {"--help", "-h"}, help = true)
    public Boolean help = false;
  }

  /* public static void main(String[] args) throws Exception {
     final Config cfg = new Config();
     JCommander cmd = new JCommander(cfg, args);
     if (cfg.help || args.length == 0) {
       cmd.usage();
       System.exit(1);
     }

     JavaSparkContext jssc = UtilHelpers.buildSparkContext("delta-streamer-" + cfg.targetTableName, cfg.sparkMaster);
     new HoodieDeltaStreamer(cfg, jssc).sync();
   }
 */
  public SourceFormatAdapter getFormatAdapter() {
    return formatAdapter;
  }

  public SchemaProvider getSchemaProvider() {
    return schemaProvider;
  }

  public Transformer getTransformer() {
    return transformer;
  }

  public KeyGenerator getKeyGenerator() {
    return keyGenerator;
  }

  public FileSystem getFs() {
    return fs;
  }

  public Optional<HoodieTimeline> getCommitTimelineOpt() {
    return commitTimelineOpt;
  }

  public JavaSparkContext getJssc() {
    return jssc;
  }

  public SparkSession getSparkSession() {
    return sparkSession;
  }

  public HiveConf getHiveConf() {
    return hiveConf;
  }

  public TypedProperties getProps() {
    return props;
  }
}
