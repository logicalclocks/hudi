/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.sql.hive

import org.apache.hudi.hive.HiveSyncConfig

import org.apache.hadoop.hive.metastore.IMetaStoreClient
import org.apache.hadoop.hive.metastore.api.{Database, EnvironmentContext, FieldSchema, Partition, SerDeInfo, StorageDescriptor, Table}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.catalog.{CatalogDatabase, CatalogStorageFormat, CatalogTable, CatalogTablePartition, CatalogTableType}
import org.apache.spark.sql.catalyst.parser.CatalystSqlParser
import org.apache.spark.sql.types.{Metadata, StructField, StructType}

import java.net.URI
import java.util

import scala.collection.JavaConverters._

/**
 * IMetaStoreClient implementation backed by Spark catalog/external-catalog APIs for
 * methods used by HoodieHiveSyncClient/HMSDDLExecutor.
 */
class SparkCatalogMetaStoreClient(syncConfig: HiveSyncConfig)
  extends IMetaStoreClient {

  private val sparkSession = SparkSession.getActiveSession.getOrElse(SparkSession.builder()
    .enableHiveSupport()
    .getOrCreate())

  private val externalCatalog = sparkSession.sessionState.catalog.externalCatalog

  override def createDatabase(database: Database): Unit = {
    val catalogDb = CatalogDatabase(
      name = database.getName,
      description = Option(database.getDescription).getOrElse(""),
      locationUri = Option(database.getLocationUri).map(new URI(_))
        .getOrElse(new URI(sparkSession.sessionState.conf.warehousePath)),
      properties = Option(database.getParameters).map(_.asScala.toMap).getOrElse(Map.empty))
    externalCatalog.createDatabase(catalogDb, ignoreIfExists = false)
  }

  override def createTable(table: Table): Unit = {
    externalCatalog.createTable(toCatalogTable(table), ignoreIfExists = false)
  }

  override def getTable(dbName: String, tableName: String): Table = {
    fromCatalogTable(externalCatalog.getTable(dbName, tableName))
  }

  // scalastyle:off method.name
  override def alter_table(dbName: String, tableName: String, table: Table): Unit = {
    val updated = toCatalogTable(table).copy(identifier = TableIdentifier(tableName, Some(dbName)))
    externalCatalog.alterTable(updated)
  }

  override def alter_table_with_environmentContext(dbName: String,
                                                   tableName: String,
                                                   table: Table,
                                                   environmentContext: EnvironmentContext): Unit = {
    alter_table(dbName, tableName, table)
  }

  override def listPartitions(dbName: String, tableName: String, max: Short): util.List[Partition] = {
    val table = getTable(dbName, tableName)
    val partitionKeys = table.getPartitionKeys.asScala.map(_.getName).toList
    externalCatalog.listPartitions(dbName, tableName, None).map(fromCatalogPartition(_, dbName, tableName, partitionKeys)).asJava
  }

  override def listPartitionsByFilter(dbName: String,
                                      tableName: String,
                                      filter: String,
                                      max: Short): util.List[Partition] = {
    // Spark external catalog does not expose Hive filter-string API; fall back to listing all.
    listPartitions(dbName, tableName, max)
  }

  override def add_partitions(parts: util.List[Partition], ifNotExists: Boolean, needResults: Boolean): util.List[Partition] = {
    if (parts == null || parts.isEmpty) {
      new util.ArrayList[Partition]()
    } else {
      val first = parts.get(0)
      val db = first.getDbName
      val tbl = first.getTableName
      val catalogParts = parts.asScala.map(toCatalogPartition).toSeq
      externalCatalog.createPartitions(db, tbl, catalogParts, ignoreIfExists = ifNotExists)
      if (needResults) parts else new util.ArrayList[Partition]()
    }
  }

  override def alter_partitions(dbName: String,
                                tableName: String,
                                newParts: util.List[Partition],
                                environmentContext: EnvironmentContext): Unit = {
    externalCatalog.alterPartitions(dbName, tableName, newParts.asScala.map(toCatalogPartition).toSeq)
  }

  override def dropPartition(dbName: String, tableName: String, partName: String, deleteData: Boolean): Boolean = {
    val spec = parsePartitionClause(partName)
    externalCatalog.dropPartitions(dbName, tableName, Seq(spec), ignoreIfNotExists = true, purge = true, retainData = !deleteData)
    true
  }
  // scalastyle:on method.name

  override def tableExists(dbName: String, tableName: String): Boolean = {
    sparkSession.catalog.tableExists(dbName, tableName)
  }

  override def getDatabase(dbName: String): Database = {
    val db = externalCatalog.getDatabase(dbName)
    new Database(db.name, db.description, db.locationUri.toString, db.properties.asJava)
  }

  override def getSchema(dbName: String, tableName: String): util.List[FieldSchema] = {
    val table = externalCatalog.getTable(dbName, tableName)
    val cols = table.schema.fields.map { f =>
      new FieldSchema(f.name, f.dataType.catalogString, Option(f.getComment()).map(_.toString).getOrElse(""))
    }
    val partitionCols = table.partitionColumnNames.map { name =>
      val dt = table.partitionSchema.fields.find(_.name == name).map(_.dataType.catalogString).getOrElse("string")
      new FieldSchema(name, dt, "")
    }
    (cols ++ partitionCols).toList.asJava
  }

  override def dropTable(dbName: String, tableName: String): Unit = {
    externalCatalog.dropTable(dbName, tableName, ignoreIfNotExists = true, purge = true)
  }

  // scalastyle:off
  private def unsupported[T](): T = {
    throw new UnsupportedOperationException("Method is not supported in SparkCatalogMetaStoreClient")
  }

  // setMetaConf is no-op: HoodieHiveSyncClient.setMetaConf forwards
  // hive.metastore.callerContext.* values to the metastore for audit/tracing. With Spark's
  // external catalog there is no remote HMS to forward to, so accept the call silently
  // instead of breaking sync clients that exercise the standard IMetaStoreClient contract.
  override def setMetaConf(arg0: String, arg1: String): Unit = {}
  override def abortCompactions(arg0: org.apache.hadoop.hive.metastore.api.AbortCompactionRequest): org.apache.hadoop.hive.metastore.api.AbortCompactResponse = unsupported[org.apache.hadoop.hive.metastore.api.AbortCompactResponse]()
  override def abortTxns(arg0: java.util.List[java.lang.Long]): Unit = unsupported[Unit]()
  override def abortTxns(arg0: org.apache.hadoop.hive.metastore.api.AbortTxnsRequest): Unit = unsupported[Unit]()
  override def addCheckConstraint(arg0: java.util.List[org.apache.hadoop.hive.metastore.api.SQLCheckConstraint]): Unit = unsupported[Unit]()
  override def addDefaultConstraint(arg0: java.util.List[org.apache.hadoop.hive.metastore.api.SQLDefaultConstraint]): Unit = unsupported[Unit]()
  override def addDynamicPartitions(arg0: Long, arg1: Long, arg2: java.lang.String, arg3: java.lang.String, arg4: java.util.List[java.lang.String]): Unit = unsupported[Unit]()
  override def addDynamicPartitions(arg0: Long, arg1: Long, arg2: java.lang.String, arg3: java.lang.String, arg4: java.util.List[java.lang.String], arg5: org.apache.hadoop.hive.metastore.api.DataOperationType): Unit = unsupported[Unit]()
  override def addForeignKey(arg0: java.util.List[org.apache.hadoop.hive.metastore.api.SQLForeignKey]): Unit = unsupported[Unit]()
  override def addMasterKey(arg0: java.lang.String): Int = unsupported[Int]()
  override def addNotNullConstraint(arg0: java.util.List[org.apache.hadoop.hive.metastore.api.SQLNotNullConstraint]): Unit = unsupported[Unit]()
  override def addPackage(arg0: org.apache.hadoop.hive.metastore.api.AddPackageRequest): Unit = unsupported[Unit]()
  override def addPrimaryKey(arg0: java.util.List[org.apache.hadoop.hive.metastore.api.SQLPrimaryKey]): Unit = unsupported[Unit]()
  override def addReplicationMetrics(arg0: org.apache.hadoop.hive.metastore.api.ReplicationMetricList): Unit = unsupported[Unit]()
  override def addRuntimeStat(arg0: org.apache.hadoop.hive.metastore.api.RuntimeStat): Unit = unsupported[Unit]()
  override def addSchemaVersion(arg0: org.apache.hadoop.hive.metastore.api.SchemaVersion): Unit = unsupported[Unit]()
  override def addSerDe(arg0: org.apache.hadoop.hive.metastore.api.SerDeInfo): Unit = unsupported[Unit]()
  override def addToken(arg0: java.lang.String, arg1: java.lang.String): Boolean = unsupported[Boolean]()
  override def addUniqueConstraint(arg0: java.util.List[org.apache.hadoop.hive.metastore.api.SQLUniqueConstraint]): Unit = unsupported[Unit]()
  override def addWriteIdsToMinHistory(arg0: Long, arg1: java.util.Map[java.lang.String, java.lang.Long]): Unit = unsupported[Unit]()
  override def addWriteNotificationLog(arg0: org.apache.hadoop.hive.metastore.api.WriteNotificationLogRequest): Unit = unsupported[Unit]()
  override def addWriteNotificationLogInBatch(arg0: org.apache.hadoop.hive.metastore.api.WriteNotificationLogBatchRequest): Unit = unsupported[Unit]()
  override def add_partition(arg0: org.apache.hadoop.hive.metastore.api.Partition): org.apache.hadoop.hive.metastore.api.Partition = unsupported[org.apache.hadoop.hive.metastore.api.Partition]()
  override def add_partitions(arg0: java.util.List[org.apache.hadoop.hive.metastore.api.Partition]): Int = unsupported[Int]()
  override def add_partitions_pspec(arg0: org.apache.hadoop.hive.metastore.partition.spec.PartitionSpecProxy): Int = unsupported[Int]()
  override def allocateTableWriteId(arg0: Long, arg1: java.lang.String, arg2: java.lang.String): Long = unsupported[Long]()
  override def allocateTableWriteId(arg0: Long, arg1: java.lang.String, arg2: java.lang.String, arg3: Boolean): Long = unsupported[Long]()
  override def allocateTableWriteIdsBatch(arg0: java.util.List[java.lang.Long], arg1: java.lang.String, arg2: java.lang.String): java.util.List[org.apache.hadoop.hive.metastore.api.TxnToWriteId] = unsupported[java.util.List[org.apache.hadoop.hive.metastore.api.TxnToWriteId]]()
  override def alterCatalog(arg0: java.lang.String, arg1: org.apache.hadoop.hive.metastore.api.Catalog): Unit = unsupported[Unit]()
  override def alterDataConnector(arg0: java.lang.String, arg1: org.apache.hadoop.hive.metastore.api.DataConnector): Unit = unsupported[Unit]()
  override def alterDatabase(arg0: java.lang.String, arg1: org.apache.hadoop.hive.metastore.api.Database): Unit = unsupported[Unit]()
  override def alterDatabase(arg0: java.lang.String, arg1: java.lang.String, arg2: org.apache.hadoop.hive.metastore.api.Database): Unit = unsupported[Unit]()
  override def alterFunction(arg0: java.lang.String, arg1: java.lang.String, arg2: java.lang.String, arg3: org.apache.hadoop.hive.metastore.api.Function): Unit = unsupported[Unit]()
  override def alterFunction(arg0: java.lang.String, arg1: java.lang.String, arg2: org.apache.hadoop.hive.metastore.api.Function): Unit = unsupported[Unit]()
  override def alterISchema(arg0: java.lang.String, arg1: java.lang.String, arg2: java.lang.String, arg3: org.apache.hadoop.hive.metastore.api.ISchema): Unit = unsupported[Unit]()
  override def alterResourcePlan(arg0: java.lang.String, arg1: java.lang.String, arg2: org.apache.hadoop.hive.metastore.api.WMNullableResourcePlan, arg3: Boolean, arg4: Boolean, arg5: Boolean): org.apache.hadoop.hive.metastore.api.WMFullResourcePlan = unsupported[org.apache.hadoop.hive.metastore.api.WMFullResourcePlan]()
  override def alterWMPool(arg0: org.apache.hadoop.hive.metastore.api.WMNullablePool, arg1: java.lang.String): Unit = unsupported[Unit]()
  override def alterWMTrigger(arg0: org.apache.hadoop.hive.metastore.api.WMTrigger): Unit = unsupported[Unit]()
  override def alter_partition(arg0: java.lang.String, arg1: java.lang.String, arg2: java.lang.String, arg3: org.apache.hadoop.hive.metastore.api.Partition, arg4: org.apache.hadoop.hive.metastore.api.EnvironmentContext, arg5: java.lang.String): Unit = unsupported[Unit]()
  override def alter_partition(arg0: java.lang.String, arg1: java.lang.String, arg2: org.apache.hadoop.hive.metastore.api.Partition, arg3: org.apache.hadoop.hive.metastore.api.EnvironmentContext): Unit = unsupported[Unit]()
  override def alter_partition(arg0: java.lang.String, arg1: java.lang.String, arg2: java.lang.String, arg3: org.apache.hadoop.hive.metastore.api.Partition, arg4: org.apache.hadoop.hive.metastore.api.EnvironmentContext): Unit = unsupported[Unit]()
  override def alter_partition(arg0: java.lang.String, arg1: java.lang.String, arg2: org.apache.hadoop.hive.metastore.api.Partition): Unit = unsupported[Unit]()
  override def alter_partitions(arg0: java.lang.String, arg1: java.lang.String, arg2: java.util.List[org.apache.hadoop.hive.metastore.api.Partition], arg3: org.apache.hadoop.hive.metastore.api.EnvironmentContext, arg4: java.lang.String, arg5: Long): Unit = unsupported[Unit]()
  override def alter_partitions(arg0: java.lang.String, arg1: java.lang.String, arg2: java.lang.String, arg3: java.util.List[org.apache.hadoop.hive.metastore.api.Partition], arg4: org.apache.hadoop.hive.metastore.api.EnvironmentContext, arg5: java.lang.String, arg6: Long): Unit = unsupported[Unit]()
  override def alter_partitions(arg0: java.lang.String, arg1: java.lang.String, arg2: java.util.List[org.apache.hadoop.hive.metastore.api.Partition]): Unit = unsupported[Unit]()
  override def alter_table(arg0: java.lang.String, arg1: java.lang.String, arg2: java.lang.String, arg3: org.apache.hadoop.hive.metastore.api.Table, arg4: org.apache.hadoop.hive.metastore.api.EnvironmentContext, arg5: java.lang.String): Unit = unsupported[Unit]()
  override def alter_table(arg0: java.lang.String, arg1: java.lang.String, arg2: org.apache.hadoop.hive.metastore.api.Table, arg3: Boolean): Unit = unsupported[Unit]()
  override def alter_table(arg0: java.lang.String, arg1: java.lang.String, arg2: java.lang.String, arg3: org.apache.hadoop.hive.metastore.api.Table, arg4: org.apache.hadoop.hive.metastore.api.EnvironmentContext): Unit = unsupported[Unit]()
  override def appendPartition(arg0: java.lang.String, arg1: java.lang.String, arg2: java.util.List[java.lang.String]): org.apache.hadoop.hive.metastore.api.Partition = unsupported[org.apache.hadoop.hive.metastore.api.Partition]()
  override def appendPartition(arg0: java.lang.String, arg1: java.lang.String, arg2: java.lang.String, arg3: java.util.List[java.lang.String]): org.apache.hadoop.hive.metastore.api.Partition = unsupported[org.apache.hadoop.hive.metastore.api.Partition]()
  override def appendPartition(arg0: java.lang.String, arg1: java.lang.String, arg2: java.lang.String, arg3: java.lang.String): org.apache.hadoop.hive.metastore.api.Partition = unsupported[org.apache.hadoop.hive.metastore.api.Partition]()
  override def appendPartition(arg0: java.lang.String, arg1: java.lang.String, arg2: java.lang.String): org.apache.hadoop.hive.metastore.api.Partition = unsupported[org.apache.hadoop.hive.metastore.api.Partition]()
  override def cacheFileMetadata(arg0: java.lang.String, arg1: java.lang.String, arg2: java.lang.String, arg3: Boolean): Boolean = unsupported[Boolean]()
  override def cancelDelegationToken(arg0: java.lang.String): Unit = unsupported[Unit]()
  override def checkLock(arg0: Long): org.apache.hadoop.hive.metastore.api.LockResponse = unsupported[org.apache.hadoop.hive.metastore.api.LockResponse]()
  override def clearFileMetadata(arg0: java.util.List[java.lang.Long]): Unit = unsupported[Unit]()
  override def close(): Unit = unsupported[Unit]()
  override def commitTxn(arg0: Long): Unit = unsupported[Unit]()
  override def commitTxn(arg0: org.apache.hadoop.hive.metastore.api.CommitTxnRequest): Unit = unsupported[Unit]()
  override def commitTxnWithKeyValue(arg0: Long, arg1: Long, arg2: java.lang.String, arg3: java.lang.String): Unit = unsupported[Unit]()
  override def compact(arg0: java.lang.String, arg1: java.lang.String, arg2: java.lang.String, arg3: org.apache.hadoop.hive.metastore.api.CompactionType, arg4: java.util.Map[java.lang.String, java.lang.String]): Unit = unsupported[Unit]()
  override def compact(arg0: java.lang.String, arg1: java.lang.String, arg2: java.lang.String, arg3: org.apache.hadoop.hive.metastore.api.CompactionType): Unit = unsupported[Unit]()
  override def compact2(arg0: java.lang.String, arg1: java.lang.String, arg2: java.lang.String, arg3: org.apache.hadoop.hive.metastore.api.CompactionType, arg4: java.util.Map[java.lang.String, java.lang.String]): org.apache.hadoop.hive.metastore.api.CompactionResponse = unsupported[org.apache.hadoop.hive.metastore.api.CompactionResponse]()
  override def compact2(arg0: org.apache.hadoop.hive.metastore.api.CompactionRequest): org.apache.hadoop.hive.metastore.api.CompactionResponse = unsupported[org.apache.hadoop.hive.metastore.api.CompactionResponse]()
  override def createCatalog(arg0: org.apache.hadoop.hive.metastore.api.Catalog): Unit = unsupported[Unit]()
  override def createDataConnector(arg0: org.apache.hadoop.hive.metastore.api.DataConnector): Unit = unsupported[Unit]()
  override def createFunction(arg0: org.apache.hadoop.hive.metastore.api.Function): Unit = unsupported[Unit]()
  override def createISchema(arg0: org.apache.hadoop.hive.metastore.api.ISchema): Unit = unsupported[Unit]()
  override def createOrDropTriggerToPoolMapping(arg0: java.lang.String, arg1: java.lang.String, arg2: java.lang.String, arg3: Boolean, arg4: java.lang.String): Unit = unsupported[Unit]()
  override def createOrUpdateWMMapping(arg0: org.apache.hadoop.hive.metastore.api.WMMapping, arg1: Boolean): Unit = unsupported[Unit]()
  override def createResourcePlan(arg0: org.apache.hadoop.hive.metastore.api.WMResourcePlan, arg1: java.lang.String): Unit = unsupported[Unit]()
  override def createStoredProcedure(arg0: org.apache.hadoop.hive.metastore.api.StoredProcedure): Unit = unsupported[Unit]()
  override def createTable(arg0: org.apache.hadoop.hive.metastore.api.CreateTableRequest): Unit = unsupported[Unit]()
  override def createTableWithConstraints(arg0: org.apache.hadoop.hive.metastore.api.Table, arg1: java.util.List[org.apache.hadoop.hive.metastore.api.SQLPrimaryKey], arg2: java.util.List[org.apache.hadoop.hive.metastore.api.SQLForeignKey], arg3: java.util.List[org.apache.hadoop.hive.metastore.api.SQLUniqueConstraint], arg4: java.util.List[org.apache.hadoop.hive.metastore.api.SQLNotNullConstraint], arg5: java.util.List[org.apache.hadoop.hive.metastore.api.SQLDefaultConstraint], arg6: java.util.List[org.apache.hadoop.hive.metastore.api.SQLCheckConstraint]): Unit = unsupported[Unit]()
  override def createWMPool(arg0: org.apache.hadoop.hive.metastore.api.WMPool): Unit = unsupported[Unit]()
  override def createWMTrigger(arg0: org.apache.hadoop.hive.metastore.api.WMTrigger): Unit = unsupported[Unit]()
  override def create_role(arg0: org.apache.hadoop.hive.metastore.api.Role): Boolean = unsupported[Boolean]()
  override def deleteColumnStatistics(arg0: org.apache.hadoop.hive.metastore.api.DeleteColumnStatisticsRequest): Boolean = unsupported[Boolean]()
  override def dropCatalog(arg0: java.lang.String, arg1: Boolean): Unit = unsupported[Unit]()
  override def dropCatalog(arg0: java.lang.String): Unit = unsupported[Unit]()
  override def dropConstraint(arg0: java.lang.String, arg1: java.lang.String, arg2: java.lang.String): Unit = unsupported[Unit]()
  override def dropConstraint(arg0: java.lang.String, arg1: java.lang.String, arg2: java.lang.String, arg3: java.lang.String): Unit = unsupported[Unit]()
  override def dropDataConnector(arg0: java.lang.String, arg1: Boolean, arg2: Boolean): Unit = unsupported[Unit]()
  override def dropDatabase(arg0: org.apache.hadoop.hive.metastore.api.DropDatabaseRequest): Unit = unsupported[Unit]()
  override def dropDatabase(arg0: java.lang.String, arg1: Boolean, arg2: Boolean): Unit = unsupported[Unit]()
  override def dropDatabase(arg0: java.lang.String): Unit = unsupported[Unit]()
  override def dropDatabase(arg0: java.lang.String, arg1: Boolean, arg2: Boolean, arg3: Boolean): Unit = unsupported[Unit]()
  override def dropFunction(arg0: java.lang.String, arg1: java.lang.String): Unit = unsupported[Unit]()
  override def dropFunction(arg0: java.lang.String, arg1: java.lang.String, arg2: java.lang.String): Unit = unsupported[Unit]()
  override def dropISchema(arg0: java.lang.String, arg1: java.lang.String, arg2: java.lang.String): Unit = unsupported[Unit]()
  override def dropPackage(arg0: org.apache.hadoop.hive.metastore.api.DropPackageRequest): Unit = unsupported[Unit]()
  override def dropPartition(arg0: java.lang.String, arg1: java.lang.String, arg2: java.lang.String, arg3: java.util.List[java.lang.String], arg4: Boolean): Boolean = unsupported[Boolean]()
  override def dropPartition(arg0: java.lang.String, arg1: java.lang.String, arg2: java.util.List[java.lang.String], arg3: org.apache.hadoop.hive.metastore.PartitionDropOptions): Boolean = unsupported[Boolean]()
  override def dropPartition(arg0: java.lang.String, arg1: java.lang.String, arg2: java.util.List[java.lang.String], arg3: Boolean): Boolean = unsupported[Boolean]()
  override def dropPartition(arg0: java.lang.String, arg1: java.lang.String, arg2: java.lang.String, arg3: java.lang.String, arg4: Boolean): Boolean = unsupported[Boolean]()
  override def dropPartition(arg0: java.lang.String, arg1: java.lang.String, arg2: java.lang.String, arg3: java.util.List[java.lang.String], arg4: org.apache.hadoop.hive.metastore.PartitionDropOptions): Boolean = unsupported[Boolean]()
  override def dropPartitions(arg0: java.lang.String, arg1: java.lang.String, arg2: java.lang.String, arg3: java.util.List[org.apache.commons.lang3.tuple.Pair[java.lang.Integer, Array[Byte]]], arg4: org.apache.hadoop.hive.metastore.PartitionDropOptions): java.util.List[org.apache.hadoop.hive.metastore.api.Partition] = unsupported[java.util.List[org.apache.hadoop.hive.metastore.api.Partition]]()
  override def dropPartitions(arg0: java.lang.String, arg1: java.lang.String, arg2: java.util.List[org.apache.commons.lang3.tuple.Pair[java.lang.Integer, Array[Byte]]], arg3: org.apache.hadoop.hive.metastore.PartitionDropOptions): java.util.List[org.apache.hadoop.hive.metastore.api.Partition] = unsupported[java.util.List[org.apache.hadoop.hive.metastore.api.Partition]]()
  override def dropPartitions(arg0: java.lang.String, arg1: java.lang.String, arg2: java.util.List[org.apache.commons.lang3.tuple.Pair[java.lang.Integer, Array[Byte]]], arg3: Boolean, arg4: Boolean, arg5: Boolean): java.util.List[org.apache.hadoop.hive.metastore.api.Partition] = unsupported[java.util.List[org.apache.hadoop.hive.metastore.api.Partition]]()
  override def dropPartitions(arg0: java.lang.String, arg1: java.lang.String, arg2: java.util.List[org.apache.commons.lang3.tuple.Pair[java.lang.Integer, Array[Byte]]], arg3: Boolean, arg4: Boolean): java.util.List[org.apache.hadoop.hive.metastore.api.Partition] = unsupported[java.util.List[org.apache.hadoop.hive.metastore.api.Partition]]()
  override def dropResourcePlan(arg0: java.lang.String, arg1: java.lang.String): Unit = unsupported[Unit]()
  override def dropSchemaVersion(arg0: java.lang.String, arg1: java.lang.String, arg2: java.lang.String, arg3: Int): Unit = unsupported[Unit]()
  override def dropStoredProcedure(arg0: org.apache.hadoop.hive.metastore.api.StoredProcedureRequest): Unit = unsupported[Unit]()
  override def dropTable(arg0: java.lang.String, arg1: java.lang.String, arg2: java.lang.String, arg3: Boolean, arg4: Boolean, arg5: Boolean): Unit = unsupported[Unit]()
  override def dropTable(arg0: java.lang.String, arg1: java.lang.String, arg2: Boolean, arg3: Boolean, arg4: Boolean): Unit = unsupported[Unit]()
  override def dropTable(arg0: org.apache.hadoop.hive.metastore.api.Table, arg1: Boolean, arg2: Boolean, arg3: Boolean): Unit = unsupported[Unit]()
  override def dropTable(arg0: java.lang.String, arg1: java.lang.String, arg2: Boolean, arg3: Boolean): Unit = unsupported[Unit]()
  override def dropWMMapping(arg0: org.apache.hadoop.hive.metastore.api.WMMapping): Unit = unsupported[Unit]()
  override def dropWMPool(arg0: java.lang.String, arg1: java.lang.String, arg2: java.lang.String): Unit = unsupported[Unit]()
  override def dropWMTrigger(arg0: java.lang.String, arg1: java.lang.String, arg2: java.lang.String): Unit = unsupported[Unit]()
  override def drop_role(arg0: java.lang.String): Boolean = unsupported[Boolean]()
  override def exchange_partition(arg0: java.util.Map[java.lang.String, java.lang.String], arg1: java.lang.String, arg2: java.lang.String, arg3: java.lang.String, arg4: java.lang.String): org.apache.hadoop.hive.metastore.api.Partition = unsupported[org.apache.hadoop.hive.metastore.api.Partition]()
  override def exchange_partition(arg0: java.util.Map[java.lang.String, java.lang.String], arg1: java.lang.String, arg2: java.lang.String, arg3: java.lang.String, arg4: java.lang.String, arg5: java.lang.String, arg6: java.lang.String): org.apache.hadoop.hive.metastore.api.Partition = unsupported[org.apache.hadoop.hive.metastore.api.Partition]()
  override def exchange_partitions(arg0: java.util.Map[java.lang.String, java.lang.String], arg1: java.lang.String, arg2: java.lang.String, arg3: java.lang.String, arg4: java.lang.String, arg5: java.lang.String, arg6: java.lang.String): java.util.List[org.apache.hadoop.hive.metastore.api.Partition] = unsupported[java.util.List[org.apache.hadoop.hive.metastore.api.Partition]]()
  override def exchange_partitions(arg0: java.util.Map[java.lang.String, java.lang.String], arg1: java.lang.String, arg2: java.lang.String, arg3: java.lang.String, arg4: java.lang.String): java.util.List[org.apache.hadoop.hive.metastore.api.Partition] = unsupported[java.util.List[org.apache.hadoop.hive.metastore.api.Partition]]()
  override def findColumnsWithStats(arg0: org.apache.hadoop.hive.metastore.api.CompactionInfoStruct): java.util.List[java.lang.String] = unsupported[java.util.List[java.lang.String]]()
  override def findNextCompact(arg0: java.lang.String): org.apache.hadoop.hive.metastore.api.OptionalCompactionInfoStruct = unsupported[org.apache.hadoop.hive.metastore.api.OptionalCompactionInfoStruct]()
  override def findNextCompact(arg0: org.apache.hadoop.hive.metastore.api.FindNextCompactRequest): org.apache.hadoop.hive.metastore.api.OptionalCompactionInfoStruct = unsupported[org.apache.hadoop.hive.metastore.api.OptionalCompactionInfoStruct]()
  override def findPackage(arg0: org.apache.hadoop.hive.metastore.api.GetPackageRequest): org.apache.hadoop.hive.metastore.api.Package = unsupported[org.apache.hadoop.hive.metastore.api.Package]()
  override def fireListenerEvent(arg0: org.apache.hadoop.hive.metastore.api.FireEventRequest): org.apache.hadoop.hive.metastore.api.FireEventResponse = unsupported[org.apache.hadoop.hive.metastore.api.FireEventResponse]()
  override def flushCache(): Unit = unsupported[Unit]()
  override def getActiveResourcePlan(arg0: java.lang.String): org.apache.hadoop.hive.metastore.api.WMFullResourcePlan = unsupported[org.apache.hadoop.hive.metastore.api.WMFullResourcePlan]()
  override def getAggrColStatsFor(arg0: java.lang.String, arg1: java.lang.String, arg2: java.util.List[java.lang.String], arg3: java.util.List[java.lang.String], arg4: java.lang.String): org.apache.hadoop.hive.metastore.api.AggrStats = unsupported[org.apache.hadoop.hive.metastore.api.AggrStats]()
  override def getAggrColStatsFor(arg0: java.lang.String, arg1: java.lang.String, arg2: java.util.List[java.lang.String], arg3: java.util.List[java.lang.String], arg4: java.lang.String, arg5: java.lang.String): org.apache.hadoop.hive.metastore.api.AggrStats = unsupported[org.apache.hadoop.hive.metastore.api.AggrStats]()
  override def getAggrColStatsFor(arg0: java.lang.String, arg1: java.lang.String, arg2: java.lang.String, arg3: java.util.List[java.lang.String], arg4: java.util.List[java.lang.String], arg5: java.lang.String, arg6: java.lang.String): org.apache.hadoop.hive.metastore.api.AggrStats = unsupported[org.apache.hadoop.hive.metastore.api.AggrStats]()
  override def getAggrColStatsFor(arg0: java.lang.String, arg1: java.lang.String, arg2: java.lang.String, arg3: java.util.List[java.lang.String], arg4: java.util.List[java.lang.String], arg5: java.lang.String): org.apache.hadoop.hive.metastore.api.AggrStats = unsupported[org.apache.hadoop.hive.metastore.api.AggrStats]()
  override def getAllDataConnectorNames(): java.util.List[java.lang.String] = unsupported[java.util.List[java.lang.String]]()
  override def getAllDatabases(): java.util.List[java.lang.String] = unsupported[java.util.List[java.lang.String]]()
  override def getAllDatabases(arg0: java.lang.String): java.util.List[java.lang.String] = unsupported[java.util.List[java.lang.String]]()
  override def getAllFunctions(): org.apache.hadoop.hive.metastore.api.GetAllFunctionsResponse = unsupported[org.apache.hadoop.hive.metastore.api.GetAllFunctionsResponse]()
  override def getAllMaterializedViewObjectsForRewriting(): java.util.List[org.apache.hadoop.hive.metastore.api.Table] = unsupported[java.util.List[org.apache.hadoop.hive.metastore.api.Table]]()
  override def getAllResourcePlans(arg0: java.lang.String): java.util.List[org.apache.hadoop.hive.metastore.api.WMResourcePlan] = unsupported[java.util.List[org.apache.hadoop.hive.metastore.api.WMResourcePlan]]()
  override def getAllStoredProcedures(arg0: org.apache.hadoop.hive.metastore.api.ListStoredProcedureRequest): java.util.List[java.lang.String] = unsupported[java.util.List[java.lang.String]]()
  override def getAllTableConstraints(arg0: org.apache.hadoop.hive.metastore.api.AllTableConstraintsRequest): org.apache.hadoop.hive.metastore.api.SQLAllTableConstraints = unsupported[org.apache.hadoop.hive.metastore.api.SQLAllTableConstraints]()
  override def getAllTables(arg0: java.lang.String, arg1: java.lang.String): java.util.List[java.lang.String] = unsupported[java.util.List[java.lang.String]]()
  override def getAllTables(arg0: java.lang.String): java.util.List[java.lang.String] = unsupported[java.util.List[java.lang.String]]()
  override def getAllTokenIdentifiers(): java.util.List[java.lang.String] = unsupported[java.util.List[java.lang.String]]()
  override def getAllWriteEventInfo(arg0: org.apache.hadoop.hive.metastore.api.GetAllWriteEventInfoRequest): java.util.List[org.apache.hadoop.hive.metastore.api.WriteEventInfo] = unsupported[java.util.List[org.apache.hadoop.hive.metastore.api.WriteEventInfo]]()
  override def getCatalog(arg0: java.lang.String): org.apache.hadoop.hive.metastore.api.Catalog = unsupported[org.apache.hadoop.hive.metastore.api.Catalog]()
  override def getCatalogs(): java.util.List[java.lang.String] = unsupported[java.util.List[java.lang.String]]()
  override def getCheckConstraints(arg0: org.apache.hadoop.hive.metastore.api.CheckConstraintsRequest): java.util.List[org.apache.hadoop.hive.metastore.api.SQLCheckConstraint] = unsupported[java.util.List[org.apache.hadoop.hive.metastore.api.SQLCheckConstraint]]()
  override def getConfigValue(arg0: java.lang.String, arg1: java.lang.String): java.lang.String = unsupported[java.lang.String]()
  override def getCurrentNotificationEventId(): org.apache.hadoop.hive.metastore.api.CurrentNotificationEventId = unsupported[org.apache.hadoop.hive.metastore.api.CurrentNotificationEventId]()
  override def getDataConnector(arg0: java.lang.String): org.apache.hadoop.hive.metastore.api.DataConnector = unsupported[org.apache.hadoop.hive.metastore.api.DataConnector]()
  override def getDatabase(arg0: java.lang.String, arg1: java.lang.String): org.apache.hadoop.hive.metastore.api.Database = unsupported[org.apache.hadoop.hive.metastore.api.Database]()
  override def getDatabases(arg0: java.lang.String, arg1: java.lang.String): java.util.List[java.lang.String] = unsupported[java.util.List[java.lang.String]]()
  override def getDatabases(arg0: java.lang.String): java.util.List[java.lang.String] = unsupported[java.util.List[java.lang.String]]()
  override def getDefaultConstraints(arg0: org.apache.hadoop.hive.metastore.api.DefaultConstraintsRequest): java.util.List[org.apache.hadoop.hive.metastore.api.SQLDefaultConstraint] = unsupported[java.util.List[org.apache.hadoop.hive.metastore.api.SQLDefaultConstraint]]()
  override def getDelegationToken(arg0: java.lang.String, arg1: java.lang.String): java.lang.String = unsupported[java.lang.String]()
  override def getFields(arg0: java.lang.String, arg1: java.lang.String, arg2: java.lang.String): java.util.List[org.apache.hadoop.hive.metastore.api.FieldSchema] = unsupported[java.util.List[org.apache.hadoop.hive.metastore.api.FieldSchema]]()
  override def getFields(arg0: java.lang.String, arg1: java.lang.String): java.util.List[org.apache.hadoop.hive.metastore.api.FieldSchema] = unsupported[java.util.List[org.apache.hadoop.hive.metastore.api.FieldSchema]]()
  override def getFieldsRequest(arg0: org.apache.hadoop.hive.metastore.api.GetFieldsRequest): org.apache.hadoop.hive.metastore.api.GetFieldsResponse = unsupported[org.apache.hadoop.hive.metastore.api.GetFieldsResponse]()
  override def getFileMetadata(arg0: java.util.List[java.lang.Long]): java.lang.Iterable[java.util.Map.Entry[java.lang.Long, java.nio.ByteBuffer]] = unsupported[java.lang.Iterable[java.util.Map.Entry[java.lang.Long, java.nio.ByteBuffer]]]()
  override def getFileMetadataBySarg(arg0: java.util.List[java.lang.Long], arg1: java.nio.ByteBuffer, arg2: Boolean): java.lang.Iterable[java.util.Map.Entry[java.lang.Long, org.apache.hadoop.hive.metastore.api.MetadataPpdResult]] = unsupported[java.lang.Iterable[java.util.Map.Entry[java.lang.Long, org.apache.hadoop.hive.metastore.api.MetadataPpdResult]]]()
  override def getForeignKeys(arg0: org.apache.hadoop.hive.metastore.api.ForeignKeysRequest): java.util.List[org.apache.hadoop.hive.metastore.api.SQLForeignKey] = unsupported[java.util.List[org.apache.hadoop.hive.metastore.api.SQLForeignKey]]()
  override def getFunction(arg0: java.lang.String, arg1: java.lang.String): org.apache.hadoop.hive.metastore.api.Function = unsupported[org.apache.hadoop.hive.metastore.api.Function]()
  override def getFunction(arg0: java.lang.String, arg1: java.lang.String, arg2: java.lang.String): org.apache.hadoop.hive.metastore.api.Function = unsupported[org.apache.hadoop.hive.metastore.api.Function]()
  override def getFunctions(arg0: java.lang.String, arg1: java.lang.String): java.util.List[java.lang.String] = unsupported[java.util.List[java.lang.String]]()
  override def getFunctions(arg0: java.lang.String, arg1: java.lang.String, arg2: java.lang.String): java.util.List[java.lang.String] = unsupported[java.util.List[java.lang.String]]()
  override def getFunctionsRequest(arg0: org.apache.hadoop.hive.metastore.api.GetFunctionsRequest): org.apache.hadoop.hive.metastore.api.GetFunctionsResponse = unsupported[org.apache.hadoop.hive.metastore.api.GetFunctionsResponse]()
  override def getISchema(arg0: java.lang.String, arg1: java.lang.String, arg2: java.lang.String): org.apache.hadoop.hive.metastore.api.ISchema = unsupported[org.apache.hadoop.hive.metastore.api.ISchema]()
  override def getLatestCommittedCompactionInfo(arg0: org.apache.hadoop.hive.metastore.api.GetLatestCommittedCompactionInfoRequest): org.apache.hadoop.hive.metastore.api.GetLatestCommittedCompactionInfoResponse = unsupported[org.apache.hadoop.hive.metastore.api.GetLatestCommittedCompactionInfoResponse]()
  override def getLatestTxnIdInConflict(arg0: Long): Long = unsupported[Long]()
  override def getMasterKeys(): Array[java.lang.String] = unsupported[Array[java.lang.String]]()
  override def getMaterializationInvalidationInfo(arg0: org.apache.hadoop.hive.metastore.api.CreationMetadata, arg1: java.lang.String): org.apache.hadoop.hive.metastore.api.Materialization = unsupported[org.apache.hadoop.hive.metastore.api.Materialization]()
  override def getMaterializedViewsForRewriting(arg0: java.lang.String, arg1: java.lang.String): java.util.List[java.lang.String] = unsupported[java.util.List[java.lang.String]]()
  override def getMaterializedViewsForRewriting(arg0: java.lang.String): java.util.List[java.lang.String] = unsupported[java.util.List[java.lang.String]]()
  override def getMaxAllocatedWriteId(arg0: java.lang.String, arg1: java.lang.String): Long = unsupported[Long]()
  override def getMetaConf(arg0: java.lang.String): java.lang.String = unsupported[java.lang.String]()
  override def getMetastoreDbUuid(): java.lang.String = unsupported[java.lang.String]()
  override def getNextNotification(arg0: Long, arg1: Int, arg2: org.apache.hadoop.hive.metastore.IMetaStoreClient.NotificationFilter): org.apache.hadoop.hive.metastore.api.NotificationEventResponse = unsupported[org.apache.hadoop.hive.metastore.api.NotificationEventResponse]()
  override def getNextNotification(arg0: org.apache.hadoop.hive.metastore.api.NotificationEventRequest, arg1: Boolean, arg2: org.apache.hadoop.hive.metastore.IMetaStoreClient.NotificationFilter): org.apache.hadoop.hive.metastore.api.NotificationEventResponse = unsupported[org.apache.hadoop.hive.metastore.api.NotificationEventResponse]()
  override def getNotNullConstraints(arg0: org.apache.hadoop.hive.metastore.api.NotNullConstraintsRequest): java.util.List[org.apache.hadoop.hive.metastore.api.SQLNotNullConstraint] = unsupported[java.util.List[org.apache.hadoop.hive.metastore.api.SQLNotNullConstraint]]()
  override def getNotificationEventsCount(arg0: org.apache.hadoop.hive.metastore.api.NotificationEventsCountRequest): org.apache.hadoop.hive.metastore.api.NotificationEventsCountResponse = unsupported[org.apache.hadoop.hive.metastore.api.NotificationEventsCountResponse]()
  override def getNumPartitionsByFilter(arg0: java.lang.String, arg1: java.lang.String, arg2: java.lang.String, arg3: java.lang.String): Int = unsupported[Int]()
  override def getNumPartitionsByFilter(arg0: java.lang.String, arg1: java.lang.String, arg2: java.lang.String): Int = unsupported[Int]()
  override def getOpenTxns(): org.apache.hadoop.hive.metastore.api.GetOpenTxnsResponse = unsupported[org.apache.hadoop.hive.metastore.api.GetOpenTxnsResponse]()
  override def getPartition(arg0: java.lang.String, arg1: java.lang.String, arg2: java.util.List[java.lang.String]): org.apache.hadoop.hive.metastore.api.Partition = unsupported[org.apache.hadoop.hive.metastore.api.Partition]()
  override def getPartition(arg0: java.lang.String, arg1: java.lang.String, arg2: java.lang.String, arg3: java.lang.String): org.apache.hadoop.hive.metastore.api.Partition = unsupported[org.apache.hadoop.hive.metastore.api.Partition]()
  override def getPartition(arg0: java.lang.String, arg1: java.lang.String, arg2: java.lang.String): org.apache.hadoop.hive.metastore.api.Partition = unsupported[org.apache.hadoop.hive.metastore.api.Partition]()
  override def getPartition(arg0: java.lang.String, arg1: java.lang.String, arg2: java.lang.String, arg3: java.util.List[java.lang.String]): org.apache.hadoop.hive.metastore.api.Partition = unsupported[org.apache.hadoop.hive.metastore.api.Partition]()
  override def getPartitionColumnStatistics(arg0: java.lang.String, arg1: java.lang.String, arg2: java.lang.String, arg3: java.util.List[java.lang.String], arg4: java.util.List[java.lang.String], arg5: java.lang.String, arg6: java.lang.String): java.util.Map[java.lang.String, java.util.List[org.apache.hadoop.hive.metastore.api.ColumnStatisticsObj]] = unsupported[java.util.Map[java.lang.String, java.util.List[org.apache.hadoop.hive.metastore.api.ColumnStatisticsObj]]]()
  override def getPartitionColumnStatistics(arg0: java.lang.String, arg1: java.lang.String, arg2: java.lang.String, arg3: java.util.List[java.lang.String], arg4: java.util.List[java.lang.String], arg5: java.lang.String): java.util.Map[java.lang.String, java.util.List[org.apache.hadoop.hive.metastore.api.ColumnStatisticsObj]] = unsupported[java.util.Map[java.lang.String, java.util.List[org.apache.hadoop.hive.metastore.api.ColumnStatisticsObj]]]()
  override def getPartitionColumnStatistics(arg0: java.lang.String, arg1: java.lang.String, arg2: java.util.List[java.lang.String], arg3: java.util.List[java.lang.String], arg4: java.lang.String, arg5: java.lang.String): java.util.Map[java.lang.String, java.util.List[org.apache.hadoop.hive.metastore.api.ColumnStatisticsObj]] = unsupported[java.util.Map[java.lang.String, java.util.List[org.apache.hadoop.hive.metastore.api.ColumnStatisticsObj]]]()
  override def getPartitionColumnStatistics(arg0: java.lang.String, arg1: java.lang.String, arg2: java.util.List[java.lang.String], arg3: java.util.List[java.lang.String], arg4: java.lang.String): java.util.Map[java.lang.String, java.util.List[org.apache.hadoop.hive.metastore.api.ColumnStatisticsObj]] = unsupported[java.util.Map[java.lang.String, java.util.List[org.apache.hadoop.hive.metastore.api.ColumnStatisticsObj]]]()
  override def getPartitionRequest(arg0: org.apache.hadoop.hive.metastore.api.GetPartitionRequest): org.apache.hadoop.hive.metastore.api.GetPartitionResponse = unsupported[org.apache.hadoop.hive.metastore.api.GetPartitionResponse]()
  override def getPartitionWithAuthInfo(arg0: java.lang.String, arg1: java.lang.String, arg2: java.util.List[java.lang.String], arg3: java.lang.String, arg4: java.util.List[java.lang.String]): org.apache.hadoop.hive.metastore.api.Partition = unsupported[org.apache.hadoop.hive.metastore.api.Partition]()
  override def getPartitionWithAuthInfo(arg0: java.lang.String, arg1: java.lang.String, arg2: java.lang.String, arg3: java.util.List[java.lang.String], arg4: java.lang.String, arg5: java.util.List[java.lang.String]): org.apache.hadoop.hive.metastore.api.Partition = unsupported[org.apache.hadoop.hive.metastore.api.Partition]()
  override def getPartitionsByNames(arg0: java.lang.String, arg1: java.lang.String, arg2: java.util.List[java.lang.String]): java.util.List[org.apache.hadoop.hive.metastore.api.Partition] = unsupported[java.util.List[org.apache.hadoop.hive.metastore.api.Partition]]()
  override def getPartitionsByNames(arg0: java.lang.String, arg1: java.lang.String, arg2: java.lang.String, arg3: java.util.List[java.lang.String]): java.util.List[org.apache.hadoop.hive.metastore.api.Partition] = unsupported[java.util.List[org.apache.hadoop.hive.metastore.api.Partition]]()
  override def getPartitionsByNames(arg0: org.apache.hadoop.hive.metastore.api.GetPartitionsByNamesRequest): org.apache.hadoop.hive.metastore.api.GetPartitionsByNamesResult = unsupported[org.apache.hadoop.hive.metastore.api.GetPartitionsByNamesResult]()
  override def getPartitionsRequest(arg0: org.apache.hadoop.hive.metastore.api.PartitionsRequest): org.apache.hadoop.hive.metastore.api.PartitionsResponse = unsupported[org.apache.hadoop.hive.metastore.api.PartitionsResponse]()
  override def getPartitionsWithSpecs(arg0: org.apache.hadoop.hive.metastore.api.GetPartitionsRequest): org.apache.hadoop.hive.metastore.api.GetPartitionsResponse = unsupported[org.apache.hadoop.hive.metastore.api.GetPartitionsResponse]()
  override def getPrimaryKeys(arg0: org.apache.hadoop.hive.metastore.api.PrimaryKeysRequest): java.util.List[org.apache.hadoop.hive.metastore.api.SQLPrimaryKey] = unsupported[java.util.List[org.apache.hadoop.hive.metastore.api.SQLPrimaryKey]]()
  override def getReplicationMetrics(arg0: org.apache.hadoop.hive.metastore.api.GetReplicationMetricsRequest): org.apache.hadoop.hive.metastore.api.ReplicationMetricList = unsupported[org.apache.hadoop.hive.metastore.api.ReplicationMetricList]()
  override def getResourcePlan(arg0: java.lang.String, arg1: java.lang.String): org.apache.hadoop.hive.metastore.api.WMFullResourcePlan = unsupported[org.apache.hadoop.hive.metastore.api.WMFullResourcePlan]()
  override def getRuntimeStats(arg0: Int, arg1: Int): java.util.List[org.apache.hadoop.hive.metastore.api.RuntimeStat] = unsupported[java.util.List[org.apache.hadoop.hive.metastore.api.RuntimeStat]]()
  override def getScheduledQuery(arg0: org.apache.hadoop.hive.metastore.api.ScheduledQueryKey): org.apache.hadoop.hive.metastore.api.ScheduledQuery = unsupported[org.apache.hadoop.hive.metastore.api.ScheduledQuery]()
  override def getSchema(arg0: java.lang.String, arg1: java.lang.String, arg2: java.lang.String): java.util.List[org.apache.hadoop.hive.metastore.api.FieldSchema] = unsupported[java.util.List[org.apache.hadoop.hive.metastore.api.FieldSchema]]()
  override def getSchemaAllVersions(arg0: java.lang.String, arg1: java.lang.String, arg2: java.lang.String): java.util.List[org.apache.hadoop.hive.metastore.api.SchemaVersion] = unsupported[java.util.List[org.apache.hadoop.hive.metastore.api.SchemaVersion]]()
  override def getSchemaByCols(arg0: org.apache.hadoop.hive.metastore.api.FindSchemasByColsRqst): org.apache.hadoop.hive.metastore.api.FindSchemasByColsResp = unsupported[org.apache.hadoop.hive.metastore.api.FindSchemasByColsResp]()
  override def getSchemaLatestVersion(arg0: java.lang.String, arg1: java.lang.String, arg2: java.lang.String): org.apache.hadoop.hive.metastore.api.SchemaVersion = unsupported[org.apache.hadoop.hive.metastore.api.SchemaVersion]()
  override def getSchemaRequest(arg0: org.apache.hadoop.hive.metastore.api.GetSchemaRequest): org.apache.hadoop.hive.metastore.api.GetSchemaResponse = unsupported[org.apache.hadoop.hive.metastore.api.GetSchemaResponse]()
  override def getSchemaVersion(arg0: java.lang.String, arg1: java.lang.String, arg2: java.lang.String, arg3: Int): org.apache.hadoop.hive.metastore.api.SchemaVersion = unsupported[org.apache.hadoop.hive.metastore.api.SchemaVersion]()
  override def getSerDe(arg0: java.lang.String): org.apache.hadoop.hive.metastore.api.SerDeInfo = unsupported[org.apache.hadoop.hive.metastore.api.SerDeInfo]()
  override def getServerVersion(): java.lang.String = unsupported[java.lang.String]()
  override def getStoredProcedure(arg0: org.apache.hadoop.hive.metastore.api.StoredProcedureRequest): org.apache.hadoop.hive.metastore.api.StoredProcedure = unsupported[org.apache.hadoop.hive.metastore.api.StoredProcedure]()
  override def getTable(arg0: java.lang.String, arg1: java.lang.String, arg2: Boolean, arg3: java.lang.String): org.apache.hadoop.hive.metastore.api.Table = unsupported[org.apache.hadoop.hive.metastore.api.Table]()
  override def getTable(arg0: org.apache.hadoop.hive.metastore.api.GetTableRequest): org.apache.hadoop.hive.metastore.api.Table = unsupported[org.apache.hadoop.hive.metastore.api.Table]()
  override def getTable(arg0: java.lang.String, arg1: java.lang.String, arg2: java.lang.String, arg3: java.lang.String, arg4: Boolean, arg5: java.lang.String): org.apache.hadoop.hive.metastore.api.Table = unsupported[org.apache.hadoop.hive.metastore.api.Table]()
  override def getTable(arg0: java.lang.String, arg1: java.lang.String, arg2: java.lang.String, arg3: java.lang.String): org.apache.hadoop.hive.metastore.api.Table = unsupported[org.apache.hadoop.hive.metastore.api.Table]()
  override def getTable(arg0: java.lang.String, arg1: java.lang.String, arg2: java.lang.String): org.apache.hadoop.hive.metastore.api.Table = unsupported[org.apache.hadoop.hive.metastore.api.Table]()
  override def getTableColumnStatistics(arg0: java.lang.String, arg1: java.lang.String, arg2: java.lang.String, arg3: java.util.List[java.lang.String], arg4: java.lang.String, arg5: java.lang.String): java.util.List[org.apache.hadoop.hive.metastore.api.ColumnStatisticsObj] = unsupported[java.util.List[org.apache.hadoop.hive.metastore.api.ColumnStatisticsObj]]()
  override def getTableColumnStatistics(arg0: java.lang.String, arg1: java.lang.String, arg2: java.util.List[java.lang.String], arg3: java.lang.String, arg4: java.lang.String): java.util.List[org.apache.hadoop.hive.metastore.api.ColumnStatisticsObj] = unsupported[java.util.List[org.apache.hadoop.hive.metastore.api.ColumnStatisticsObj]]()
  override def getTableColumnStatistics(arg0: java.lang.String, arg1: java.lang.String, arg2: java.lang.String, arg3: java.util.List[java.lang.String], arg4: java.lang.String): java.util.List[org.apache.hadoop.hive.metastore.api.ColumnStatisticsObj] = unsupported[java.util.List[org.apache.hadoop.hive.metastore.api.ColumnStatisticsObj]]()
  override def getTableColumnStatistics(arg0: java.lang.String, arg1: java.lang.String, arg2: java.util.List[java.lang.String], arg3: java.lang.String): java.util.List[org.apache.hadoop.hive.metastore.api.ColumnStatisticsObj] = unsupported[java.util.List[org.apache.hadoop.hive.metastore.api.ColumnStatisticsObj]]()
  override def getTableMeta(arg0: java.lang.String, arg1: java.lang.String, arg2: java.util.List[java.lang.String]): java.util.List[org.apache.hadoop.hive.metastore.api.TableMeta] = unsupported[java.util.List[org.apache.hadoop.hive.metastore.api.TableMeta]]()
  override def getTableMeta(arg0: java.lang.String, arg1: java.lang.String, arg2: java.lang.String, arg3: java.util.List[java.lang.String]): java.util.List[org.apache.hadoop.hive.metastore.api.TableMeta] = unsupported[java.util.List[org.apache.hadoop.hive.metastore.api.TableMeta]]()
  override def getTableObjectsByName(arg0: java.lang.String, arg1: java.util.List[java.lang.String]): java.util.List[org.apache.hadoop.hive.metastore.api.Table] = unsupported[java.util.List[org.apache.hadoop.hive.metastore.api.Table]]()
  override def getTableObjectsByName(arg0: java.lang.String, arg1: java.lang.String, arg2: java.util.List[java.lang.String]): java.util.List[org.apache.hadoop.hive.metastore.api.Table] = unsupported[java.util.List[org.apache.hadoop.hive.metastore.api.Table]]()
  override def getTables(arg0: java.lang.String, arg1: java.lang.String, arg2: java.lang.String, arg3: org.apache.hadoop.hive.metastore.TableType): java.util.List[java.lang.String] = unsupported[java.util.List[java.lang.String]]()
  override def getTables(arg0: java.lang.String, arg1: java.lang.String, arg2: org.apache.hadoop.hive.metastore.TableType): java.util.List[java.lang.String] = unsupported[java.util.List[java.lang.String]]()
  override def getTables(arg0: java.lang.String, arg1: java.lang.String, arg2: java.lang.String): java.util.List[java.lang.String] = unsupported[java.util.List[java.lang.String]]()
  override def getTables(arg0: java.lang.String, arg1: java.lang.String): java.util.List[java.lang.String] = unsupported[java.util.List[java.lang.String]]()
  override def getTables(arg0: java.lang.String, arg1: java.lang.String, arg2: java.util.List[java.lang.String], arg3: org.apache.hadoop.hive.metastore.api.GetProjectionsSpec): java.util.List[org.apache.hadoop.hive.metastore.api.Table] = unsupported[java.util.List[org.apache.hadoop.hive.metastore.api.Table]]()
  override def getTablesExt(arg0: java.lang.String, arg1: java.lang.String, arg2: java.lang.String, arg3: Int, arg4: Int): java.util.List[org.apache.hadoop.hive.metastore.api.ExtendedTableInfo] = unsupported[java.util.List[org.apache.hadoop.hive.metastore.api.ExtendedTableInfo]]()
  override def getToken(arg0: java.lang.String): java.lang.String = unsupported[java.lang.String]()
  override def getTokenStrForm(): java.lang.String = unsupported[java.lang.String]()
  override def getTranslateTableDryrun(arg0: org.apache.hadoop.hive.metastore.api.Table): org.apache.hadoop.hive.metastore.api.Table = unsupported[org.apache.hadoop.hive.metastore.api.Table]()
  override def getTriggersForResourcePlan(arg0: java.lang.String, arg1: java.lang.String): java.util.List[org.apache.hadoop.hive.metastore.api.WMTrigger] = unsupported[java.util.List[org.apache.hadoop.hive.metastore.api.WMTrigger]]()
  override def getUniqueConstraints(arg0: org.apache.hadoop.hive.metastore.api.UniqueConstraintsRequest): java.util.List[org.apache.hadoop.hive.metastore.api.SQLUniqueConstraint] = unsupported[java.util.List[org.apache.hadoop.hive.metastore.api.SQLUniqueConstraint]]()
  override def getValidTxns(arg0: Long, arg1: java.util.List[org.apache.hadoop.hive.metastore.api.TxnType]): org.apache.hadoop.hive.common.ValidTxnList = unsupported[org.apache.hadoop.hive.common.ValidTxnList]()
  override def getValidTxns(): org.apache.hadoop.hive.common.ValidTxnList = unsupported[org.apache.hadoop.hive.common.ValidTxnList]()
  override def getValidTxns(arg0: Long): org.apache.hadoop.hive.common.ValidTxnList = unsupported[org.apache.hadoop.hive.common.ValidTxnList]()
  override def getValidWriteIds(arg0: java.util.List[java.lang.String], arg1: java.lang.String): java.util.List[org.apache.hadoop.hive.metastore.api.TableValidWriteIds] = unsupported[java.util.List[org.apache.hadoop.hive.metastore.api.TableValidWriteIds]]()
  override def getValidWriteIds(arg0: java.lang.String): org.apache.hadoop.hive.common.ValidWriteIdList = unsupported[org.apache.hadoop.hive.common.ValidWriteIdList]()
  override def getValidWriteIds(arg0: java.lang.String, arg1: java.lang.Long): org.apache.hadoop.hive.common.ValidWriteIdList = unsupported[org.apache.hadoop.hive.common.ValidWriteIdList]()
  override def get_databases_req(arg0: org.apache.hadoop.hive.metastore.api.GetDatabaseObjectsRequest): org.apache.hadoop.hive.metastore.api.GetDatabaseObjectsResponse = unsupported[org.apache.hadoop.hive.metastore.api.GetDatabaseObjectsResponse]()
  override def get_principals_in_role(arg0: org.apache.hadoop.hive.metastore.api.GetPrincipalsInRoleRequest): org.apache.hadoop.hive.metastore.api.GetPrincipalsInRoleResponse = unsupported[org.apache.hadoop.hive.metastore.api.GetPrincipalsInRoleResponse]()
  override def get_privilege_set(arg0: org.apache.hadoop.hive.metastore.api.HiveObjectRef, arg1: java.lang.String, arg2: java.util.List[java.lang.String]): org.apache.hadoop.hive.metastore.api.PrincipalPrivilegeSet = unsupported[org.apache.hadoop.hive.metastore.api.PrincipalPrivilegeSet]()
  override def get_role_grants_for_principal(arg0: org.apache.hadoop.hive.metastore.api.GetRoleGrantsForPrincipalRequest): org.apache.hadoop.hive.metastore.api.GetRoleGrantsForPrincipalResponse = unsupported[org.apache.hadoop.hive.metastore.api.GetRoleGrantsForPrincipalResponse]()
  override def grant_privileges(arg0: org.apache.hadoop.hive.metastore.api.PrivilegeBag): Boolean = unsupported[Boolean]()
  override def grant_role(arg0: java.lang.String, arg1: java.lang.String, arg2: org.apache.hadoop.hive.metastore.api.PrincipalType, arg3: java.lang.String, arg4: org.apache.hadoop.hive.metastore.api.PrincipalType, arg5: Boolean): Boolean = unsupported[Boolean]()
  override def heartbeat(arg0: Long, arg1: Long): Unit = unsupported[Unit]()
  override def heartbeatLockMaterializationRebuild(arg0: java.lang.String, arg1: java.lang.String, arg2: Long): Boolean = unsupported[Boolean]()
  override def heartbeatTxnRange(arg0: Long, arg1: Long): org.apache.hadoop.hive.metastore.api.HeartbeatTxnRangeResponse = unsupported[org.apache.hadoop.hive.metastore.api.HeartbeatTxnRangeResponse]()
  override def insertTable(arg0: org.apache.hadoop.hive.metastore.api.Table, arg1: Boolean): Unit = unsupported[Unit]()
  override def isCompatibleWith(arg0: org.apache.hadoop.conf.Configuration): Boolean = unsupported[Boolean]()
  override def isLocalMetaStore(): Boolean = unsupported[Boolean]()
  override def isPartitionMarkedForEvent(arg0: java.lang.String, arg1: java.lang.String, arg2: java.util.Map[java.lang.String, java.lang.String], arg3: org.apache.hadoop.hive.metastore.api.PartitionEventType): Boolean = unsupported[Boolean]()
  override def isPartitionMarkedForEvent(arg0: java.lang.String, arg1: java.lang.String, arg2: java.lang.String, arg3: java.util.Map[java.lang.String, java.lang.String], arg4: org.apache.hadoop.hive.metastore.api.PartitionEventType): Boolean = unsupported[Boolean]()
  override def isSameConfObj(arg0: org.apache.hadoop.conf.Configuration): Boolean = unsupported[Boolean]()
  override def listPackages(arg0: org.apache.hadoop.hive.metastore.api.ListPackageRequest): java.util.List[java.lang.String] = unsupported[java.util.List[java.lang.String]]()
  override def listPartitionNames(arg0: java.lang.String, arg1: java.lang.String, arg2: java.lang.String, arg3: java.util.List[java.lang.String], arg4: Int): java.util.List[java.lang.String] = unsupported[java.util.List[java.lang.String]]()
  override def listPartitionNames(arg0: java.lang.String, arg1: java.lang.String, arg2: java.util.List[java.lang.String], arg3: Short): java.util.List[java.lang.String] = unsupported[java.util.List[java.lang.String]]()
  override def listPartitionNames(arg0: org.apache.hadoop.hive.metastore.api.PartitionsByExprRequest): java.util.List[java.lang.String] = unsupported[java.util.List[java.lang.String]]()
  override def listPartitionNames(arg0: java.lang.String, arg1: java.lang.String, arg2: Short): java.util.List[java.lang.String] = unsupported[java.util.List[java.lang.String]]()
  override def listPartitionNames(arg0: java.lang.String, arg1: java.lang.String, arg2: java.lang.String, arg3: Int): java.util.List[java.lang.String] = unsupported[java.util.List[java.lang.String]]()
  override def listPartitionNamesRequest(arg0: org.apache.hadoop.hive.metastore.api.GetPartitionNamesPsRequest): org.apache.hadoop.hive.metastore.api.GetPartitionNamesPsResponse = unsupported[org.apache.hadoop.hive.metastore.api.GetPartitionNamesPsResponse]()
  override def listPartitionSpecs(arg0: java.lang.String, arg1: java.lang.String, arg2: java.lang.String, arg3: Int): org.apache.hadoop.hive.metastore.partition.spec.PartitionSpecProxy = unsupported[org.apache.hadoop.hive.metastore.partition.spec.PartitionSpecProxy]()
  override def listPartitionSpecs(arg0: java.lang.String, arg1: java.lang.String, arg2: Int): org.apache.hadoop.hive.metastore.partition.spec.PartitionSpecProxy = unsupported[org.apache.hadoop.hive.metastore.partition.spec.PartitionSpecProxy]()
  override def listPartitionSpecsByFilter(arg0: java.lang.String, arg1: java.lang.String, arg2: java.lang.String, arg3: java.lang.String, arg4: Int): org.apache.hadoop.hive.metastore.partition.spec.PartitionSpecProxy = unsupported[org.apache.hadoop.hive.metastore.partition.spec.PartitionSpecProxy]()
  override def listPartitionSpecsByFilter(arg0: java.lang.String, arg1: java.lang.String, arg2: java.lang.String, arg3: Int): org.apache.hadoop.hive.metastore.partition.spec.PartitionSpecProxy = unsupported[org.apache.hadoop.hive.metastore.partition.spec.PartitionSpecProxy]()
  override def listPartitionValues(arg0: org.apache.hadoop.hive.metastore.api.PartitionValuesRequest): org.apache.hadoop.hive.metastore.api.PartitionValuesResponse = unsupported[org.apache.hadoop.hive.metastore.api.PartitionValuesResponse]()
  override def listPartitions(arg0: java.lang.String, arg1: java.lang.String, arg2: java.lang.String, arg3: java.util.List[java.lang.String], arg4: Int): java.util.List[org.apache.hadoop.hive.metastore.api.Partition] = unsupported[java.util.List[org.apache.hadoop.hive.metastore.api.Partition]]()
  override def listPartitions(arg0: java.lang.String, arg1: java.lang.String, arg2: java.util.List[java.lang.String], arg3: Short): java.util.List[org.apache.hadoop.hive.metastore.api.Partition] = unsupported[java.util.List[org.apache.hadoop.hive.metastore.api.Partition]]()
  override def listPartitions(arg0: java.lang.String, arg1: java.lang.String, arg2: java.lang.String, arg3: Int): java.util.List[org.apache.hadoop.hive.metastore.api.Partition] = unsupported[java.util.List[org.apache.hadoop.hive.metastore.api.Partition]]()
  override def listPartitionsByExpr(arg0: java.lang.String, arg1: java.lang.String, arg2: Array[Byte], arg3: java.lang.String, arg4: Short, arg5: java.util.List[org.apache.hadoop.hive.metastore.api.Partition]): Boolean = unsupported[Boolean]()
  override def listPartitionsByExpr(arg0: java.lang.String, arg1: java.lang.String, arg2: java.lang.String, arg3: Array[Byte], arg4: java.lang.String, arg5: Int, arg6: java.util.List[org.apache.hadoop.hive.metastore.api.Partition]): Boolean = unsupported[Boolean]()
  override def listPartitionsByFilter(arg0: java.lang.String, arg1: java.lang.String, arg2: java.lang.String, arg3: java.lang.String, arg4: Int): java.util.List[org.apache.hadoop.hive.metastore.api.Partition] = unsupported[java.util.List[org.apache.hadoop.hive.metastore.api.Partition]]()
  override def listPartitionsSpecByExpr(arg0: org.apache.hadoop.hive.metastore.api.PartitionsByExprRequest, arg1: java.util.List[org.apache.hadoop.hive.metastore.api.PartitionSpec]): Boolean = unsupported[Boolean]()
  override def listPartitionsWithAuthInfo(arg0: java.lang.String, arg1: java.lang.String, arg2: Short, arg3: java.lang.String, arg4: java.util.List[java.lang.String]): java.util.List[org.apache.hadoop.hive.metastore.api.Partition] = unsupported[java.util.List[org.apache.hadoop.hive.metastore.api.Partition]]()
  override def listPartitionsWithAuthInfo(arg0: java.lang.String, arg1: java.lang.String, arg2: java.util.List[java.lang.String], arg3: Short, arg4: java.lang.String, arg5: java.util.List[java.lang.String]): java.util.List[org.apache.hadoop.hive.metastore.api.Partition] = unsupported[java.util.List[org.apache.hadoop.hive.metastore.api.Partition]]()
  override def listPartitionsWithAuthInfo(arg0: java.lang.String, arg1: java.lang.String, arg2: java.lang.String, arg3: Int, arg4: java.lang.String, arg5: java.util.List[java.lang.String]): java.util.List[org.apache.hadoop.hive.metastore.api.Partition] = unsupported[java.util.List[org.apache.hadoop.hive.metastore.api.Partition]]()
  override def listPartitionsWithAuthInfo(arg0: java.lang.String, arg1: java.lang.String, arg2: java.lang.String, arg3: java.util.List[java.lang.String], arg4: Int, arg5: java.lang.String, arg6: java.util.List[java.lang.String]): java.util.List[org.apache.hadoop.hive.metastore.api.Partition] = unsupported[java.util.List[org.apache.hadoop.hive.metastore.api.Partition]]()
  override def listPartitionsWithAuthInfoRequest(arg0: org.apache.hadoop.hive.metastore.api.GetPartitionsPsWithAuthRequest): org.apache.hadoop.hive.metastore.api.GetPartitionsPsWithAuthResponse = unsupported[org.apache.hadoop.hive.metastore.api.GetPartitionsPsWithAuthResponse]()
  override def listRoleNames(): java.util.List[java.lang.String] = unsupported[java.util.List[java.lang.String]]()
  override def listTableNamesByFilter(arg0: java.lang.String, arg1: java.lang.String, arg2: Short): java.util.List[java.lang.String] = unsupported[java.util.List[java.lang.String]]()
  override def listTableNamesByFilter(arg0: java.lang.String, arg1: java.lang.String, arg2: java.lang.String, arg3: Int): java.util.List[java.lang.String] = unsupported[java.util.List[java.lang.String]]()
  override def list_privileges(arg0: java.lang.String, arg1: org.apache.hadoop.hive.metastore.api.PrincipalType, arg2: org.apache.hadoop.hive.metastore.api.HiveObjectRef): java.util.List[org.apache.hadoop.hive.metastore.api.HiveObjectPrivilege] = unsupported[java.util.List[org.apache.hadoop.hive.metastore.api.HiveObjectPrivilege]]()
  override def list_roles(arg0: java.lang.String, arg1: org.apache.hadoop.hive.metastore.api.PrincipalType): java.util.List[org.apache.hadoop.hive.metastore.api.Role] = unsupported[java.util.List[org.apache.hadoop.hive.metastore.api.Role]]()
  override def lock(arg0: org.apache.hadoop.hive.metastore.api.LockRequest): org.apache.hadoop.hive.metastore.api.LockResponse = unsupported[org.apache.hadoop.hive.metastore.api.LockResponse]()
  override def lockMaterializationRebuild(arg0: java.lang.String, arg1: java.lang.String, arg2: Long): org.apache.hadoop.hive.metastore.api.LockResponse = unsupported[org.apache.hadoop.hive.metastore.api.LockResponse]()
  override def mapSchemaVersionToSerde(arg0: java.lang.String, arg1: java.lang.String, arg2: java.lang.String, arg3: Int, arg4: java.lang.String): Unit = unsupported[Unit]()
  override def markCleaned(arg0: org.apache.hadoop.hive.metastore.api.CompactionInfoStruct): Unit = unsupported[Unit]()
  override def markCompacted(arg0: org.apache.hadoop.hive.metastore.api.CompactionInfoStruct): Unit = unsupported[Unit]()
  override def markFailed(arg0: org.apache.hadoop.hive.metastore.api.CompactionInfoStruct): Unit = unsupported[Unit]()
  override def markPartitionForEvent(arg0: java.lang.String, arg1: java.lang.String, arg2: java.lang.String, arg3: java.util.Map[java.lang.String, java.lang.String], arg4: org.apache.hadoop.hive.metastore.api.PartitionEventType): Unit = unsupported[Unit]()
  override def markPartitionForEvent(arg0: java.lang.String, arg1: java.lang.String, arg2: java.util.Map[java.lang.String, java.lang.String], arg3: org.apache.hadoop.hive.metastore.api.PartitionEventType): Unit = unsupported[Unit]()
  override def markRefused(arg0: org.apache.hadoop.hive.metastore.api.CompactionInfoStruct): Unit = unsupported[Unit]()
  override def openTxn(arg0: java.lang.String): Long = unsupported[Long]()
  override def openTxn(arg0: java.lang.String, arg1: org.apache.hadoop.hive.metastore.api.TxnType): Long = unsupported[Long]()
  override def openTxns(arg0: java.lang.String, arg1: Int): org.apache.hadoop.hive.metastore.api.OpenTxnsResponse = unsupported[org.apache.hadoop.hive.metastore.api.OpenTxnsResponse]()
  override def partitionNameToSpec(arg0: java.lang.String): java.util.Map[java.lang.String, java.lang.String] = unsupported[java.util.Map[java.lang.String, java.lang.String]]()
  override def partitionNameToVals(arg0: java.lang.String): java.util.List[java.lang.String] = unsupported[java.util.List[java.lang.String]]()
  override def putFileMetadata(arg0: java.util.List[java.lang.Long], arg1: java.util.List[java.nio.ByteBuffer]): Unit = unsupported[Unit]()
  override def reconnect(): Unit = unsupported[Unit]()
  override def recycleDirToCmPath(arg0: org.apache.hadoop.hive.metastore.api.CmRecycleRequest): org.apache.hadoop.hive.metastore.api.CmRecycleResponse = unsupported[org.apache.hadoop.hive.metastore.api.CmRecycleResponse]()
  override def refresh_privileges(arg0: org.apache.hadoop.hive.metastore.api.HiveObjectRef, arg1: java.lang.String, arg2: org.apache.hadoop.hive.metastore.api.PrivilegeBag): Boolean = unsupported[Boolean]()
  override def removeCompactionMetricsData(arg0: org.apache.hadoop.hive.metastore.api.CompactionMetricsDataRequest): Unit = unsupported[Unit]()
  override def removeMasterKey(arg0: java.lang.Integer): Boolean = unsupported[Boolean]()
  override def removeToken(arg0: java.lang.String): Boolean = unsupported[Boolean]()
  override def renamePartition(arg0: java.lang.String, arg1: java.lang.String, arg2: java.lang.String, arg3: java.util.List[java.lang.String], arg4: org.apache.hadoop.hive.metastore.api.Partition, arg5: java.lang.String, arg6: Long, arg7: Boolean): Unit = unsupported[Unit]()
  override def renamePartition(arg0: java.lang.String, arg1: java.lang.String, arg2: java.util.List[java.lang.String], arg3: org.apache.hadoop.hive.metastore.api.Partition): Unit = unsupported[Unit]()
  override def renewDelegationToken(arg0: java.lang.String): Long = unsupported[Long]()
  override def replAllocateTableWriteIdsBatch(arg0: java.lang.String, arg1: java.lang.String, arg2: java.lang.String, arg3: java.util.List[org.apache.hadoop.hive.metastore.api.TxnToWriteId]): java.util.List[org.apache.hadoop.hive.metastore.api.TxnToWriteId] = unsupported[java.util.List[org.apache.hadoop.hive.metastore.api.TxnToWriteId]]()
  override def replOpenTxn(arg0: java.lang.String, arg1: java.util.List[java.lang.Long], arg2: java.lang.String, arg3: org.apache.hadoop.hive.metastore.api.TxnType): java.util.List[java.lang.Long] = unsupported[java.util.List[java.lang.Long]]()
  override def replRollbackTxn(arg0: Long, arg1: java.lang.String, arg2: org.apache.hadoop.hive.metastore.api.TxnType): Unit = unsupported[Unit]()
  override def replTableWriteIdState(arg0: java.lang.String, arg1: java.lang.String, arg2: java.lang.String, arg3: java.util.List[java.lang.String]): Unit = unsupported[Unit]()
  override def revoke_privileges(arg0: org.apache.hadoop.hive.metastore.api.PrivilegeBag, arg1: Boolean): Boolean = unsupported[Boolean]()
  override def revoke_role(arg0: java.lang.String, arg1: java.lang.String, arg2: org.apache.hadoop.hive.metastore.api.PrincipalType, arg3: Boolean): Boolean = unsupported[Boolean]()
  override def rollbackTxn(arg0: Long): Unit = unsupported[Unit]()
  override def rollbackTxn(arg0: org.apache.hadoop.hive.metastore.api.AbortTxnRequest): Unit = unsupported[Unit]()
  override def scheduledQueryMaintenance(arg0: org.apache.hadoop.hive.metastore.api.ScheduledQueryMaintenanceRequest): Unit = unsupported[Unit]()
  override def scheduledQueryPoll(arg0: org.apache.hadoop.hive.metastore.api.ScheduledQueryPollRequest): org.apache.hadoop.hive.metastore.api.ScheduledQueryPollResponse = unsupported[org.apache.hadoop.hive.metastore.api.ScheduledQueryPollResponse]()
  override def scheduledQueryProgress(arg0: org.apache.hadoop.hive.metastore.api.ScheduledQueryProgressInfo): Unit = unsupported[Unit]()
  override def seedTxnId(arg0: Long): Unit = unsupported[Unit]()
  override def seedWriteId(arg0: java.lang.String, arg1: java.lang.String, arg2: Long): Unit = unsupported[Unit]()
  override def setHadoopJobid(arg0: java.lang.String, arg1: Long): Unit = unsupported[Unit]()
  override def setHiveAddedJars(arg0: java.lang.String): Unit = unsupported[Unit]()
  override def setPartitionColumnStatistics(arg0: org.apache.hadoop.hive.metastore.api.SetPartitionsStatsRequest): Boolean = unsupported[Boolean]()
  override def setSchemaVersionState(arg0: java.lang.String, arg1: java.lang.String, arg2: java.lang.String, arg3: Int, arg4: org.apache.hadoop.hive.metastore.api.SchemaVersionState): Unit = unsupported[Unit]()
  override def showCompactions(arg0: org.apache.hadoop.hive.metastore.api.ShowCompactRequest): org.apache.hadoop.hive.metastore.api.ShowCompactResponse = unsupported[org.apache.hadoop.hive.metastore.api.ShowCompactResponse]()
  override def showCompactions(): org.apache.hadoop.hive.metastore.api.ShowCompactResponse = unsupported[org.apache.hadoop.hive.metastore.api.ShowCompactResponse]()
  override def showLocks(): org.apache.hadoop.hive.metastore.api.ShowLocksResponse = unsupported[org.apache.hadoop.hive.metastore.api.ShowLocksResponse]()
  override def showLocks(arg0: org.apache.hadoop.hive.metastore.api.ShowLocksRequest): org.apache.hadoop.hive.metastore.api.ShowLocksResponse = unsupported[org.apache.hadoop.hive.metastore.api.ShowLocksResponse]()
  override def showTxns(): org.apache.hadoop.hive.metastore.api.GetOpenTxnsInfoResponse = unsupported[org.apache.hadoop.hive.metastore.api.GetOpenTxnsInfoResponse]()
  override def submitForCleanup(arg0: org.apache.hadoop.hive.metastore.api.CompactionRequest, arg1: Long, arg2: Long): Boolean = unsupported[Boolean]()
  override def tableExists(arg0: java.lang.String, arg1: java.lang.String, arg2: java.lang.String): Boolean = unsupported[Boolean]()
  override def truncateTable(arg0: java.lang.String, arg1: java.lang.String, arg2: java.lang.String, arg3: java.util.List[java.lang.String]): Unit = unsupported[Unit]()
  override def truncateTable(arg0: java.lang.String, arg1: java.lang.String, arg2: java.util.List[java.lang.String], arg3: java.lang.String, arg4: Long, arg5: Boolean): Unit = unsupported[Unit]()
  override def truncateTable(arg0: org.apache.hadoop.hive.common.TableName, arg1: java.util.List[java.lang.String]): Unit = unsupported[Unit]()
  override def truncateTable(arg0: java.lang.String, arg1: java.lang.String, arg2: java.util.List[java.lang.String]): Unit = unsupported[Unit]()
  override def truncateTable(arg0: java.lang.String, arg1: java.lang.String, arg2: java.util.List[java.lang.String], arg3: java.lang.String, arg4: Long): Unit = unsupported[Unit]()
  override def unlock(arg0: Long): Unit = unsupported[Unit]()
  override def updateCompactionMetricsData(arg0: org.apache.hadoop.hive.metastore.api.CompactionMetricsDataStruct): Boolean = unsupported[Boolean]()
  override def updateCompactorState(arg0: org.apache.hadoop.hive.metastore.api.CompactionInfoStruct, arg1: Long): Unit = unsupported[Unit]()
  override def updateCreationMetadata(arg0: java.lang.String, arg1: java.lang.String, arg2: java.lang.String, arg3: org.apache.hadoop.hive.metastore.api.CreationMetadata): Unit = unsupported[Unit]()
  override def updateCreationMetadata(arg0: java.lang.String, arg1: java.lang.String, arg2: org.apache.hadoop.hive.metastore.api.CreationMetadata): Unit = unsupported[Unit]()
  override def updateMasterKey(arg0: java.lang.Integer, arg1: java.lang.String): Unit = unsupported[Unit]()
  override def updatePartitionColumnStatistics(arg0: org.apache.hadoop.hive.metastore.api.ColumnStatistics): Boolean = unsupported[Boolean]()
  override def updateTableColumnStatistics(arg0: org.apache.hadoop.hive.metastore.api.ColumnStatistics): Boolean = unsupported[Boolean]()
  override def updateTransactionalStatistics(arg0: org.apache.hadoop.hive.metastore.api.UpdateTransactionalStatsRequest): Unit = unsupported[Unit]()
  override def validatePartitionNameCharacters(arg0: java.util.List[java.lang.String]): Unit = unsupported[Unit]()
  override def validateResourcePlan(arg0: java.lang.String, arg1: java.lang.String): org.apache.hadoop.hive.metastore.api.WMValidateResourcePlanResponse = unsupported[org.apache.hadoop.hive.metastore.api.WMValidateResourcePlanResponse]()

  // scalastyle:on

  private def toCatalogTable(table: Table): CatalogTable = {
    val db = table.getDbName
    val tbl = table.getTableName
    val cols = Option(table.getSd).map(_.getCols).map(_.asScala.toList).getOrElse(Nil)
    val partCols = Option(table.getPartitionKeys).map(_.asScala.toList).getOrElse(Nil)

    val dataFields = cols.map(fs => StructField(fs.getName, CatalystSqlParser.parseDataType(fs.getType), nullable = true, Metadata.empty))
    val partitionFields = partCols.map(fs => StructField(fs.getName, CatalystSqlParser.parseDataType(fs.getType), nullable = true, Metadata.empty))

    // Strip "spark.sql.*" properties before handing off to Spark's external catalog.
    // HiveExternalCatalog.alterTable / createTable rejects such keys ("Cannot persist ...
    // table property keys may not start with 'spark.sql.'") because they are reserved for
    // Spark's internal use (provider, schema parts, create version). Spark re-derives and
    // writes these from the CatalogTable itself, so dropping them on the way in is safe.
    //
    // Also strip "EXTERNAL". HMSDDLExecutor.createTable sets both
    // `tableType=EXTERNAL_TABLE` and `parameters[EXTERNAL]=TRUE`. Spark's
    // HiveExternalCatalog.verifyTableProperties rejects "EXTERNAL" as a property key
    // ("Cannot set or change the preserved property key: 'EXTERNAL'") because it controls
    // table type via CatalogTableType instead. The tableType field below already encodes
    // that information, so dropping the property is safe.
    val tableProperties = Option(table.getParameters).map(_.asScala.toMap).getOrElse(Map.empty)
      .filterNot { case (k, _) => k.startsWith("spark.sql.") || k == "EXTERNAL" }

    CatalogTable(
      identifier = TableIdentifier(tbl, Some(db)),
      tableType = if ("EXTERNAL_TABLE".equalsIgnoreCase(table.getTableType)) CatalogTableType.EXTERNAL else CatalogTableType.MANAGED,
      storage = CatalogStorageFormat(
        locationUri = Option(table.getSd).map(_.getLocation).map(new URI(_)),
        inputFormat = Option(table.getSd).map(_.getInputFormat),
        outputFormat = Option(table.getSd).map(_.getOutputFormat),
        serde = Option(table.getSd).flatMap(sd => Option(sd.getSerdeInfo)).map(_.getSerializationLib),
        compressed = false,
        properties = Option(table.getSd).flatMap(sd => Option(sd.getSerdeInfo)).flatMap(si => Option(si.getParameters)).map(_.asScala.toMap).getOrElse(Map.empty)),
      schema = StructType(dataFields ++ partitionFields),
      provider = Some("hudi"),
      partitionColumnNames = partCols.map(_.getName),
      properties = tableProperties)
  }

  private def fromCatalogTable(table: CatalogTable): Table = {
    val t = new Table()
    t.setDbName(table.identifier.database.getOrElse("default"))
    t.setTableName(table.identifier.table)
    t.setTableType(if (table.tableType == CatalogTableType.EXTERNAL) "EXTERNAL_TABLE" else "MANAGED_TABLE")
    t.setParameters(new util.HashMap[String, String](table.properties.asJava))

    val nonPartitionFields = table.schema.fields.filterNot(f => table.partitionColumnNames.contains(f.name))
    val cols = nonPartitionFields.map(f => new FieldSchema(f.name, f.dataType.catalogString, f.getComment().orNull)).toList.asJava
    val partCols = table.partitionColumnNames.map { name =>
      val dt = table.partitionSchema.fields.find(_.name == name).map(_.dataType.catalogString).getOrElse("string")
      new FieldSchema(name, dt, "")
    }.toList.asJava

    val serdeInfo = new SerDeInfo(null, table.storage.serde.orNull, new util.HashMap[String, String](table.storage.properties.asJava))
    val sd = new StorageDescriptor(cols, table.storage.locationUri.map(_.toString).orNull,
      table.storage.inputFormat.orNull, table.storage.outputFormat.orNull, false, 0, serdeInfo, null, null, null)
    t.setSd(sd)
    t.setPartitionKeys(partCols)
    t
  }

  private def fromCatalogPartition(part: CatalogTablePartition, db: String, table: String, partitionKeys: List[String]): Partition = {
    val values = partitionKeys.map(k => part.spec.getOrElse(k, "")).asJava
    val serdeInfo = new SerDeInfo()
    val sd = new StorageDescriptor()
    sd.setLocation(part.storage.locationUri.map(_.toString).orNull)
    sd.setInputFormat(part.storage.inputFormat.orNull)
    sd.setOutputFormat(part.storage.outputFormat.orNull)
    sd.setSerdeInfo(serdeInfo)
    new Partition(values, db, table, 0, 0, sd, new util.HashMap[String, String]())
  }

  private def toCatalogPartition(part: Partition): CatalogTablePartition = {
    val table = getTable(part.getDbName, part.getTableName)
    val keys = Option(table.getPartitionKeys).map(_.asScala.toList).getOrElse(Nil)
    val values = Option(part.getValues).map(_.asScala.toList).getOrElse(Nil)
    val spec = keys.zip(values).map { case (k, v) => (k.getName, v) }.toMap
    CatalogTablePartition(
      spec = spec,
      storage = CatalogStorageFormat(
        locationUri = Option(part.getSd).map(_.getLocation).map(new URI(_)),
        inputFormat = Option(part.getSd).map(_.getInputFormat),
        outputFormat = Option(part.getSd).map(_.getOutputFormat),
        serde = Option(part.getSd).flatMap(sd => Option(sd.getSerdeInfo)).map(_.getSerializationLib),
        compressed = false,
        properties = Option(part.getSd).flatMap(sd => Option(sd.getSerdeInfo)).flatMap(si => Option(si.getParameters)).map(_.asScala.toMap).getOrElse(Map.empty)),
      parameters = Option(part.getParameters).map(_.asScala.toMap).getOrElse(Map.empty))
  }

  private def parsePartitionClause(partName: String): Map[String, String] = {
    partName.split("/").flatMap { token =>
      token.split("=").toList match {
        case k :: v :: Nil =>
          Some(k.trim -> v.trim.stripPrefix("'").stripSuffix("'"))
        case _ => None
      }
    }.toMap
  }
}
