/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package cn.edu.tsinghua.iot.benchmark.dolphindb;

import cn.edu.tsinghua.iot.benchmark.conf.Config;
import cn.edu.tsinghua.iot.benchmark.conf.ConfigDescriptor;
import cn.edu.tsinghua.iot.benchmark.entity.Batch.IBatch;
import cn.edu.tsinghua.iot.benchmark.entity.Record;
import cn.edu.tsinghua.iot.benchmark.entity.Sensor;
import cn.edu.tsinghua.iot.benchmark.entity.enums.SensorType;
import cn.edu.tsinghua.iot.benchmark.measurement.Status;
import cn.edu.tsinghua.iot.benchmark.schema.MetaUtil;
import cn.edu.tsinghua.iot.benchmark.schema.schemaImpl.DeviceSchema;
import cn.edu.tsinghua.iot.benchmark.tsdb.DBConfig;
import cn.edu.tsinghua.iot.benchmark.tsdb.IDatabase;
import cn.edu.tsinghua.iot.benchmark.tsdb.TsdbException;
import cn.edu.tsinghua.iot.benchmark.utils.TimeUtils;
import cn.edu.tsinghua.iot.benchmark.workload.query.impl.AggRangeQuery;
import cn.edu.tsinghua.iot.benchmark.workload.query.impl.AggRangeValueQuery;
import cn.edu.tsinghua.iot.benchmark.workload.query.impl.AggValueQuery;
import cn.edu.tsinghua.iot.benchmark.workload.query.impl.GroupByQuery;
import cn.edu.tsinghua.iot.benchmark.workload.query.impl.LatestPointQuery;
import cn.edu.tsinghua.iot.benchmark.workload.query.impl.PreciseQuery;
import cn.edu.tsinghua.iot.benchmark.workload.query.impl.RangeQuery;
import cn.edu.tsinghua.iot.benchmark.workload.query.impl.ValueRangeQuery;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

public class TDengine implements IDatabase {
  private static final Config config = ConfigDescriptor.getInstance().getConfig();
  private static final Logger LOGGER = LoggerFactory.getLogger(TDengine.class);

  private static final String DOLPHINDB_DRIVER = "com.dolphindb.jdbc.Driver";

  private static final String DOLPHINDB_URL = "jdbc:dolphindb://%s:%s?user=%s&password=%s";

  private static final String CREATE_DATABASE = "create database if not exists %s";
  private static final String DROP_DATABASE = "drop database if exists %s";
  private static final String USE_DB = "use %s";
  private static final String SUPER_TABLE_NAME = "device";

  private final String CREATE_STABLE;
  private final String CREATE_TABLE;

  private Connection connection;
  private DBConfig dbConfig;
  private String testDatabaseName;

  public DolphinDB(DBConfig dbConfig) {
    this.dbConfig = dbConfig;
    this.testDatabaseName = dbConfig.getDB_NAME();
    // 创建一个示例表的SQL
    StringBuilder createTable = new StringBuilder("create table if not exists my_table (id int, time timestamp");
    for (int i = 0; i < config.getTAG_NUMBER(); i++) {
      createTable.append(", ").append(config.getTAG_KEY_PREFIX()).append(i).append(" binary(20)");
    }
    createTable.append(")");
    CREATE_TABLE = createTable.toString();
  }

  @Override
  public void init() {
    try {
      Class.forName("com.dolphindb.jdbc.Driver");
      String url = String.format("jdbc:dolphindb://%s:%d?user=%s&password=%s",
              dbConfig.getHOST().get(0),
              dbConfig.getPORT().get(0),
              dbConfig.getUSERNAME(),
              dbConfig.getPASSWORD());
      connection = DriverManager.getConnection(url);
      LOGGER.info("DolphinDB init success.");

      try (Statement stmt = connection.createStatement()) {
        stmt.execute(CREATE_TABLE);  // 执行创建表的SQL
        LOGGER.info("Table created successfully.");
      }
    } catch (SQLException e) {
      LOGGER.error("Failed to initialize DolphinDB connection or execute SQL", e);
    } catch (ClassNotFoundException e) {
      LOGGER.error("DolphinDB JDBC Driver not found", e);
    }
  }

  @Override
  public void close() throws TsdbException {
    if (connection != null) {
      try {
        connection.close();
      } catch (SQLException e) {
        LOGGER.error("Failed to close DolphinDB connection because ", e);
        throw new TsdbAllableException(e);
      }
    }
  }

  @Override
  public Double registerSchema(List<DeviceSchema> schemaList) throws TsdbException {
    long start = System.nanoTime();
    if (config.hasWrite()) {
      if (config.getSENSOR_NUMBER() > 1024) {
        LOGGER.error("DolphinDB does not support more than 1024 columns for one table, now is ", config.getSENSOR_NUMBER());
        throw new TsquadException("DolphinDB does not support more than 1024 columns for one table.");
      }
      try (Statement statement = connection.createStatement()) {
        // 创建数据库，假设数据库名已通过config获取
        String createDatabaseSql = String.format("CREATE DATABASE IF NOT EXISTS `%s`", testDatabaseName);
        statement.execute(createDatabaseName);

        // 遍历schemaList创建表
        for (DeviceSchema deviceSchema : schemaList) {
          StringBuilder createTableSql = new StringBuilder("CREATE TABLE IF NOT EXISTS `" + deviceSchema.getDevice() + "` (");
          for (Sensor sensor : deviceSchema.getSensors()) {
            createTableSql.append(sensor.getName()).append(" ").append(typeMap(sensor.getSensorType())).append(",");
          }
          createTableSql.deleteCharAt(createTableSql.length() - 1); // 删除最后一个逗号
          createTableSql.append(")");
          LOGGER.info("Creating table with SQL: {}", createTableSql.toString());
          statement.execute(createTableSql.toString());
        }
      } catch (SQLException e) {
        LOGGER.error("Register DolphinDB schema failed because ", e);
        throw new TsdbException(e);
      }
    }
    long end = System.nanoTime();
    return TimeUtils.convertToSeconds(end - start, "ns");
  }

  @Override
  public Status insertOneBatch(IBatch batch) {
    try (Statement statement = connection.createStatement()) {
      StringBuilder builder = new StringBuilder("INSERT INTO ");
      builder.append(batch.getDeviceSchema().getDevice()).append(" (");
      // 添加列名
      List<Sensor> sensors = batch.getDeviceSchema().getSensors();
      for (int i = 0; i < sensors.size(); i++) {
        builder.append(sensors.get(i).getName());
        if (i < sensors.size() - 1) builder.append(", ");
      }
      builder.append(") VALUES ");
      // 添加值
      for (Record record : batch.getRecords()) {
        builder.append("(");
        builder.append(record.getTimestamp()); // 假设第一个是时间戳
        for (Object value : record.getRecordDataValue()) {
          builder.append(", ").append(value);
        }
        builder.append("),");
      }
      builder.deleteCharAt(builder.length() - 1); // 删除最后一个逗号
      LOGGER.debug("Executing SQL: {}", builder.toString());
      statement.execute(builder.toString()); // 直接执行
      return new Status(true);
    } catch (Exception e) {
      LOGGER.error("Failed to insert batch into DolphinDB", e);
      return new Status(false, 0, e, e.toString());
    }
  }

  private String getInsertOneRecordSql(DeviceSchema deviceSchema, long timestamp, List<Object> values) {
    StringBuilder builder = new StringBuilder();
    builder.append("(");  // 括号包围数据
    builder.append(timestamp);  // 时间戳
    List<Sensor> sensors = deviceSchema.getSensors();
    for (int i = 0; i < sensors.size(); i++) {
      builder.append(", ");
      Sensor sensor = sensors.get(i);
      Object value = values.get(i);
      switch (typeMap(sensor.getSensorType())) {
        case "BOOL":
        case "INT":
        case "LONG":
        case "FLOAT":
        case "DOUBLE":
          builder.append(value.toString());  // 直接转换为字符串
          break;
        case "STRING":
          builder.append("'").append(value).append("'");
        default:
          throw new IllegalArgumentException("Unsupported data type: " + sensor.getSensorType());
      }
    }
    builder.append(")");
    return builder.toString();
  }

  @Override
  public Status preciseQuery(PreciseQuery preciseQuery) {
    // Format the timestamp for DolphinDB SQL query
    String strTime = String.format("timestamp('%s')", preciseQuery.getTimestamp());
    // Construct SQL query using DolphinDB syntax
    String sql = String.format("SELECT * FROM %s WHERE time = %s", preciseQuery.getDeviceSchema().getDevice(), strTime);
    // Execute the query and get status
    return executeQueryAndGetStatus(sql);
  }

  /**
   * eg. SELECT s_0 FROM group_2 WHERE ( device = 'd_8' ) AND time >= 1535558405000000000 AND time
   * <= 153555800000.
   */
  @Override
  public Status rangeQuery(RangeQuery rangeQuery) {
    String sqlHeader = getSimpleQuerySqlHead(rangeQuery.getDeviceSchema());
    String sql = addWhereTimeClause(sqlHeader, rangeQuery);
    return executeQueryAndGetStatus(sql);
  }

  /**
   * eg. SELECT s_3 FROM group_0 WHERE ( device = 'd_3' ) AND time >= 1535558420000000000 AND time
   * <= 153555800000 AND s_3 > -5.0.
   */
  @Override
  public Status valueRangeQuery(ValueRangeQuery valueRangeQuery) {

    String rangeQueryHead = getSimpleQuerySqlHead(valueRangeQuery.getDeviceSchema());
    String sqlWithTimeFilter = addWhereTimeClause(rangeQueryHead, valueRangeQuery);
    String sqlWithValueFilter = addWhereValueClause(valueRangeQuery.getDeviceSchema(), sqlWithTimeFilter, valueRangeQuery.getValueThreshold());

    return executeQueryAndGetStatus(sqlWithValueFilter);
  }


  /**
  2024.06.18
   */




  /**
   * eg. SELECT count(s_3) FROM group_4 WHERE ( device = 'd_16' ) AND time >= 1535558410000000000
   * AND time <=8660000000000.
   */
  @Override
  public Status aggRangeQuery(AggRangeQuery aggRangeQuery) {
    String aggQuerySqlHead =
            getAggQuerySqlHead(aggRangeQuery.getDeviceSchema(), aggRangeQuery.getAggFun());
    String sql = addWhereTimeClause(aggQuerySqlHead, aggRangeQuery);
    return executeQueryAndGetStatus(sql);
  }

  /** eg. SELECT count(s_3) FROM group_3 WHERE ( device = 'd_12' ) AND s_3 > -5.0. */
  @Override
  public Status aggValueQuery(AggValueQuery aggValueQuery) {
    String aggQuerySqlHead =
        getAggQuerySqlHead(aggValueQuery.getDeviceSchema(), aggValueQuery.getAggFun());
    String sql =
        addWhereValueWithoutTimeClause(
            aggValueQuery.getDeviceSchema(), aggQuerySqlHead, aggValueQuery.getValueThreshold());
    return executeQueryAndGetStatus(sql);
  }

  /**
   * eg. SELECT count(s_1) FROM group_2 WHERE ( device = 'd_8' ) AND time >= 1535558400000000000 AND
   * time <= 650000000000 AND s_1 > -5.0.
   */
  @Override
  public Status aggRangeValueQuery(AggRangeValueQuery aggRangeValueQuery) {
    String rangeQueryHead =
        getAggQuerySqlHead(aggRangeValueQuery.getDeviceSchema(), aggRangeValueQuery.getAggFun());
    String sqlWithTimeFilter = addWhereTimeClause(rangeQueryHead, aggRangeValueQuery);
    String sqlWithValueFilter =
        addWhereValueClause(
            aggRangeValueQuery.getDeviceSchema(),
            sqlWithTimeFilter,
            aggRangeValueQuery.getValueThreshold());
    return executeQueryAndGetStatus(sqlWithValueFilter);
  }

  /**
   * eg. SELECT count(s_3) FROM group_4 WHERE ( device = 'd_16' ) AND time >= 1535558430000000000
   * AND time <=8680000000000 GROUP BY time(20000ms).
   */
  @Override
  public Status groupByQuery(GroupByQuery groupByQuery) {
    String sqlHeader = getAggQuerySqlHead(groupByQuery.getDeviceSchema(), groupByQuery.getAggFun());
    String sqlWithTimeFilter = addWhereTimeClause(sqlHeader, groupByQuery);
    String sqlWithGroupBy = addGroupByClause(sqlWithTimeFilter, groupByQuery.getGranularity());
    return executeQueryAndGetStatus(sqlWithGroupBy);
  }

  /** eg. SELECT last(s_2) FROM group_2 WHERE ( device = 'd_8' ). */
  @Override
  public Status latestPointQuery(LatestPointQuery latestPointQuery) {
    String sql = getAggQuerySqlHead(latestPointQuery.getDeviceSchema(), "last");
    return executeQueryAndGetStatus(sql);
  }

  @Override
  public Status rangeQueryOrderByDesc(RangeQuery rangeQuery) {
    String rangeQueryHead = getSimpleQuerySqlHead(rangeQuery.getDeviceSchema());
    String sql = addWhereTimeClause(rangeQueryHead, rangeQuery) + " order by timestamp desc";
    return executeQueryAndGetStatus(sql);
  }

  @Override
  public Status valueRangeQueryOrderByDesc(ValueRangeQuery valueRangeQuery) {
    String rangeQueryHead = getSimpleQuerySqlHead(valueRangeQuery.getDeviceSchema());
    String sqlWithTimeFilter = addWhereTimeClause(rangeQueryHead, valueRangeQuery);
    String sqlWithValueFilter =
        addWhereValueClause(
                valueRangeQuery.getDeviceSchema(),
                sqlWithTimeFilter,
                valueRangeQuery.getValueThreshold())
            + " order by timestamp desc";
    return executeQueryAndGetStatus(sqlWithValueFilter);
  }

  private String getPreciseQuerySql(PreciseQuery preciseQuery) {
    String strTime = preciseQuery.getTimestamp() + "";
    return getSimpleQuerySqlHead(preciseQuery.getDeviceSchema()) + " Where time = " + strTime;
  }

  /**
   * generate simple query header.
   *
   * @param devices schema list of query devices
   * @return Simple Query header. e.g. SELECT s_0, s_3 FROM root.group_0, root.group_1
   *     WHERE(device='d_0' OR device='d_1')
   */
  private static String getSimpleQuerySqlHead(List<DeviceSchema> devices) {
    StringBuilder builder = new StringBuilder();
    builder.append("SELECT ");
    List<Sensor> querySensors = devices.get(0).getSensors();

    builder.append(querySensors.get(0).getName());
    for (int i = 1; i < querySensors.size(); i++) {
      builder.append(", ").append(querySensors.get(i).getName());
    }

    builder.append(" FROM ").append(devices.get(0).getDevice());
    return builder.toString();
  }

  /**
   * generate from and where clause for specified devices.
   *
   * @param devices schema list of query devices
   * @return from and where clause
   */
  private static String generateConstrainForDevices(List<DeviceSchema> devices) {
    StringBuilder builder = new StringBuilder();
    builder.append(" FROM ").append(devices.get(0).getDevice());
    // builder.append(" WHERE ");
    /*for (DeviceSchema d : devices) {
      builder.append(" device = '").append(d.getDevice()).append("' OR");
    }
    builder.delete(builder.lastIndexOf("OR"), builder.length());
    builder.append(")");
    */
    return builder.toString();
  }

  private Status executeQueryAndGetStatus(String sql) {
    if (!config.isIS_QUIET_MODE()) {
      LOGGER.info("{} query SQL: {}", Thread.currentThread().getName(), sql);
    }
    LOGGER.debug("execute sql {}", sql);
    int line = 0;
    int queryResultPointNum = 0;
    try (Statement statement = connection.createStatement()) {
      statement.execute(String.format(USE_DB, testDatabaseName));
      try (ResultSet resultSet = statement.executeQuery(sql)) {
        while (resultSet.next()) {
          line++;
        }
      }
      queryResultPointNum = line * config.getQUERY_SENSOR_NUM() * config.getQUERY_DEVICE_NUM();
      return new Status(true, queryResultPointNum);
    } catch (Exception e) {
      return new Status(false, queryResultPointNum, e, sql);
    } catch (Throwable t) {
      return new Status(false, queryResultPointNum, new Exception(t), sql);
    }
  }

  /**
   * add time filter for query statements.
   *
   * @param sql sql header
   * @param rangeQuery range query
   * @return sql with time filter
   */
  private static String addWhereTimeClause(String sql, RangeQuery rangeQuery) {
    String startTime = String.format("timestamp('%s')", rangeQuery.getStartTimestamp());
    String endTime = String.format("timestamp('%s')", rangeQuery.getEndTimestamp());
    return sql + " WHERE time >= " + startTime + " AND time <= " + endTime;
  }

  /**
   * add value filter for query statements.
   *
   * @param devices query device schema
   * @param sqlHeader sql header
   * @param valueThreshold lower bound of query value filter
   * @return sql with value filter
   */
  private static String addWhereValueClause(List<DeviceSchema> devices, String sqlHeader, double valueThreshold) {
    StringBuilder builder = new StringBuilder(sqlHeader);
    List<Sensor> sensors = devices.get(0).getSensors();
    for (Sensor sensor : sensors) {
      builder.append(" AND ").append(sensor.getName()).append(" > ").append(valueThreshold);
    }
    return builder.toString();
  }

  /**
   * add value filter without time filter for query statements.
   *
   * @param devices query device schema
   * @param sqlHeader sql header
   * @param valueThreshold lower bound of query value filter
   * @return sql with value filter
   */
  private static String addWhereValueWithoutTimeClause(
      List<DeviceSchema> devices, String sqlHeader, double valueThreshold) {
    StringBuilder builder = new StringBuilder(sqlHeader);
    builder.append(" Where ");
    for (Sensor sensor : devices.get(0).getSensors()) {
      builder.append(sensor.getName()).append(" > ").append(valueThreshold).append(" AND ");
    }
    builder.delete(builder.lastIndexOf("AND"), builder.length());
    return builder.toString();
  }

  /**
   * generate aggregation query header.
   *
   * @param devices schema list of query devices
   * @return Simple Query header. e.g. SELECT count(s_0), count(s_3) FROM root.group_0, root.group_1
   *     WHERE(device='d_0' OR device='d_1')
   */
  private static String getAggQuerySqlHead(List<DeviceSchema> devices, String method) {
    StringBuilder builder = new StringBuilder();
    builder.append("SELECT ");
    List<Sensor> querySensors = devices.get(0).getSensors();

    builder.append(method).append("(").append(querySensors.get(0)).append(")");
    for (int i = 1; i < querySensors.size(); i++) {
      builder
          .append(", ")
          .append(method)
          .append("(")
          .append(querySensors.get(i).getName())
          .append(")");
    }

    builder.append(generateConstrainForDevices(devices));
    return builder.toString();
  }

  /**
   * add group by clause for query.
   *
   * @param sqlHeader sql header
   * @param timeGranularity time granularity of group by
   */
  private static String addGroupByClause(String sqlHeader, long timeGranularity) {
    return sqlHeader + " interval (" + timeGranularity + "a)";
  }

  @Override
  public String typeMap(SensorType sensorType) {
    switch (sensorType) {
      case BOOLEAN:
        return "BOOL";
      case INT32:
        return "INT";
      case INT64:
        return "LONG";
      case FLOAT:
        return "FLOAT";
      case DOUBLE:
        return "DOUBLE";
      case TEXT:
        return "STRING";  // 根据DolphinDB文档, TEXT在DolphinDB中对应为STRING类型
      default:
        LOGGER.error("Unsupported data sensorType {}, using default data type: STRING.", sensorType);
        return "STRING";  // 如果遇到未支持的类型，默认转为STRING
    }
  }
}
