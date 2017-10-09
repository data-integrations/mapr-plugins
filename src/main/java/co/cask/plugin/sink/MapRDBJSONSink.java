/*
 * Copyright © 2017 Cask Data, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package co.cask.plugin.sink;

import co.cask.cdap.api.annotation.Description;
import co.cask.cdap.api.annotation.Macro;
import co.cask.cdap.api.annotation.Name;
import co.cask.cdap.api.annotation.Plugin;
import co.cask.cdap.api.data.batch.Output;
import co.cask.cdap.api.data.batch.OutputFormatProvider;
import co.cask.cdap.api.data.format.StructuredRecord;
import co.cask.cdap.api.data.schema.Schema;
import co.cask.cdap.api.dataset.lib.KeyValue;
import co.cask.cdap.etl.api.Emitter;
import co.cask.cdap.etl.api.batch.BatchRuntimeContext;
import co.cask.cdap.etl.api.batch.BatchSink;
import co.cask.cdap.etl.api.batch.BatchSinkContext;
import co.cask.cdap.format.StructuredRecordStringConverter;
import co.cask.hydrator.common.ReferenceBatchSink;
import co.cask.hydrator.common.ReferencePluginConfig;
import co.cask.hydrator.common.batch.JobUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.mapreduce.Job;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.lang.reflect.Method;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Sink for writing to MapR-DB Json tables.
 */
@Plugin(type = BatchSink.PLUGIN_TYPE)
@Name("MapRDBJSON")
@Description("MapR-DB JSON Sink")
public class MapRDBJSONSink  extends ReferenceBatchSink<StructuredRecord, Object, Object> {
  private static final Logger LOG = LoggerFactory.getLogger(MapRDBJSONSink.class);
  private MapRDBJSONSinkConfig config;
  private Schema outputSchema;
  public static final String MAPR_FS_IMPLEMENTATION_KEY = "fs.maprfs.impl";

  public MapRDBJSONSink(MapRDBJSONSinkConfig config) {
    super(config);
    this.config = config;
  }

  @Override
  public void prepareRun(BatchSinkContext context) throws Exception {
    Job job;
    String fsDefaultURI = null;
    String maprfsImplValue = null;
    ClassLoader oldClassLoader = Thread.currentThread().getContextClassLoader();
    Thread.currentThread().setContextClassLoader(getClass().getClassLoader());
    try {
      job = JobUtils.createInstance();
      Configuration configuration = job.getConfiguration();
      fsDefaultURI = configuration.get(FileSystem.FS_DEFAULT_NAME_KEY);
      maprfsImplValue = configuration.get(MAPR_FS_IMPLEMENTATION_KEY);
    } finally {
      // Switch back to the original
      Thread.currentThread().setContextClassLoader(oldClassLoader);
    }

    Configuration conf = job.getConfiguration();
    conf.set(FileSystem.FS_DEFAULT_NAME_KEY, fsDefaultURI);
    conf.set(MAPR_FS_IMPLEMENTATION_KEY, maprfsImplValue);
    context.addOutput(Output.of(config.referenceName, new MapRDBOutputFormatProvider(config, conf)));
  }

  private class MapRDBOutputFormatProvider implements OutputFormatProvider {

    private final Map<String, String> conf;

    MapRDBOutputFormatProvider(MapRDBJSONSinkConfig mapRDBSinkConfig, Configuration hConf) {
      this.conf = new HashMap<>();
      conf.put("maprdb.mapred.outputtable", mapRDBSinkConfig.tableName);
      conf.put(FileSystem.FS_DEFAULT_NAME_KEY, hConf.get(FileSystem.FS_DEFAULT_NAME_KEY));
      conf.put(MAPR_FS_IMPLEMENTATION_KEY, hConf.get(MAPR_FS_IMPLEMENTATION_KEY));
    }

    @Override
    public String getOutputFormatClassName() {
      return "com.mapr.db.mapreduce.TableOutputFormat";
    }

    @Override
    public Map<String, String> getOutputFormatConfiguration() {
      return conf;
    }
  }

  @Override
  public void initialize(BatchRuntimeContext context) throws Exception {
    super.initialize(context);
    this.outputSchema = config.getSchema();
  }

  @Override
  public void transform(StructuredRecord input, Emitter<KeyValue<Object, Object>> emitter) throws Exception {
    // Hack to get access to Mapr classes on the cluster
    ClassLoader platformClassLoader = StructuredRecord.class.getClassLoader();
    Class<?> maprDBClass = platformClassLoader.loadClass("com.mapr.db.MapRDB");
    Object document = maprDBClass.getMethod("newDocument").invoke(null);

    Class<?> idFieldType = null;

    List<Schema.Field> fields = outputSchema.getFields();
    for (Schema.Field field : fields) {
      Object val = input.get(field.getName());
      Schema schema = field.getSchema();
      if (val != null) {
        if (config.key.equals(field.getName())) {
          idFieldType = setJSONField(document, field.getName(), schema, val, true);
        } else {
          setJSONField(document, field.getName(), schema, val, false);
        }
      }
    }

    Class<?> jsonValueBuilderClass = platformClassLoader.loadClass("org.ojai.json.impl.JsonValueBuilder");
    Object key = jsonValueBuilderClass.getMethod("initFrom", idFieldType).invoke(null, input.get(config.key));

    Method setIdMethod = document.getClass().getMethod("setId", platformClassLoader.loadClass("org.ojai.Value"));
    setIdMethod.invoke(document, key);

    emitter.emit(new KeyValue<>(key, document));
  }

  private Class<?> setJSONField(Object document, String fieldName, Schema schema, Object val, boolean isIdField)
    throws Exception {
    Class<?> valueType = String.class;
    switch (schema.getType()) {
      case BOOLEAN:
        valueType = boolean.class;
        break;
      case INT:
        valueType = int.class;
        break;
      case LONG:
        valueType = long.class;
        break;
      case FLOAT:
        valueType = float.class;
        break;
      case DOUBLE:
        valueType = double.class;
        break;
      case BYTES:
        if (val instanceof ByteBuffer) {
          valueType = ByteBuffer.class;
        } else {
          valueType = byte[].class;
        }
        break;
      case STRING:
        valueType = String.class;
        break;
      case UNION: // Recursively drill down to find the type.
          setJSONField(document, fieldName, schema.getNonNullable(), val, isIdField);
        break;
      default:
        throw new IllegalArgumentException(
          "Field '" + fieldName + "' is of unsupported type '" + schema.getType() + "'."
        );
    }

    if (!isIdField) {
      Method setMethod = document.getClass().getMethod("set", String.class, valueType);
      LOG.debug("Got the method {}, value class {}", setMethod.toString(), val.getClass());
      setMethod.invoke(document, fieldName, val);
    }

    return valueType;
  }

  /**
   * Configurations for the plugin.
   */
  public static class MapRDBJSONSinkConfig extends ReferencePluginConfig {

    @Description("Path to the MapR-DB JSON table. Table must exist in the MapR-DB.")
    @Macro
    public String tableName;

    @Description("Field in the record to be used as a id for the JSON document.")
    @Macro
    public String key;

    @Description("Output schema for the MapR-DB JSON table.")
    @Macro
    public String schema;

    public MapRDBJSONSinkConfig(String referenceName) {
      super(referenceName);
    }

    public Schema getSchema() {
      try {
        return Schema.parseJson(schema);
      } catch (IOException e) {
        throw new IllegalArgumentException("Unable to parse output schema.");
      }
    }
  }
}

