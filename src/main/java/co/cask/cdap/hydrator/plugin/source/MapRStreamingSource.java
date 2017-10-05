/*
 * Copyright © 2016 Cask Data, Inc.
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

package co.cask.cdap.hydrator.plugin.source;

import co.cask.cdap.api.annotation.Description;
import co.cask.cdap.api.annotation.Name;
import co.cask.cdap.api.annotation.Plugin;
import co.cask.cdap.api.data.format.FormatSpecification;
import co.cask.cdap.api.data.format.RecordFormat;
import co.cask.cdap.api.data.format.StructuredRecord;
import co.cask.cdap.api.data.schema.Schema;
import co.cask.cdap.api.flow.flowlet.StreamEvent;
import co.cask.cdap.etl.api.PipelineConfigurer;
import co.cask.cdap.etl.api.streaming.StreamingContext;
import co.cask.cdap.etl.api.streaming.StreamingSource;
import co.cask.cdap.format.RecordFormats;
import com.google.common.collect.Sets;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.streaming.Time;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairInputDStream;
import org.apache.spark.streaming.kafka.v09.KafkaUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Tuple2;

import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;

/**
 * Kafka Streaming source
 */
@Plugin(type = StreamingSource.PLUGIN_TYPE)
@Name("MapRStream")
@Description("MapR streaming source.")
public class MapRStreamingSource extends ReferenceStreamingSource<StructuredRecord> {
  private static final Logger LOG = LoggerFactory.getLogger(MapRStreamingSource.class);
  private final MapRStreamConfig conf;

  public MapRStreamingSource(MapRStreamConfig conf) {
    super(conf);
    this.conf = conf;
  }

  @Override
  public void configurePipeline(PipelineConfigurer pipelineConfigurer) throws IllegalArgumentException {
    super.configurePipeline(pipelineConfigurer);
    pipelineConfigurer.getStageConfigurer().setOutputSchema(conf.getSchema());
  }

  @Override
  public JavaDStream<StructuredRecord> getStream(StreamingContext context) throws Exception {
    context.registerLineage(conf.referenceName);

    Map<String, String> kafkaParams = new HashMap<>();
    kafkaParams.put("metadata.broker.list", conf.getBrokers());
    kafkaParams.put("bootstrap.servers", conf.getBrokers());
    kafkaParams.put("group.id", "cdap-data-streams" + new Random().nextInt(100000));
    kafkaParams.put("enable.auto.commit", "true");
    kafkaParams.put("auto.commit.interval.ms", "1000");
    kafkaParams.put("key.deserializer", "org.apache.kafka.common.serialization.ByteArrayDeserializer");
    kafkaParams.put("value.deserializer", "org.apache.kafka.common.serialization.ByteArrayDeserializer");

    if (conf.getOffsetField().equalsIgnoreCase("beginning")) {
      kafkaParams.put("auto.offset.re set", "earliest");
    } else {
      kafkaParams.put("auto.offset.reset", "latest");
    }

    String[] topics = conf.getTopics().split(",");

    JavaPairInputDStream<byte[], byte[]> directStream = KafkaUtils.
      createDirectStream(context.getSparkStreamingContext(), byte[].class, byte[].class,
                         kafkaParams, Sets.newHashSet(topics));

    JavaDStream<Tuple2<byte[], byte[]>> tuple2JavaDStream = directStream.toJavaDStream();
    return tuple2JavaDStream.transform(new RecordTransform(conf));
  }

  /**
   * Applies the format function to each rdd.
   */
  private static class RecordTransform
    implements Function2<JavaRDD<Tuple2<byte[], byte[]>>, Time, JavaRDD<StructuredRecord>> {

    private final MapRStreamConfig conf;

    RecordTransform(MapRStreamConfig conf) {
      this.conf = conf;
    }

    @Override
    public JavaRDD<StructuredRecord> call(JavaRDD<Tuple2<byte[], byte[]>> input, Time batchTime) throws Exception {
      Function<Tuple2<byte[], byte[]>, StructuredRecord> recordFunction = conf.getFormat() == null ?
        new StringFunction(conf) : new FormatFunction(conf);
      return input.map(recordFunction);
    }
  }

  /**
   * Common logic for transforming MapR Stream key, message into a structured record.
   * Everything here should be serializable, as Spark Streaming will serialize all functions.
   */
  private abstract static class BaseFunction implements Function<Tuple2<byte[], byte[]>, StructuredRecord> {
    protected final MapRStreamConfig conf;
    private transient String messageField;
    private transient Schema schema;

    BaseFunction(MapRStreamConfig conf) {
      this.conf = conf;
    }

    @Override
    public StructuredRecord call(Tuple2<byte[], byte[]> in) throws Exception {
      // first time this was called, initialize schema and time, key, and message fields.
      if (schema == null) {
        schema = conf.getSchema();
        for (Schema.Field field : schema.getFields()) {
          String name = field.getName();
          if (!name.equals(messageField) && !name.equals(messageField)) {
            messageField = name;
            break;
          }
        }
      }

      StructuredRecord.Builder builder = StructuredRecord.builder(schema);
      addMessage(builder, messageField,in._2());
      return builder.build();
    }

    protected abstract void addMessage(StructuredRecord.Builder builder, String messageField,
                                       byte[] message) throws Exception;
  }

  private static class StringFunction extends BaseFunction {

    StringFunction(MapRStreamConfig conf) {
      super(conf);
    }

    @Override
    protected void addMessage(StructuredRecord.Builder builder, String messageField, byte[] message) {
      builder.set(messageField, message);
    }
  }

  private static class FormatFunction extends BaseFunction {
    private transient RecordFormat<StreamEvent, StructuredRecord> recordFormat;

    FormatFunction(MapRStreamConfig conf) {
      super(conf);
    }

    @Override
    protected void addMessage(StructuredRecord.Builder builder, String messageField, byte[] message) throws Exception {
      // first time this was called, initialize record format
      if (recordFormat == null) {
        Schema messageSchema = conf.getSchema();
        FormatSpecification spec =
          new FormatSpecification(conf.getFormat(), messageSchema, new HashMap<String, String>());
        recordFormat = RecordFormats.createInitializedFormat(spec);
      }

      StructuredRecord messageRecord = recordFormat.read(new StreamEvent(ByteBuffer.wrap(message)));
      for (Schema.Field field : messageRecord.getSchema().getFields()) {
        String fieldName = field.getName();
        builder.set(fieldName, messageRecord.get(fieldName));
      }
    }
  }
}
