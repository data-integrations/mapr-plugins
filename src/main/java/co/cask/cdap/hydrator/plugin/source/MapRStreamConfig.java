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
import co.cask.cdap.api.annotation.Macro;
import co.cask.cdap.api.data.schema.Schema;
import co.cask.hydrator.common.ReferencePluginConfig;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Strings;

import java.io.IOException;
import java.io.Serializable;
import javax.annotation.Nullable;

/**
 * Conf for MapR streaming source.
 */
@SuppressWarnings("unused")
public class MapRStreamConfig extends ReferencePluginConfig implements Serializable {

  private static final long serialVersionUID = 8069169417140954175L;

  @Description("List of Kafka brokers specified in host1:port1,host2:port2 form. For example, " +
    "host1.example.com:9092,host2.example.com:9092.")
  @Macro
  private String brokers;

  @Description("Comma separated list of MapR stream topics")
  @Macro
  private String topics;

  @Description("Offset to start reading from, Beginning/Latest. Default value is latest")
  @Macro
  private String offsetField;

  @Description("Output schema of the source")
  private String schema;

  @Description("Optional format of the MapR Stream event. Any format supported by CDAP is supported. " +
    "For example, a value of 'csv' will attempt to MapR Stream payloads as comma-separated values. " +
    "If no format is given, MapR Stream message payloads will be treated as bytes.")
  @Nullable
  private String format;

  public MapRStreamConfig() {
    super("");
  }

  @VisibleForTesting
  public MapRStreamConfig(String referenceName, String brokers, String topics, String schema, String format,
                          String offsetField) {
    super(referenceName);
    this.brokers = brokers;
    this.schema = schema;
    this.format = format;
    this.topics = topics;
    this.offsetField = offsetField;
  }

  public String getOffsetField() {
    return offsetField;
  }

  public String getTopics() {
    return topics;
  }

  public String getBrokers() {
    return brokers;
  }

  @Nullable
  public String getFormat() {
    return Strings.isNullOrEmpty(format) ? null : format;
  }

  public Schema getSchema() {
    try {
      return Strings.isNullOrEmpty(schema) ? null : Schema.parseJson(schema);
    } catch (IOException e) {
      throw new IllegalArgumentException("Unable to parse schema: " + e.getMessage());
    }
  }
}
