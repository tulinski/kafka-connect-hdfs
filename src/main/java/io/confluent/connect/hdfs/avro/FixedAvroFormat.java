/*
 * Copyright 2018 Confluent Inc.
 *
 * Licensed under the Confluent Community License (the "License"); you may not use
 * this file except in compliance with the License.  You may obtain a copy of the
 * License at
 *
 * http://www.confluent.io/confluent-community-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OF ANY KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations under the License.
 */

package io.confluent.connect.hdfs.avro;

import static com.rtbhouse.utils.avro.events.AvroEventSerdeSupport.AvroSerdeType.FAST;

import java.util.Optional;

import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.rtbhouse.utils.avro.events.AvroEventSerdeSupport;
import com.rtbhouse.utils.avro.events.AvroEventSerdeSupport.AvroSerdeType;

import io.confluent.connect.avro.AvroData;
import io.confluent.connect.hdfs.HdfsSinkConnectorConfig;
import io.confluent.connect.hdfs.storage.HdfsStorage;
import io.confluent.connect.storage.format.RecordWriterProvider;
import io.confluent.connect.storage.format.SchemaFileReader;
import io.confluent.connect.storage.hive.HiveFactory;

public class FixedAvroFormat
    implements io.confluent.connect.storage.format.Format<HdfsSinkConnectorConfig, Path> {

  private static final Logger logger = LoggerFactory.getLogger(FixedAvroFormat.class);

  private final HdfsStorage storage;
  private final AvroData avroData;
  private final AvroSerdeType avroSerdeType;

  // DO NOT change this signature, it is required for instantiation via reflection
  public FixedAvroFormat(HdfsStorage storage) {
    this.storage = storage;
    this.avroData = new AvroData(storage.conf().avroDataConfig());
    this.avroSerdeType = Optional.ofNullable(storage.conf().originalsStrings().get("avro.serde.type"))
            .map(String::toUpperCase)
            .map(AvroSerdeType::valueOf)
            .orElse(FAST);
    logger.info("AvroSerdeType: {}", this.avroSerdeType);
  }

  @Override
  public RecordWriterProvider<HdfsSinkConnectorConfig> getRecordWriterProvider() {
    return new AvroRecordWriterProvider(storage, avroData, avroSerdeType);
  }

  @Override
  public SchemaFileReader<HdfsSinkConnectorConfig, Path> getSchemaFileReader() {
    return new AvroFileReader(avroData);
  }

  @Override
  public HiveFactory getHiveFactory() {
    return new AvroHiveFactory(avroData);
  }
}
