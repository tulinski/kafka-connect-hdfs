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

import java.io.IOException;
import java.io.OutputStream;

import org.apache.avro.file.CodecFactory;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.io.DatumWriter;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.errors.DataException;
import org.apache.kafka.connect.sink.SinkRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.rtbhouse.utils.avro.FastGenericDatumWriter;
import com.rtbhouse.utils.avro.events.AvroEventSerdeSupport.AvroSerdeType;

import io.confluent.connect.avro.AvroData;
import io.confluent.connect.hdfs.HdfsSinkConnectorConfig;
import io.confluent.connect.hdfs.storage.HdfsStorage;
import io.confluent.kafka.serializers.NonRecordContainer;

public class AvroRecordWriterProvider
    implements io.confluent.connect.storage.format.RecordWriterProvider<HdfsSinkConnectorConfig> {
  private static final Logger log = LoggerFactory.getLogger(AvroRecordWriterProvider.class);
  private static final String EXTENSION = ".avro";
  private final HdfsStorage storage;
  private final AvroData avroData;
  private final AvroSerdeType avroSerdeType;

  AvroRecordWriterProvider(HdfsStorage storage, AvroData avroData) {
    this(storage, avroData, FAST);
  }

  AvroRecordWriterProvider(HdfsStorage storage, AvroData avroData, AvroSerdeType avroSerdeType) {
    this.storage = storage;
    this.avroData = avroData;
    this.avroSerdeType = avroSerdeType;
  }

  @Override
  public String getExtension() {
    return EXTENSION;
  }

  @Override
  public io.confluent.connect.storage.format.RecordWriter getRecordWriter(
      final HdfsSinkConnectorConfig conf,
      final String filename
  ) {
    return new io.confluent.connect.storage.format.RecordWriter() {
      DataFileWriter<Object> writer = null;
      Schema schema = null;

      @Override
      public void write(SinkRecord record) {
        if (schema == null) {
          schema = record.valueSchema();
          try {
            log.info("Opening record writer for: {}", filename);
            final OutputStream out = storage.create(filename, true);
            org.apache.avro.Schema avroSchema = avroData.fromConnectSchema(schema);
            writer = new DataFileWriter<>(createGenericDatumWriter(avroSchema));
            writer.setCodec(CodecFactory.fromString(conf.getAvroCodec()));
            writer.create(avroSchema, out);
          } catch (IOException e) {
            throw new ConnectException(e);
          }
        }

        log.trace("Sink record: {}", record);
        Object value = avroData.fromConnectData(schema, record.value());
        try {
          // AvroData wraps primitive types so their schema can be included. We need to unwrap
          // NonRecordContainers to just their value to properly handle these types
          if (value instanceof NonRecordContainer) {
            writer.append(((NonRecordContainer) value).getValue());
          } else {
            writer.append(value);
          }
        } catch (IOException e) {
          throw new DataException(e);
        }
      }

      @Override
      public void close() {
        try {
          writer.close();
        } catch (IOException e) {
          throw new DataException(e);
        }
      }

      @Override
      public void commit() {}
    };
  }

  private <D> DatumWriter<D> createGenericDatumWriter(org.apache.avro.Schema avroSchema) {
    switch (this.avroSerdeType) {
      case SLOW:
        return new GenericDatumWriter<>(avroSchema);
      case FAST:
        return new FastGenericDatumWriter<>(avroSchema);
      default:
        throw new IllegalStateException("Unsupported AvroSerdeType: " + this.avroSerdeType);
    }
  }
}
