package com.rtbhouse;

import static com.google.common.base.Preconditions.checkState;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import org.apache.avro.generic.GenericContainer;
import org.apache.avro.generic.IndexedRecord;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.errors.DataException;
import org.apache.kafka.connect.storage.Converter;

import com.google.common.collect.ImmutableMap;
import com.rtbhouse.utils.avro.events.AvroEventSerdeSupport;
import com.rtbhouse.utils.avro.registry.SchemaRegistry;

import io.confluent.connect.avro.AvroData;
import io.confluent.connect.avro.AvroDataConfig;
import io.confluent.kafka.serializers.NonRecordContainer;

public class AvroValueConverter implements Converter {

    private boolean isKey;
    private AvroData avroData;
    private AvroEventSerdeSupport rtbAvroSerde;

    public AvroValueConverter() {
    }

    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {
        checkState(!isKey);
        this.isKey = false;

        avroData = new AvroData(new AvroDataConfig(configs));
        rtbAvroSerde = createAvroSerde(configs);
    }

    private AvroEventSerdeSupport createAvroSerde(Map<String, ?> configs) {

        // hardcoded properties because we don't want to keep credentials in kafka
        Properties properties = new Properties();

        properties.put("schema.registry.enable", "true");
        properties.put("minio.bucket", "core");
        properties.put("minio.schema.registry.path", "dc/ams/schema_registry2/");
        properties.put("minio.access.key", "9U70U4N4QMXVTLEJSJRJ");
        properties.put("minio.secret.key", "ipVBRWZQ7AiM2MLRlJexKUYsChqHMFSOiYPP/ViT");
        properties.put("minio.endpoint", "http://minio.creativecdn.net:9000");

        SchemaRegistry schemaRegistry = SchemaRegistryHolder.provide(properties);
        return new AvroEventSerdeSupport(schemaRegistry);
    }

    @Override
    public byte[] fromConnectData(String topic, Schema schema, Object value) {
        return rtbAvroSerde.serialize((GenericContainer) avroData.fromConnectData(schema, value));
    }

    /**
     * returns SchemaAndValue where schema is connect.Schema converted from avro.Schema
     * and value is connect.Struct probably
     */
    @Override
    public SchemaAndValue toConnectData(String topic, byte[] value) {
        try {
            GenericContainer deserialized = rtbAvroSerde.deserialize(value);
            //TODO: not sure it is a proper usage of connect version
            Integer version = SchemaHelper.getSchemaId(deserialized);

            if (deserialized instanceof IndexedRecord) {
                SchemaAndValue schemaAndValue = avroData.toConnectData(deserialized.getSchema(), deserialized, version);
                return schemaAndValue;
            } else if (deserialized instanceof NonRecordContainer) {
                SchemaAndValue schemaAndValue = avroData.toConnectData(
                        deserialized.getSchema(), ((NonRecordContainer) deserialized).getValue(), version);
                return schemaAndValue;
            }
            throw new DataException(
                    String.format("Unsupported type returned during deserialization of topic %s ", topic)
            );
        } catch (SerializationException e) {
            throw new DataException(
                    String.format("Failed to deserialize data for topic %s to Avro: ", topic),
                    e
            );
        }
    }
}
