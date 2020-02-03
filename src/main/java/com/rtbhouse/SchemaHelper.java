package com.rtbhouse;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.avro.Schema;
import org.apache.avro.SchemaNormalization;
import org.apache.avro.generic.GenericContainer;

import com.google.common.base.Charsets;
import com.google.common.hash.HashFunction;
import com.google.common.hash.Hashing;

// code taken from rtb.common-utils (not rtb-common-utils)
public final class SchemaHelper {

    private static final HashFunction HASH_FUNCTION = Hashing.murmur3_128();
    private static final Map<Schema, Integer> SCHEMA_IDS_CACHE = new ConcurrentHashMap<>();;

    private SchemaHelper() {
    }

    public static int getSchemaId(Schema schema) {
        Integer schemaId = SCHEMA_IDS_CACHE.get(schema);
        if (schemaId == null) {
            String schemaString = SchemaNormalization.toParsingForm(schema);
            schemaId = HASH_FUNCTION.hashString(schemaString, Charsets.UTF_8).asInt();
            SCHEMA_IDS_CACHE.put(schema, schemaId);
        }

        return schemaId;
    }

    public static int getSchemaId(GenericContainer record) {
        return getSchemaId(record.getSchema());
    }
}

