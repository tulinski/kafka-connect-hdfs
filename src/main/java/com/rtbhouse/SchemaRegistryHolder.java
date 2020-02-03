package com.rtbhouse;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import com.adpilot.utils.properties.PropertiesReader;
import com.rtbhouse.utils.avro.registry.SchemaRegistry;
import com.rtbhouse.utils.avro.registry.SchemasProvider;
import com.rtbhouse.utils.avro.registry.providers.ClasspathLatestSchemaIdsProvider;
import com.rtbhouse.utils.avro.registry.providers.SchemaClasspathProvider;
import com.rtbhouse.utils.avro.registry.providers.SchemaMinioProvider;

//code taken from rtb-kafka-hdfs-writer
final class SchemaRegistryHolder {
    private static final String COM_RTBHOUSE_DOMAIN = "com.rtbhouse.domain";
    private static final String ENABLE_SCHEMA_REGISTRY_PROPERTY = "schema.registry.enable";

    private SchemaRegistryHolder() {
    }

    private static SchemaRegistry registry = null;

    static synchronized SchemaRegistry provide(Properties config) {
        if (registry == null) {
            registry = produce(config);
        }
        return registry;
    }

    private static SchemaRegistry produce(Properties config) {
        List<SchemasProvider> providers = new ArrayList<>();
        //SchemaClasspathProvider class loader conflicts with kafka.connect.PluginClassLoader
//        providers.add(new SchemaClasspathProvider(COM_RTBHOUSE_DOMAIN));

        SchemaRegistry.SchemaRegistryBuilder schemaRegistryBuilder = SchemaRegistry.newBuilder();

        if ("true".equalsIgnoreCase(config.getProperty(ENABLE_SCHEMA_REGISTRY_PROPERTY))) {
            SchemaMinioProvider schemaProvider = new SchemaMinioProvider(
                    PropertiesReader.getPropertiesReader(config));
            providers.add(schemaProvider);
            schemaRegistryBuilder.withLatestSchemasIdsProvider(schemaProvider);
        } else {
            schemaRegistryBuilder
                    .withLatestSchemasIdsProvider(new ClasspathLatestSchemaIdsProvider(COM_RTBHOUSE_DOMAIN));
        }

        schemaRegistryBuilder.withSchemaProviders(providers);

        return schemaRegistryBuilder.build();
    }
}
