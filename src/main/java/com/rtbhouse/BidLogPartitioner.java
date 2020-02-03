package com.rtbhouse;

import static java.util.stream.Collectors.joining;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;
import java.util.TimeZone;
import java.util.stream.Stream;

import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.sink.SinkRecord;

import io.confluent.connect.hdfs.partitioner.Partitioner;
import io.confluent.connect.storage.errors.PartitionException;
import io.confluent.connect.storage.partitioner.DefaultPartitioner;

public class BidLogPartitioner extends DefaultPartitioner<FieldSchema> implements Partitioner {

    private static final ThreadLocal<SimpleDateFormat> DAY_FORMATTER = ThreadLocal
            .withInitial(() -> {
                SimpleDateFormat dayFormatter = new SimpleDateFormat("yyyyMMdd");
                dayFormatter.setTimeZone(TimeZone.getTimeZone("UTC"));
                return dayFormatter;
            });

    @Override
    public String encodePartition(SinkRecord sinkRecord) {
        Object value = sinkRecord.value();
        if (value instanceof Struct) {
            final Struct struct = (Struct) value;

            long time = (long) struct.get("time");
            String day = DAY_FORMATTER.get().format(new Date(time));
            int sample = (int) struct.get("bucket");

            return Stream.of(
                    "day=" + day,
                    "sample=" + sample
            ).collect(joining(this.delim));
        } else {
            throw new PartitionException("Error encoding partition: value is not Struct type.");
        }
    }

    @Override
    public List<FieldSchema> partitionFields() {
        if (partitionFields == null) {
            partitionFields = newSchemaGenerator(config).newPartitionFields("day,sample");
        }
        return partitionFields;
    }
}
