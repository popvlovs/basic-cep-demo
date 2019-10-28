// Kafka Sink (uid: {uid})
FlinkKafkaProducer010<String> sink_{uid} = new FlinkKafkaProducer010<>(
        "{bootstrap.servers}",
        "{topic}",
        new SimpleStringSchema()
);
sink_{uid}.setWriteTimestampToKafka(true);