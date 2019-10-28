// Kafka source (uid: {uid})
Properties properties_{uid} = new Properties();
properties_{uid}.setProperty("bootstrap.servers", "{bootstrap.servers}");
properties_{uid}.setProperty("group.id", "{group.id}");
DataStream<ObjectNode> source_{uid} = env.addSource(
            new FlinkKafkaConsumer010<>("{topic}", new JSONKeyValueDeserializationSchema(false), properties_{uid})
                .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<ObjectNode>(Time.milliseconds({watermark})) {
                    @Override
                    public long extractTimestamp(ObjectNode element) {
                            JsonNode timestamp = element.findValue("{eventTime.field}");
                            if (timestamp == null) {
                                return System.currentTimeMillis();
                            } else {
                                ZonedDateTime zdt = ZonedDateTime.parse(timestamp.asText(), DateTimeFormatter.ofPattern("{eventTime.format}"));
                                return zdt.toInstant().toEpochMilli();
                            }
                    }
                })
                .setStartFromEarliest()
        )
        .name("{name}")
        .uid("{uid}");