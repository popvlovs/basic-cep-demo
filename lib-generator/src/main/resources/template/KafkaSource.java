Properties properties_{uid} = new Properties();
{props}
String topic_{uid} = "{topic}";
int watermark_{uid} = {watermark};
DataStream<ObjectNode> stream_{uid} = env.addSource(
            new FlinkKafkaConsumer010<>(topic, new JSONKeyValueDeserializationSchema(false), properties)
                .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<ObjectNode>(Time.milliseconds(watermark_{uid})) {
                    @Override
                    public long extractTimestamp(ObjectNode element) {
                            JsonNode timestamp = element.findValue("eventTime");
                            if (timestamp == null) {
                                return System.currentTimeMillis();
                            } else {
                                ZonedDateTime zdt = ZonedDateTime.parse(timestamp.asText(), DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss.SSSz"));
                                return zdt.toInstant().toEpochMilli();
                            }
                    }
                })
                .setStartFromEarliest()
        ).uid("{uid}");