package scripts

import com.alibaba.fastjson.JSONObject
import org.apache.flink.api.common.functions.FilterFunction
import org.apache.flink.api.common.serialization.DeserializationSchema
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.java.functions.KeySelector
import org.apache.flink.api.java.typeutils.TypeExtractor
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.datastream.DataStream
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks
import org.apache.flink.streaming.api.functions.source.SourceFunction
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer010
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer010

import java.time.ZonedDateTime
import java.time.format.DateTimeFormatter

class GroovyEnvTest {

    private static long parseTime(String timestamp) {
        ZonedDateTime zdt = ZonedDateTime.parse(timestamp, DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss.SSSz"));
        return zdt.toInstant().toEpochMilli();
    }

    StreamExecutionEnvironment getEnv() {
        // set up the streaming execution environment
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment()

        // use event time
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "172.16.106.67:9092")
        properties.setProperty("group.id", "consumer-group-A1")

        AssignerWithPeriodicWatermarks watermarkAssigner = new BoundedOutOfOrdernessTimestampExtractor<Map>(Time.milliseconds(3000)) {
            @Override
            long extractTimestamp(Map element) {
                String timestamp = ((JSONObject) element).getString("eventTime")
                return parseTime(timestamp)
            }
        }

        SourceFunction source = new FlinkKafkaConsumer010<>("userActionsV3", new DeserializationSchema<Map>() {
            @Override
            Map deserialize(byte[] message) throws IOException {
                String data = new String(message)
                return JSONObject.parseObject(data)
            }

            @Override
            boolean isEndOfStream(Map nextElement) {
                return false
            }

            @Override
            TypeInformation<Map> getProducedType() {
                return TypeExtractor.getForClass(Map.class)
            }
        }, properties).assignTimestampsAndWatermarks(watermarkAssigner).setStartFromEarliest()

        DataStream<Map> stream = env.addSource(source)

        FlinkKafkaProducer010<String> kafkaSink = new FlinkKafkaProducer010<String>(
                "172.16.106.67:9092",
                "result-topic-A1",
                new SimpleStringSchema()
        );
        stream.keyBy(new KeySelector<Map, Object>() {
            @Override
            Object getKey(Map value) throws Exception {
                return value.get("username")
            }
        }).filter(new FilterFunction<Map>() {
            @Override
            boolean filter(Map value) throws Exception {
                return Objects.equals(value.get("action"), "NONE")
            }
        }).map(JSONObject::toJSONString)
                .addSink(kafkaSink)
                .name("kafkaSink");

        return env
    }
}
