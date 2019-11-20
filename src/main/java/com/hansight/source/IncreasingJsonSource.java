package com.hansight.source;

import com.hansight.util.MiscUtil;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ObjectNode;

/**
 * Copyright: 瀚思安信（北京）软件技术有限公司，保留所有权利。
 *
 * @author yitian_song
 * @created 2019/11/20
 * @description .
 */
public class IncreasingJsonSource extends BaseSourceWithKeyRange<ObjectNode> {

    private static final long serialVersionUID = 2941333602938145526L;

    public IncreasingJsonSource(int numEvents, int numKeys) {
        super(numEvents, numKeys);
    }

    @Override
    protected ObjectNode getElement(int keyId) {
        try {
            return MiscUtil.getObjectNodeFromJson("{\"nta\":\"1\",\"app_protocol\":\"dns\",\"threat_rule_id\":\"2027863\",\"receive_time\":1572379271341,\"collector_source\":\"Hansight-NTA\",\"event_level\":0,\"occur_time\":" + System.currentTimeMillis() + ",\"dst_address\":\"8.8.4.4\",\"threat_feature\":\".P...........smarttender.biz.....\",\"src_port\":51944,\"event_name\":\"网络连接\",\"sensor_id\":\"16279cde-adb9-4e97-9efc-cb4cb756074a\",\"end_time\":1572379244327,\"rule_id\":\"72190fa7-2a8d-457d-8d83-4985ac8c9b48\",\"dst_port\":53,\"threat_info\":\"EXPLOIT.AAC\",\"response\":\"allowed\",\"id\":\"11883905637720065\",\"event_digest\":\"nta_alert\",\"src_mac\":\"7C:1E:06:DF:3E:01\",\"event_type\":\"/14YRL6KY0003\",\"tran_protocol\":17,\"tx_id\":19,\"first_time\":1572379244325,\"in_iface\":\"enp7s0\",\"src_address\":\"172.16.104.58\",\"rule_name\":\"nta_dispatcher\",\"unit\":\"bytes\",\"log_type\":\"Potentially Bad Traffic\",\"flow_id\":195281868879527,\"dev_address\":\"172.16.100.127\",\"original_type\":\"YWxlcnQgZG5zICRIT01FX05FVCBhbnkgLT4gYW55IGFueSAobXNnOiJJTkZPIE9ic2VydmVkIEROUyBRdWVyeSB0byAuYml6IFRMRCI7IGRuc19xdWVyeTsgY29udGVudDoiLmJpeiI7IG5vY2FzZTsgaXNkYXRhYXQ6ITEscmVsYXRpdmU7IG1ldGFkYXRhOiBmb3JtZXJfY2F0ZWdvcnkgSU5GTzsgcmVmZXJlbmNlOnVybCx3d3cuc3BhbWhhdXMub3JnL3N0YXRpc3RpY3MvdGxkcy87IGNsYXNzdHlwZTpiYWQtdW5rbm93bjsgc2lkOjIwMjc4NjM7IHJldjoyOyBtZXRhZGF0YTphZmZlY3RlZF9wcm9kdWN0IEFueSwgYXR0YWNrX3RhcmdldCBDbGllbnRfRW5kcG9pbnQsIGRlcGxveW1lbnQgUGVyaW1ldGVyLCBzaWduYXR1cmVfc2V2ZXJpdHkgTWFqb3IsIGNyZWF0ZWRfYXQgMjAxOV8wOF8xMywgdXBkYXRlZF9hdCAyMDE5XzA5XzI4OykN\",\"vendor\":\"NTA（HanSight）\",\"data_source\":\"NTA（HanSight）\",\"dst_mac\":\"74:85:C4:EA:00:52\",\"original_info\":\"vlABAAABAAAAAAAAC3NtYXJ0dGVuZGVyA2JpegAAAQAB\",\"protocol\":\"alert\",\"in_iface_2\":\"enp7s0\", \"partition\": \"" + keyId + "\"}", "occur_time");
        } catch (Exception e) {
            return null;
        }
    }
}
