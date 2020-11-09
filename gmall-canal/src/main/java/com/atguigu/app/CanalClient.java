package com.atguigu.app;

import com.alibaba.fastjson.JSONObject;
import com.alibaba.otter.canal.client.CanalConnector;
import com.alibaba.otter.canal.client.CanalConnectors;
import com.alibaba.otter.canal.protocol.CanalEntry;
import com.alibaba.otter.canal.protocol.Message;
import com.atguigu.constants.GmallConstants;
import com.atguigu.utils.MyKafkaSender;
import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;

import java.net.InetSocketAddress;
import java.util.List;

public class CanalClient {

    public static void main(String[] args) {

        //获取连接对象
        CanalConnector canalConnector = CanalConnectors.newSingleConnector(new InetSocketAddress("hadoop102", 11111),
                "example",
                "",
                "");

        while (true) {

            //连接
            canalConnector.connect();
            //消费的数据表
            canalConnector.subscribe("gmall2020.*");
            //抓取数据
            Message message = canalConnector.get(100);

            if (message.getEntries().size() <= 0) {

                System.out.println("当前没有数据，休息一会！！！");

                try {
                    Thread.sleep(5000);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            } else {
                //获取entry
                List<CanalEntry.Entry> entries = message.getEntries();

                for (CanalEntry.Entry entry : entries) {

                    //EntryType
                    CanalEntry.EntryType entryType = entry.getEntryType();

                    if (CanalEntry.EntryType.ROWDATA.equals(entryType)) {
                        //表名
                        String tableName = entry.getHeader().getTableName();
                        //获取序列化数据
                        ByteString storeValue = entry.getStoreValue();
                        //反序列化
                        try {
                            CanalEntry.RowChange rowChange = CanalEntry.RowChange.parseFrom(storeValue);
                            //获取事件类型
                            CanalEntry.EventType eventType = rowChange.getEventType();
                            //获取数据集合
                            List<CanalEntry.RowData> rowDatasList = rowChange.getRowDatasList();

                            handler(tableName, eventType, rowDatasList);
                        } catch (InvalidProtocolBufferException e) {
                            e.printStackTrace();
                        }
                    }
                }
            }
        }
    }

    private static void handler(String tableName, CanalEntry.EventType eventType, List<CanalEntry.RowData> rowDatasList) {

        if ("order_info".equals(tableName) && CanalEntry.EventType.INSERT.equals(eventType)) {

            for (CanalEntry.RowData rowData : rowDatasList) {

                List<CanalEntry.Column> afterColumnsList = rowData.getAfterColumnsList();

                JSONObject jsonObject = new JSONObject();

                for (CanalEntry.Column column : afterColumnsList) {

                    jsonObject.put(column.getName(), column.getValue());
                }

                System.out.println(jsonObject.toString());

                MyKafkaSender.send(GmallConstants.KAFKA_ORDER_INFO, jsonObject.toString());
            }
        }
    }
}

//    public static void main(String[] args) {

//        CanalConnector canalConnector = CanalConnectors.newSingleConnector(new InetSocketAddress("hadoop102", 11111),
//                "example",
//                "",
//                "");
//
//        while (true) {
//
//            canalConnector.connect();
//            canalConnector.subscribe("gmall.*");
//            Message message = canalConnector.get(100);
//            if (message.getEntries().size() <= 0) {
//                System.out.println("当前没数据，等会重新抓取");
//                try {
//                    Thread.sleep(5000);
//                } catch (InterruptedException e) {
//                    e.printStackTrace();
//                }
//            } else {
//                List<CanalEntry.Entry> entries = message.getEntries();
//                for (CanalEntry.Entry entry : entries) {
//                    CanalEntry.EntryType entryType = entry.getEntryType();
//                    if(CanalEntry.EntryType.ROWDATA.equals(entryType)){
//                        String tableName = entry.getHeader().getTableName();
//                        ByteString storeValue = entry.getStoreValue();
//                        CanalEntry
//                    }
//                }
//            }
//        }
//    }
