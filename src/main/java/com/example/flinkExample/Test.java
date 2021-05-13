package com.example.flinkExample;

import com.alibaba.fastjson.JSONObject;

/**
 * @author bertram
 * @date 2021/5/10 15:13
 * @desc
 */
public class Test {
    public static final byte[] FAMILIES = "info".getBytes();

    public static void main(String[] args) {

        String data  = "{\n" +
                "\t\"data\": {\n" +
                "\t\t\"sku_id\": {\n" +
                "\t\t\t\"isIndex\": 1,\n" +
                "\t\t\t\"isStore\": 0,\n" +
                "\t\t\t\"isUpdateCache\": 0,\n" +
                "\t\t\t\"type\": \"string\",\n" +
                "\t\t\t\"value\": \"1005044\"\n" +
                "\t\t},\n" +
                "\t\t\"store_code_list\": {\n" +
                "\t\t\t\"isIndex\": 1,\n" +
                "\t\t\t\"isStore\": 0,\n" +
                "\t\t\t\"isUpdateCache\": 0,\n" +
                "\t\t\t\"type\": \"jsonarray\",\n" +
                "\t\t\t\"value\": [\n" +
                "\t\t\t\t\"8002\",\n" +
                "\t\t\t\t\"8004\"\n" +
                "\t\t\t]\n" +
                "\t\t}\n" +
                "\t},\n" +
                "\t\"id\": \"1005044\",\n" +
                "\t\"operation\": \"update\",\n" +
                "\t\"source\": {\n" +
                "\t\t\"batchid\": \"1364924\",\n" +
                "\t\t\"table\": \"adm_sku_goods_stores_da\"\n" +
                "\t}\n" +
                "}";

        JSONObject jsonObject = JSONObject.parseObject(data);

        JSONObject dataJSONObject = jsonObject.getJSONObject("data");
        for (String tagName : dataJSONObject.keySet()) {
            System.out.println(FAMILIES+"||"+tagName);
        }
    }
}
