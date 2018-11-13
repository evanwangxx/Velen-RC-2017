package com.sankuai.payrc.mlearn.spark.util;

import com.alibaba.fastjson.JSON;
import com.googlecode.aviator.AviatorEvaluator;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class AviatorHelper {
    public static String execute(String expr, String json) {
        Map<String, Object> context = JSON.parseObject(json, Map.class);

        return JSON.toJSONString(AviatorEvaluator.execute(expr, context));
    }

    public static Object getValue(Object value, Integer resultType) {
        if (value == null || resultType == 0) {
            return null;
        }

        String valueStr = value.toString().trim().toLowerCase();
        if (resultType == 1) {
            return "true".equals(valueStr) ? true : false;
        } else if (resultType == 2) {
            return Integer.parseInt(valueStr);
        } else if (resultType == 3) {
            return Long.parseLong(valueStr);
        } else if (resultType == 4) {
            return Float.parseFloat(valueStr);
        } else if (resultType == 5) {
            return Double.parseDouble(valueStr);
        } else {
            return value.toString();
        }
    }

    public static String batchExecute(String feats, String json) {

        Map<String, Object> context = JSON.parseObject(json, Map.class);
        Map<String, Object> result = new HashMap<>();

        for(Object obj : JSON.parseObject(feats, List.class)) {
            MapContext<String, Object> feat = MapContext.fromObject(obj);

            String name = feat.get("name", String.class);
            Integer featType = feat.get("featType", Integer.class);

            Object r = null;
            if (featType == 1) {
                Integer resultType = feat.get("resultType", Integer.class);
                try {
                    r = getValue(context.get(name), resultType);
                } catch (Exception e) {

                }
            } else if (featType == 2) {
                String expr = feat.get("expression", String.class);
                try {
                    r = AviatorEvaluator.execute(expr, context);
                } catch (Exception e) {

                }
            }

            context.put(name, r);
            result.put(name, r);
        }

        return JSON.toJSONString(result);
    }
}
