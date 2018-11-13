package com.sankuai.payrc.mlearn.spark.util;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.parser.ParserConfig;
import com.alibaba.fastjson.util.TypeUtils;

import java.io.BufferedReader;
import java.io.FileReader;
import java.util.HashMap;
import java.util.Map;


public class MapContext<K, V> extends HashMap {
    public MapContext() {
    }

    public MapContext(Map<K, V> map) {
        for(K key: map.keySet()){
            put(key, map.get(key));
        }
    }

    public static MapContext fromObject(Object obj) {
        Map map = cast(obj, Map.class);
        return new MapContext(map);
    }

    public <T> T get(String key, Class<T> clazz){
        if (!containsKey(key)) {
            return null;
        }

        return cast(get(key), clazz);
    }

    public <T> T get(String key, Class<T> clazz, T defaultValue) {
        T value = get(key, clazz);
        if (null == value) {
            value = defaultValue;
        }
        return value;
    }

    public static <T> T cast(Object obj, Class<T> clazz) {
        return TypeUtils.cast(obj, clazz, ParserConfig.getGlobalInstance());
    }

    public static String readJSONFile(String path) throws Exception {
        BufferedReader bufferedReader = new BufferedReader(new FileReader(path));
        StringBuffer stringBuffer = new StringBuffer();
        String line = "";
        while (null != (line = bufferedReader.readLine())){
            stringBuffer.append(line);
        }
        return stringBuffer.toString();
    }

    public static <K, V> MapContext fromString(String str, Class<K> keyClz, Class<V> valClz) {
        Map<K, V> map = JSON.parseObject(str, Map.class);
        return new MapContext<K, V>(map);
    }

    public static <K, V> MapContext loads(String str, Class<K> keyClz, Class<V> valClz) {
        Map<K, V> map = JSON.parseObject(str, Map.class);
        return new MapContext<K, V>(map);
    }
}
