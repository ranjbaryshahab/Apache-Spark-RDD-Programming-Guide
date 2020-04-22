package com.shahab;

import scala.Tuple2;

import java.util.*;

public class GroupByAndSort {
    public Map<String, Long> groupBy(List<Tuple2<String, Long>> list) {
        Map<String, Long> hashMap = new HashMap<>();
        for (Tuple2<String, Long> tuple2 : list) {
            if (!hashMap.containsKey(tuple2._1)) {
                hashMap.put(tuple2._1, tuple2._2);
            } else {
                Long aLong = hashMap.get(tuple2._1);
                hashMap.replace(tuple2._1, aLong + 1);
            }
        }
        return hashMap;
    }

    public Map<String, Long> sortByKey(Map<String, Long> map) {
        List<String> keys = new ArrayList<>(map.keySet());
        Collections.sort(keys);
        Map<String, Long> linkedHashMap = new LinkedHashMap<>();
        for (String key : keys) {
            linkedHashMap.put(key, map.get(key));
        }
        return linkedHashMap;
    }
}
