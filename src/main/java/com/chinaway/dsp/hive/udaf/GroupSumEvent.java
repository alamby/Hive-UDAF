package com.chinaway.dsp.hive.udaf;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.hadoop.hive.ql.udf.generic.AbstractGenericUDAFResolver;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFEvaluator;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorUtils;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.StringObjectInspector;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.io.Text;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;

public class GroupSumEvent extends AbstractGenericUDAFResolver {
    private static Logger logger = LoggerFactory.getLogger(GroupSumEvent.class);
    @Override
    public GenericUDAFEvaluator getEvaluator(TypeInfo[] parameters) {
        try {
            return new GenericUdafMeberLevelEvaluator();
        }catch (Exception e){
            e.printStackTrace();
            return null;
        }
    }
    public static class GenericUdafMeberLevelEvaluator extends GenericUDAFEvaluator {
        private StringObjectInspector inputOI1;
        private StringObjectInspector outputOI;
        private String result;
        @Override
        public ObjectInspector init(Mode m, ObjectInspector[] parameters) {
            try{
                super.init(m, parameters);
                //init input
                if (m == Mode.PARTIAL1 || m == Mode.COMPLETE){ //必须得有
                    inputOI1 = (StringObjectInspector) parameters[0];
                }
                //init output
                if (m == Mode.PARTIAL2 || m == Mode.FINAL) {
                    outputOI = (StringObjectInspector) parameters[0];
                    result = "";
                    return PrimitiveObjectInspectorFactory.writableStringObjectInspector;
                }else{
                    result = "";
                    return PrimitiveObjectInspectorFactory.writableStringObjectInspector;
                }
            }catch (Exception e){
                e.printStackTrace();
                return  null;
            }
        }
        /** class for storing count value. */
        static class SumAgg implements AggregationBuffer {
            String data;
        }
        @Override
        //创建新的聚合计算的需要的内存，用来存储mapper,combiner,reducer运算过程中的相加总和。
        //使用buffer对象前，先进行内存的清空——reset
        public AggregationBuffer getNewAggregationBuffer() {
            try{
                SumAgg buffer = new SumAgg();
                reset(buffer);
                return buffer;
            }catch (Exception e){
                e.printStackTrace();
                return  null;
            }
        }
        
        @Override
        //重置为0
        //mapreduce支持mapper和reducer的重用，所以为了兼容，也需要做内存的重用。
        public void reset(AggregationBuffer agg){
            try{
                ((SumAgg) agg).data = "";
            }catch (Exception e){
                e.printStackTrace();
            }
        }
        private boolean warned = false;
        //迭代
        //只要把保存当前和的对象agg，再加上输入的参数，就可以了。
        @Override
        public void iterate(AggregationBuffer agg, Object[] parameters) {
            if (parameters == null) {
                return;
            }
            try {
                logger.info("ready to be merged:"+parameters[0]);
                merge(agg, parameters[0]);  //这里将迭代数据放入combiner进行合并
            } catch (NumberFormatException e) {
                if (!warned) {
                    warned = true;
                    e.printStackTrace();
                }
            }
        }
        @Override
        //这里的操作就是具体的聚合操作。
        public void merge(AggregationBuffer agg, Object partial) {
            if (partial != null) {
                // 通过ObejctInspector取每一个字段的数据
                if (inputOI1 != null) {
                    String data = PrimitiveObjectInspectorUtils.getString(partial, inputOI1);
                    String formerData = ((SumAgg) agg).data;
                    logger.info("previous agg:"+formerData+",now data:"+data);
                    //累加data中的事件
                    Map<String, Integer> result = new HashMap<String, Integer>();
                    Map<String, Object> dataMap = toMap(JSON.parseObject(data));
                    Map<String, Object> formerDataMap = new HashMap<>();
                    if (!formerData.isEmpty()) {
                        formerDataMap = toMap(JSON.parseObject(formerData));
                        for (Entry<String, Object> entry : formerDataMap.entrySet()) {
                            result.put(entry.getKey(), (Integer) entry.getValue());     //先把之前聚合的全部放入result
                        }
                    }
                    for (Entry<String, Object> entry : dataMap.entrySet()) {
                        String key = entry.getKey();
                        Integer value = (Integer) entry.getValue();
                        logger.info("key:"+key+",value:"+value);
                        if (!formerDataMap.containsKey(key)) {
                            result.put(key, value);
                        }else {
                            result.put(key, value+(Integer)formerDataMap.get(key));
                        }
                    }
                    String output = JSON.toJSONString(result);
                    logger.info("output:"+output);
                    ((SumAgg) agg).data = output;
                }
            }
        }
        @Override
        public Object terminatePartial(AggregationBuffer agg) {
            return ((SumAgg) agg).data;
        }

        @Override
        public Object terminate(AggregationBuffer agg){
            SumAgg myagg = (SumAgg) agg;
            result = myagg.data;
            logger.info("result:"+result);
            return new Text(result);
        }
    }
    
    /**
     * JSONObject转为map
     * @param object json对象
     * @return 转化后的Map
     */
    public static Map<String, Object> toMap(JSONObject object){
        Map<String, Object> map = new HashMap<String, Object>();

        for (String key : object.keySet()) {
            Object value = object.get(key);
            if(value instanceof JSONArray) {
                value = toList((JSONArray) value);
            }else if(value instanceof JSONObject) {
                value = toMap((JSONObject) value);
            }
            map.put(key, value);
        }

        return map;
    }
    
    /**
     * JSONArray转为List
     * @param array json数组
     * @return 转化后的List
     */
    public static List<Object> toList(JSONArray array){
        List<Object> list = new ArrayList<Object>();
        for(int i = 0; i < array.size(); i++) {
            Object value = array.get(i);
            if(value instanceof JSONArray) {
                value = toList((JSONArray) value);
            }else if(value instanceof JSONObject) {
                value = toMap((JSONObject) value);
            }
            list.add(value);
        }
        return list;
    }
}
