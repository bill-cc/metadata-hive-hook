package org.billcc.lineage.hive.hook.utils;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonParser;
import com.google.gson.reflect.TypeToken;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class JsonMapper {

    private static JsonMapper defaultJsonMapper = null;
    private static JsonMapper nonEmptyJsonMapper = null;
    private static JsonMapper nonDefaultJsonMapper = null;
    private static JsonMapper defaultUnwrapRootJsonMapper = null;

    private Gson gson;
    private JsonParser parser;

    public JsonMapper() {
    	GsonBuilder gsonBuilder = new GsonBuilder();
        //mapper.disable(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES);
        //mapper.configure(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS, false);
        //mapper.configure(JsonParser.Feature.ALLOW_SINGLE_QUOTES, true);
        //mapper.configure(JsonParser.Feature.ALLOW_UNQUOTED_CONTROL_CHARS, true);
        //mapper.configure(JsonParser.Feature.ALLOW_UNQUOTED_FIELD_NAMES, true);
        //mapper.configure(JsonParser.Feature.ALLOW_COMMENTS, true);
        //mapper.setDateFormat(new SimpleDateFormat("yyyy-MM-dd HH:mm:ss"));
    	
        //if (unwrapRoot) {
        //    mapper.configure(DeserializationFeature.UNWRAP_ROOT_VALUE, true);
        //}
    	this.gson = gsonBuilder.disableHtmlEscaping().create();
    	this.parser = new JsonParser();
    }

    /**
     * 创建只输出非Null且非Empty(如List.isEmpty)的属性到Json字符串的Mapper,建议在外部接口中使用.
     */
    public synchronized static JsonMapper nonEmptyMapper() {
        if (nonEmptyJsonMapper == null) {
            nonEmptyJsonMapper = new JsonMapper();
        }
        return nonEmptyJsonMapper;
    }

    /**
     * 创建只输出初始值被改变的属性到Json字符串的Mapper, 最节约的存储方式，建议在内部接口中使用。
     */
    public synchronized static JsonMapper nonDefaultMapper() {
        if (nonDefaultJsonMapper == null) {
            nonDefaultJsonMapper = new JsonMapper();
        }
        return nonDefaultJsonMapper;
    }

    public synchronized static JsonMapper defaultUnwrapRootMapper() {
        if (defaultUnwrapRootJsonMapper == null) {
            defaultUnwrapRootJsonMapper = new JsonMapper();
        }
        return defaultUnwrapRootJsonMapper;
    }

    /**
     * 创建默认Mapper
     */
    public synchronized static JsonMapper defaultMapper() {
        if (defaultJsonMapper == null) {
            defaultJsonMapper = new JsonMapper();
        }
        return defaultJsonMapper;
    }

    /**
     * 对象转换成JSON字符串
     *
     * @param object
     * @return
     */
    public String toJson(Object object) {
        return gson.toJson(object);
    }

    /**
     * JSON转换成Java对象
     *
     * @param json
     * @param clazz
     * @param <T>
     * @return
     */
    public <T> T fromJson(String json, Class<T> clazz) {
        if (json == null || json.trim().length() == 0) {
            return null;
        }
        return gson.fromJson(json, clazz);
    }

    /**
     * JSON转换成Java对象
     *
     * @param json
     * @return
     */
    public Map<String, Object> json2Map(String json) {
    	//fromJson(json, HashMap.class);
    	java.lang.reflect.Type type = 
    			new TypeToken<HashMap<String, Object>>(){}.getType();
    	return gson.fromJson(json, type);
    }

    /**
     * 如果jsons 是数组格式，则挨个转换成clazz对象返回list，否则直接尝试转换成clazz对象返回list
     *
     * @param jsons
     * @param clazz
     * @param <T>
     * @return
     * @throws IOException
     */
    @SuppressWarnings("unchecked")
	public <T> List<T> fromJsons(String jsons, Class<T> clazz) throws IOException {
        if (jsons == null || jsons.trim().length() == 0) {
            return Collections.EMPTY_LIST;
        }

        List<T> list = new ArrayList<>();
        JsonElement jsonNode = parser.parse(jsons);
        if (jsonNode.isJsonArray()) {
        	JsonArray array = jsonNode.getAsJsonArray();
        	for(int i = 0; i < array.size(); i++){
        		list.add(gson.fromJson(array.get(i), clazz));
        	}
        } else {
            list.add(fromJson(jsons, clazz));
        }
        return list;
    }

    public Gson getMapper() {
        return gson;
    }

}
