package com.github.wlaforest.kes;

import com.jayway.jsonpath.JsonPath;
import io.vertx.core.json.JsonObject;

public class JsonUtils
{
    /**
     * Takes a JSON String and embeds that string in a field called raw at the top level of the object representation
     * and then serilizes it back to a String.
     * @param jsonString String to embed itself as a sting in
     * @return New version of jsonString with itself embedded
     */
    public static final String embedRawData(String jsonString)
    {
        JsonObject obj = new JsonObject(jsonString);
        obj.put("_raw_data", jsonString);
        return obj.toString();
    }

    public static final String jsonPathString(String jsonString, String path)
    {
        return JsonPath.parse(jsonString).read(path, String.class);
    }

    public static final Double jsonPathDouble(String jsonString, String path)
    {
        return JsonPath.parse(jsonString).read(path, Double.class);
    }

}
