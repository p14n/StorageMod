package jhc.data.orient;

import org.vertx.java.core.json.JsonObject;

/**
 * Created by p14n on 21/08/2014.
 */
public class JsonFunctions {

    public static JsonObject getNestedObject(JsonObject cfg, String name) {
        for (String path : name.split("\\.")) {
            cfg = cfg == null ? null : cfg.getObject(path);
        }
        return cfg;
    }

}
