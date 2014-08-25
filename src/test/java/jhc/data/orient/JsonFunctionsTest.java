package jhc.data.orient;

import junit.framework.Assert;
import org.junit.Test;
import org.vertx.java.core.json.JsonObject;

/**
 * Created by p14n on 21/08/2014.
 */
public class JsonFunctionsTest {

    @Test
    public void shouldGetNestedObject(){
        JsonObject o = new JsonObject(
                "{\"database\" : {\n" +
                "    \"orient\" : {\n" +
                "      \"createonstart\" : \"true\",\n" +
                "      \"password\" : \"admin\",\n" +
                "      \"url\" : \"plocal:/Users/p14n/database/test\",\n" +
                "      \"username\" : \"admin\"\n" +
                "    }\n" +
                "  }}");
        JsonObject o2 = JsonFunctions.getNestedObject(o,"database.orient");
        Assert.assertTrue(o2.containsField("url"));
    }
}
