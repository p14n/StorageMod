package jhc.data.orient;

import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.sql.query.OSQLSynchQuery;
import com.orientechnologies.orient.server.distributed.ODistributedServerManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.vertx.java.core.Handler;
import org.vertx.java.core.eventbus.Message;
import org.vertx.java.core.json.JsonObject;
import org.vertx.java.platform.Verticle;

import java.io.File;
import java.util.List;

/**
 * Created by p14n on 21/08/2014.
 */
public class OrientDBVerticle extends Verticle {

    public static void main(String args[]) {
        new OrientDBVerticle().start();
    }

    private final static Logger log = LoggerFactory.getLogger(OrientDBVerticle.class);

    OrientDBManager db;

    @Override
    public void stop() {
        super.stop();
        if (db != null) db.stop();
    }

    String username, password, url;

    public void start() {

        ClusterFunctions.setSerialiserClassloader();

        JsonObject cfg = getConfig();
        if (!cfg.containsField("url")) throw new IllegalArgumentException("database.orient.url must be specified");
        if (!cfg.containsField("datadirectory"))
            throw new IllegalArgumentException("database.orient.datadirectory must be specified");

        String nodeName = cfg.getString("nodename");
        String createOnStart = cfg.getString("createonstart");
        String dataDirectory = cfg.getString("datadirectory");
        url = "plocal:" + dataDirectory + cfg.getString("url");
        username = cfg.getString("username");
        password = cfg.getString("password");

        //Need to check for existence of database before startup and create the db beforehand
        //use db directory and db name
        //if not exists, start without storage or clustering, create, shutdown
        //Then start with storage and clustering

        boolean primaryNode = "true".equalsIgnoreCase(createOnStart);

        if (primaryNode)
            createDbIfNeeded(dataDirectory, nodeName);

        db = new OrientDBManager();
        boolean cluster = ClusterFunctions.hazelCastClusterExists();

        if (primaryNode) {
            db.activate(dataDirectory, nodeName, cluster, username, password, url);
        } else {
            db.activate(dataDirectory, nodeName, cluster);
        }

        try {
            vertx.eventBus().registerHandler("storagewritehandler", new Handler<Message<JsonObject>>() {
                @Override
                public void handle(Message<JsonObject> event) {
                    String json = event.body().encode();
                    if (log.isDebugEnabled())
                        log.debug("Writer received " + json);
                    write(event.body().getString("type"), json);
                }
            });
            vertx.eventBus().registerHandler("storagereadhandler", new Handler<Message<String>>() {
                @Override
                public void handle(Message<String> event) {
                    if (log.isDebugEnabled())
                        log.debug("Reader received " + event.body());
                    String json = read(event.body());
                    event.reply(new JsonObject(json));
                }
            });

        } catch (Exception e) {
            log.error("Could not register handler", e);
        }


    }

    private JsonObject getConfig() {
        log.info("Vert got config " + getContainer().config().encodePrettily());
        return JsonFunctions.getNestedObject(getContainer().config(), "database.orient");
    }

    private JsonObject getConfigDemo() {
        JsonObject jo = new JsonObject();
        jo.putString("url", "dean");
        jo.putString("createonstart", "true");
        jo.putString("datadirectory", "/Users/p14n/database/");
        jo.putString("username", "admin");
        jo.putString("password", "admin");
        return jo;
    }

    private void createDbIfNeeded(String directory, String nodeName) {
        if (!new File(url).exists()) {
            OrientDBManager tempDb = new OrientDBManager();
            tempDb.activate(directory, nodeName, false);
            tempDb.openOrCreate(url, username, password);
            tempDb.stop();
        }
    }

    private String read(String sql) {
        ODatabaseDocumentTx t = new ODatabaseDocumentTx(url).open(username, password);
        List<ODocument> rs = t.query(new OSQLSynchQuery(sql));
        if (rs != null && !rs.isEmpty())
            return rs.get(0).toJSON();
        return null;
    }


    private void write(String type, String json) {
        ODatabaseDocumentTx t = new ODatabaseDocumentTx(url).open(username, password);
        ODocument d = t.newInstance(type);
        d.fromJSON(json);
        d.save();
        t.commit();
        t.close();
    }

}
