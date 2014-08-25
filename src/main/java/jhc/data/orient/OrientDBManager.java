package jhc.data.orient;

import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import com.orientechnologies.orient.server.OServer;
import com.orientechnologies.orient.server.OServerMain;
import com.orientechnologies.orient.server.config.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;

/**
 * Created by p14n on 14/08/2014.
 */
public class OrientDBManager {


    private final static Logger log = LoggerFactory.getLogger(OrientDBManager.class);

    private OServer server;

    public void activate(String directory, String name, boolean cluster) {
        activate(directory, name, cluster, null, null);
    }

    public void activate(String directory, String name, boolean cluster, String username, String password, String... storageUrl) {
        OServerConfiguration config = createServerConfiguration(directory, name, cluster);
        log.info("Created database config");
        try {
            server = OServerMain.create();
            server.startup(config);
            server.activate();
        } catch (Exception e) {
            log.error("Unable to activate database", e);
        }
    }

    public void openOrCreate(String url, String username, String password) {
        try {
            ODatabaseDocumentTx db = new ODatabaseDocumentTx(url);
            if (db.exists()) {
                log.info("Opening database " + url);
                db.open(username, password);
            } else {
                log.info("Creating database " + url);
                db.create();
            }
            db.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public void open(String url, String username, String password) {
        try {
            ODatabaseDocumentTx db = new ODatabaseDocumentTx(url);
            if (db.exists()) {
                log.info("Opening database " + url);
                db.open(username, password);
            } else {
                log.info("Database doesn't exist" + url);
            }
            db.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public void stop() {
        if (server != null)
            server.shutdown();
    }

    private OServerConfiguration createServerConfiguration(String directory, String name, boolean cluster) {
        return createServerConfiguration(directory, name, cluster, null, null, null);
    }

    private OServerConfiguration createServerConfiguration(String directory, String name, boolean cluster, String username, String password, String... storageUrl) {
        OServerConfiguration config = new OServerConfiguration();

        config.properties = new OServerEntryConfiguration[1];
        config.properties[0] = new OServerEntryConfiguration("server.database.path", directory);


        config.handlers = new LinkedList<>();

        if (cluster) {
            config.handlers.add(createClusterConfiguration(name));
            log.info("Created database cluster config");
        }

        config.network = new OServerNetworkConfiguration();
        if (storageUrl != null) {
            config.storages = new OServerStorageConfiguration[storageUrl.length];
            for (int i = 0; i < storageUrl.length; i++) {
                config.storages[i] = new OServerStorageConfiguration();
                config.storages[i].name = storageUrl[i];
                config.storages[i].userName = username;
                config.storages[i].userPassword = password;
            }
        } else config.storages = new OServerStorageConfiguration[0];

        config.network.protocols = new ArrayList<>();
        config.network.listeners = new ArrayList<>();
        config.users = new OServerUserConfiguration[0];
        return config;
    }

    private OServerHandlerConfiguration createClusterConfiguration(String name) {
        OServerHandlerConfiguration oServerHandlerConfiguration = new OServerHandlerConfiguration();
        oServerHandlerConfiguration.clazz = ProgrammaticOHazelcastPlugin.class.getName();

        List<OServerParameterConfiguration> params = new ArrayList<>();
        if (name != null) params.add(new OServerParameterConfiguration("nodeName", name));

        params.add(new OServerParameterConfiguration("enabled", "true"));
        params.add(new OServerParameterConfiguration("configuration.db.default", "default-distributed-db-config.json"));
        params.add(new OServerParameterConfiguration("conflict.resolver.impl", "com.orientechnologies.orient.server.distributed.conflict.ODefaultReplicationConflictResolver"));
        params.add(new OServerParameterConfiguration("sharding.strategy.round-robin", "com.orientechnologies.orient.server.hazelcast.sharding.strategy.ORoundRobinPartitioninStrategy"));

        oServerHandlerConfiguration.parameters = params.toArray(new OServerParameterConfiguration[params.size()]);
        return oServerHandlerConfiguration;
    }

}
