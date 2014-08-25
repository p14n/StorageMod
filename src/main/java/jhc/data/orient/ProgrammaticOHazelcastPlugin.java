package jhc.data.orient;

import com.hazelcast.config.Config;
import com.hazelcast.core.EntryEvent;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.Member;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.server.OClientConnectionManager;
import com.orientechnologies.orient.server.distributed.ODistributedException;
import com.orientechnologies.orient.server.distributed.ODistributedServerLog;
import com.orientechnologies.orient.server.hazelcast.OHazelcastPlugin;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileNotFoundException;
import java.net.URL;
import java.net.URLClassLoader;

/**
 * Created by p14n on 14/08/2014.
 */
public class ProgrammaticOHazelcastPlugin extends OHazelcastPlugin {

    @Override
    protected HazelcastInstance configureHazelcast() throws FileNotFoundException {
        return ClusterFunctions.firstHazelCastInstance();
    }

}
