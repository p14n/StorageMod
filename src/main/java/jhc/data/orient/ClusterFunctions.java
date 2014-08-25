package jhc.data.orient;

import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Iterator;

/**
 * Created by p14n on 21/08/2014.
 */
public class ClusterFunctions {

    private final static Logger log = LoggerFactory.getLogger(ClusterFunctions.class);

    public static boolean hazelCastClusterExists(){
        return firstHazelCastInstance()!=null;
    }

    public static HazelcastInstance firstHazelCastInstance() {
        try {
            Iterator<HazelcastInstance> instances = Hazelcast.getAllHazelcastInstances().iterator();
            if(instances.hasNext())
                return instances.next();
        } catch (Exception e){
            log.warn("Could not check for HazelCast instance",e);
        }
        return null;
    }

    public static void setSerialiserClassloader(){

        try {
            Class.forName("jhc.bootstrap.DeferringClassLoader").getField("defer").set(null,ClusterFunctions.class.getClassLoader());
        } catch (Exception e) {
            log.warn("Could not set classloader",e);
        }
    }
}
