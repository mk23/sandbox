import java.util.ArrayList;
import java.util.List;

import org.apache.commons.configuration.PropertiesConfiguration;
import org.apache.commons.configuration.reloading.FileChangedReloadingStrategy;

import org.apache.commons.lang.StringUtils;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import org.apache.hadoop.net.CachedDNSToSwitchMapping;

import static org.apache.hadoop.net.NetworkTopology.DEFAULT_RACK;

public class PropertiesMapping extends CachedDNSToSwitchMapping {
    private static final Log LOG = LogFactory.getLog(PropertiesMapping.class);

    private static final String NET_TOPOLOGY_PROPERTIES_FILE  = "net.topology.properties.file";
    private static final String NET_TOPOLOGY_PROPERTIES_DELAY = "net.topology.properties.delay";

    private PropertiesConfiguration topology;

    public PropertiesMapping() {
        super(null);
    }

    private void loadTopology() {
        if (topology != null) {
            return;
        }

        String file = getConf().get(NET_TOPOLOGY_PROPERTIES_FILE, null);
        if (StringUtils.isBlank(file)) {
            LOG.warn(NET_TOPOLOGY_PROPERTIES_FILE + " not configured.");
            return;
        }

        int delay = getConf().getInt(NET_TOPOLOGY_PROPERTIES_DELAY, 300000); // 5 min

        try {
            FileChangedReloadingStrategy strategy = new FileChangedReloadingStrategy();
            strategy.setRefreshDelay(delay);

            PropertiesConfiguration topology = new PropertiesConfiguration(file);
            topology.setReloadingStrategy(strategy);

            this.topology = topology;
        } catch (Exception e) {
          LOG.warn(file + ": unable to process", e);
      }
    }

    public synchronized List<String> resolve(List<String> names) {
        List<String> results = new ArrayList<String>();

        loadTopology();
        if (topology == null) {
            for (String name : names) {
                results.add(DEFAULT_RACK);
            }
        } else {
            for (String name : names) {
                results.add(topology.getString(name, DEFAULT_RACK));
            }
        }

        return results;
    }

    public void reloadCachedMappings() {
        // Properties automatically reload on changes, so this is unnecessary
    }

    public void reloadCachedMappings(List<String> names) {
        // Properties automatically reload on changes, so this is unnecessary
    }
}
