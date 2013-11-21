import java.lang.Runnable;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import javax.management.MBeanServerConnection;
import javax.management.NotCompliantMBeanException;
import javax.management.ObjectName;
import javax.management.StandardMBean;
import javax.management.openmbean.CompositeData;
import javax.management.openmbean.TabularDataSupport;
import javax.management.remote.JMXConnector;
import javax.management.remote.JMXConnectorFactory;
import javax.management.remote.JMXServiceURL;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.metrics.util.MBeanUtil;
import org.apache.hadoop.net.DNSToSwitchMapping;

import org.mortbay.util.ajax.JSON;

import static org.apache.hadoop.net.NetworkTopology.DEFAULT_RACK;
import static org.apache.hadoop.net.NodeBase.PATH_SEPARATOR_STR;
import static org.apache.hadoop.net.NodeBase.ROOT;

public class JMXMapping extends Configured implements DNSToSwitchMapping, JMXMappingMBean {
    private static final Log LOG = LogFactory.getLog(JMXMapping.class);

    private static final String NET_TOPOLOGY_JMX_ENDPOINT_PORT  = "net.topology.jmx.endpoint.port";
    private static final String NET_TOPOLOGY_JMX_PROPERTY_NAME  = "net.topology.jmx.property.name";
    private static final String NET_TOPOLOGY_JMX_CACHE_DURATION = "net.topology.jmx.cache.duration";
    private static final String NET_TOPOLOGY_JMX_DEFAULT_PREFIX = "net.topology.jmx.default.prefix";

    private static final String NET_TOPOLOGY_COMPONENT_PREFIX =
        System.getProperty("proc_namenode") != null ? "hdfs." :
        System.getProperty("proc_jobtracker") != null ? "mapred." :
        System.getProperty("proc_jobtrackerha") != null ? "mapred." :
        "";

    private static final String NET_TOPOLOGY_COMPONENT_MBEAN =
        System.getProperty("proc_namenode") != null ? "NameNode" :
        System.getProperty("proc_jobtracker") != null ? "JobTracker" :
        System.getProperty("proc_jobtrackerha") != null ? "JobTracker" :
        "General";

    private String endpointPort = "-1";
    private String propertyName = "switch.name";
    private String defaultRack  = ROOT + DEFAULT_RACK;
    private int cacheDuration   = 1440; // 24 hours
    private int defaultDepth    = 0;

    private Map<String, String> cacheMap;
    private ScheduledExecutorService executor;

    private ObjectName mbeanName;

    public JMXMapping() {
        this(new Configuration());
    }

    public JMXMapping(Configuration conf) {
        super(conf);

        this.processConfig(conf);
        this.setupCacheMap();
        this.registerMBean();
    }

    private void processConfig(Configuration conf) {
        String endpointPort = conf.get(this.NET_TOPOLOGY_COMPONENT_PREFIX + this.NET_TOPOLOGY_JMX_ENDPOINT_PORT, conf.get(this.NET_TOPOLOGY_JMX_ENDPOINT_PORT, null));
        if (StringUtils.isBlank(endpointPort)) {
            LOG.warn(this.NET_TOPOLOGY_JMX_ENDPOINT_PORT + " not configured, using: " + this.endpointPort);
        } else {
            this.endpointPort = endpointPort;
            LOG.info("endpoint port: " + this.endpointPort);
        }

        String propertyName = conf.get(this.NET_TOPOLOGY_COMPONENT_PREFIX + this.NET_TOPOLOGY_JMX_PROPERTY_NAME, conf.get(this.NET_TOPOLOGY_JMX_PROPERTY_NAME, null));
        if (StringUtils.isBlank(propertyName)) {
            LOG.warn(this.NET_TOPOLOGY_JMX_PROPERTY_NAME + " not configured, using: " + this.propertyName);
        } else {
            this.propertyName = propertyName;
            LOG.info("property name: " + this.propertyName);
        }

        String cacheDuration = conf.get(this.NET_TOPOLOGY_COMPONENT_PREFIX + this.NET_TOPOLOGY_JMX_CACHE_DURATION, conf.get(this.NET_TOPOLOGY_JMX_CACHE_DURATION, null));
        if (StringUtils.isBlank(cacheDuration) || !StringUtils.isNumeric(cacheDuration)) {
            LOG.warn(this.NET_TOPOLOGY_JMX_CACHE_DURATION + " not configured, using: " + this.cacheDuration);
        } else {
            this.cacheDuration = Integer.parseInt(cacheDuration);
            LOG.info("cache duration: " + this.cacheDuration);
        }

        String defaultPrefix = conf.get(this.NET_TOPOLOGY_COMPONENT_PREFIX + this.NET_TOPOLOGY_JMX_DEFAULT_PREFIX, conf.get(this.NET_TOPOLOGY_JMX_DEFAULT_PREFIX, null));
        if (StringUtils.isBlank(defaultPrefix)) {
            LOG.warn(this.NET_TOPOLOGY_JMX_DEFAULT_PREFIX + " not configured, using: " + this.defaultRack);
        } else {
            if (!StringUtils.startsWith(defaultPrefix, PATH_SEPARATOR_STR)) {
                LOG.warn(this.NET_TOPOLOGY_JMX_DEFAULT_PREFIX + " value is not absolute, prepending " + PATH_SEPARATOR_STR);
                defaultPrefix = PATH_SEPARATOR_STR + defaultPrefix;
            }
            this.defaultRack = defaultPrefix + DEFAULT_RACK;
            LOG.info("default rack: " + this.defaultRack);
        }

        defaultDepth = StringUtils.countMatches(this.defaultRack, PATH_SEPARATOR_STR);
    }

    private void setupCacheMap() {
        cacheMap = new ConcurrentHashMap<String, String>();

        if (cacheDuration > 0) {
            executor = Executors.newSingleThreadScheduledExecutor();

            executor.scheduleAtFixedRate(new Runnable() {
                @Override
                public void run() {
                    LOG.debug("expiring mapping cache");
                    cacheMap.clear();
                }
            }, this.cacheDuration * 60, this.cacheDuration * 60, TimeUnit.SECONDS);
        }
    }

    private void registerMBean() {
        try {
            StandardMBean bean = new StandardMBean(this, JMXMappingMBean.class);
            mbeanName = MBeanUtil.registerMBean(NET_TOPOLOGY_COMPONENT_MBEAN, "JMXMapping", bean);
            LOG.info("registered JMXMappingMBean");
        } catch (NotCompliantMBeanException e) {
            LOG.error("unable to register mbean", e);
        }
    }

    private String lookup(String host) {
        String value = cacheMap.get(host);
        if (value != null) {
            LOG.debug("getting mapping from cache for " + host);
            return value;
        }

        JMXConnector jmxc = null;
        try {
            JMXServiceURL url = new JMXServiceURL("service:jmx:rmi:///jndi/rmi://" + host + ":" + this.endpointPort + "/jmxrmi");
            jmxc = JMXConnectorFactory.connect(url, null);

            MBeanServerConnection mbsc = jmxc.getMBeanServerConnection();
            ObjectName            bean = new ObjectName("java.lang:type=Runtime");
            TabularDataSupport    prop = (TabularDataSupport)mbsc.getAttribute(bean, "SystemProperties");
            CompositeData         data = prop.get(new Object[] {this.propertyName});

            if (!StringUtils.isBlank(value = (String)data.get("value"))) {
                LOG.info("adding cache entry for " + host + ": " + value);
                cacheMap.put(host, value);
            } else {
                throw new Exception("bad jmx return value: " + value);
            }
        } catch (Exception e) {
            LOG.error("failed to look up switch for " + host, e);
        } finally {
            try {
                if (jmxc != null) {
                    jmxc.close();
                }
            } catch (Exception e) {
                LOG.error("failed to close jmx connection for " + host, e);
            }
        }

        return value;
    }

    public synchronized List<String> resolve(List<String> names) {
        List<String> results = new ArrayList<String>(names.size());

        for (String name : names) {
            String result = lookup(name);
            if (StringUtils.isBlank(result)) {
                result = this.defaultRack;
            } else if (!StringUtils.startsWith(result, PATH_SEPARATOR_STR)) {
                LOG.warn(name + " lookup result not absolute, prepending " + PATH_SEPARATOR_STR);
                result = PATH_SEPARATOR_STR + result;
            }

            if (this.defaultDepth != StringUtils.countMatches(result, PATH_SEPARATOR_STR)) {
                LOG.warn(name + " lookup result depth does not match default depth, skipping");
                result = this.defaultRack;
            }

            LOG.debug("adding switch mapping: " + name + " : " + result);
            results.add(result);
        }

        return results;
    }

    public String getCachedMappings() {
        return JSON.toString(cacheMap);
    }

    public void reloadCachedMappings() {
        LOG.info("invoking cache purge");
        cacheMap.clear();
    }

    public static void main(String[] args) {
        DNSToSwitchMapping m = new JMXMapping();
        List<String> l = new ArrayList<String>(Arrays.asList(args));

        System.out.println(m.resolve(l));
    }
}
