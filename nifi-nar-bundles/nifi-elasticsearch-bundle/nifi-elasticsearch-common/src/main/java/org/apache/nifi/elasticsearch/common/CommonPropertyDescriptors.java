package org.apache.nifi.elasticsearch.common;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.Validator;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.ssl.SSLContextService;

public class CommonPropertyDescriptors {
    public static final PropertyDescriptor CLUSTER_NAME = new PropertyDescriptor.Builder()
            .name("Cluster Name")
            .description("Name of the ES cluster (for example, elasticsearch_brew). Defaults to 'elasticsearch'")
            .required(false)
            .addValidator(Validator.VALID)
            .build();
    public static final PropertyDescriptor HOSTS = new PropertyDescriptor.Builder()
            .name("ElasticSearch Hosts")
            .description("ElasticSearch Hosts, which should be comma separated and colon for hostname/port "
                    + "host1:port,host2:port,....  For example testcluster:9200.")
            .required(false)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    public static final PropertyDescriptor PROP_SSL_CONTEXT_SERVICE = new PropertyDescriptor.Builder()
            .name("SSL Context Service")
            .description("The SSL Context Service used to provide client certificate information for TLS/SSL "
                    + "connections. This service only applies if the Shield plugin is available.")
            .required(false)
            .identifiesControllerService(SSLContextService.class)
            .build();

    public static final PropertyDescriptor PROP_SHIELD_LOCATION = new PropertyDescriptor.Builder()
            .name("Shield plugin URL")
            .description("Specifies the location of the JAR for the Elasticsearch Shield plugin."
                    + " If the Elasticsearch cluster has been secured with the Shield plugin, then "
                    + "an SSL Context Service must be defined, and the Shield plugin JAR must also "
                    + "be available to this processor.")
            .required(false)
            .addValidator(StandardValidators.URL_VALIDATOR)
            .build();

    public static final PropertyDescriptor PING_TIMEOUT = new PropertyDescriptor.Builder()
            .name("ElasticSearch Ping Timeout")
            .description("The ping timeout used to determine when a node is unreachable. " +
                    "For example, 5s (5 seconds). If non-local recommended is 30s")
            .required(true)
            .defaultValue("5s")
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    public static final PropertyDescriptor SAMPLER_INTERVAL = new PropertyDescriptor.Builder()
            .name("Sampler Interval")
            .description("Node sampler interval. For example, 5s (5 seconds) If non-local recommended is 30s")
            .required(true)
            .defaultValue("5s")
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();
    public static List<PropertyDescriptor> getProperties() {
        final List<PropertyDescriptor> descriptors = new ArrayList<>();
        descriptors.add(CLUSTER_NAME);
        descriptors.add(HOSTS);
        descriptors.add(PROP_SSL_CONTEXT_SERVICE);
        descriptors.add(PROP_SHIELD_LOCATION);
        descriptors.add(PING_TIMEOUT);
        descriptors.add(SAMPLER_INTERVAL);

        return Collections.unmodifiableList(descriptors);
    }

}
