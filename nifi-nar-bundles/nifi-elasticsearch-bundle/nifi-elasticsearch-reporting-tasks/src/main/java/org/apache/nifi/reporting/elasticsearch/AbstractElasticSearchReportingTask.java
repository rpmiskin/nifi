package org.apache.nifi.reporting.elasticsearch;

import java.net.InetSocketAddress;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.nifi.annotation.lifecycle.OnStopped;
import org.apache.nifi.elasticsearch.common.CommonPropertyDescriptors;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.reporting.AbstractReportingTask;
import org.apache.nifi.reporting.ReportingContext;
import org.apache.nifi.ssl.SSLContextService;
import org.apache.nifi.util.StringUtils;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;

public abstract class AbstractElasticSearchReportingTask extends AbstractReportingTask {



    protected AtomicReference<Client> esClient = new AtomicReference<>();
    protected List<InetSocketAddress> esHosts;

    /**
     * Instantiate ElasticSearch Client. This should be called by subclasses' onTrigger() method to create a client
     * if one does not yet exist. The client will be destroyed when the processor is stopped.
     *
     * @param context The context for this processor
     * @throws ProcessException if an error occurs while creating an Elasticsearch client
     */
    protected void createElasticsearchClient(ReportingContext context) throws ProcessException {
        ComponentLog log = getLogger();
        if (esClient.get() != null) {
            return;
        }

        log.info("Creating ElasticSearch Client");

        try {
            final String clusterName = context.getProperty(CommonPropertyDescriptors.CLUSTER_NAME).toString();
            final String pingTimeout = context.getProperty(CommonPropertyDescriptors.PING_TIMEOUT).toString();
            final String samplerInterval = context.getProperty(CommonPropertyDescriptors.SAMPLER_INTERVAL).toString();

            final SSLContextService sslService =
                    context.getProperty(CommonPropertyDescriptors.PROP_SSL_CONTEXT_SERVICE).asControllerService(SSLContextService.class);

            Settings.Builder settingsBuilder = Settings.settingsBuilder()
                    .put("cluster.name", clusterName)
                    .put("client.transport.ping_timeout", pingTimeout)
                    .put("client.transport.nodes_sampler_interval", samplerInterval);

            String shieldUrl = null;
            if (sslService != null) {
                shieldUrl = context.getProperty(CommonPropertyDescriptors.PROP_SHIELD_LOCATION).getValue();
                settingsBuilder.put("shield.transport.ssl", "true")
                        .put("shield.ssl.keystore.path", sslService.getKeyStoreFile())
                        .put("shield.ssl.keystore.password", sslService.getKeyStorePassword())
                        .put("shield.ssl.truststore.path", sslService.getTrustStoreFile())
                        .put("shield.ssl.truststore.password", sslService.getTrustStorePassword());
            }

            TransportClient transportClient = getTransportClient(settingsBuilder, shieldUrl);

            final String hosts = context.getProperty(CommonPropertyDescriptors.HOSTS).getValue();
            esHosts = getEsHosts(hosts);

            if (esHosts != null) {
                for (final InetSocketAddress host : esHosts) {
                    transportClient.addTransportAddress(new InetSocketTransportAddress(host));
                }
            }
            esClient.set(transportClient);

        } catch (Exception e) {
            log.error("Failed to create Elasticsearch client due to {}", new Object[]{e}, e);
            throw new ProcessException(e);
        }
    }

    protected TransportClient getTransportClient(Settings.Builder settingsBuilder, String shieldUrl)
            throws MalformedURLException {

        // Create new transport client using the Builder pattern
        TransportClient.Builder builder = TransportClient.builder();

        // See if the Elasticsearch Shield JAR location was specified, and add the plugin if so

        final ClassLoader originalClassLoader = Thread.currentThread().getContextClassLoader();
        if (!StringUtils.isBlank(shieldUrl)) {
            Thread.currentThread().setContextClassLoader(
                    new URLClassLoader(new URL[]{new URL(shieldUrl)}, this.getClass().getClassLoader()));
        }
        try {
            Class shieldPluginClass = Class.forName("org.elasticsearch.shield.ShieldPlugin", true,
                    Thread.currentThread().getContextClassLoader());
            builder = builder.addPlugin(shieldPluginClass);
        } catch (ClassNotFoundException cnfe) {
            getLogger().debug("Did not detect Elasticsearch Shield plugin, secure connections will not be available");
        }

        TransportClient transportClient = builder.settings(settingsBuilder.build()).build();
        Thread.currentThread().setContextClassLoader(originalClassLoader);
        return transportClient;
    }

    /**
     * Dispose of ElasticSearch client
     */
    @OnStopped
    public final void closeClient() {
        if (esClient.get() != null) {
            getLogger().info("Closing ElasticSearch Client");
            esClient.get().close();
            esClient.set(null);
        }
    }

    /**
     * Get the ElasticSearch hosts from a Nifi attribute, e.g.
     *
     * @param hosts A comma-separated list of ElasticSearch hosts (host:port,host2:port2, etc.)
     * @return List of InetSocketAddresses for the ES hosts
     */
    private List<InetSocketAddress> getEsHosts(String hosts) {

        if (hosts == null) {
            return null;
        }
        final List<String> esList = Arrays.asList(hosts.split(","));
        List<InetSocketAddress> esHosts = new ArrayList<>();

        for (String item : esList) {

            String[] addresses = item.split(":");
            // Protect against invalid input like http://127.0.0.1:9300 (URL scheme should not be there)
            final String hostName = addresses[0].trim();
            final int port = Integer.parseInt(addresses[1].trim());

            esHosts.add(new InetSocketAddress(hostName, port));
        }
        return esHosts;
    }

}
