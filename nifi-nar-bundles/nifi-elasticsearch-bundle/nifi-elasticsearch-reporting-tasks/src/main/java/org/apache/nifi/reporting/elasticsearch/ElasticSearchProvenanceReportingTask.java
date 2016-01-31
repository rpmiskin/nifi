package org.apache.nifi.reporting.elasticsearch;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.elasticsearch.common.InsertPropertyDescriptors;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.provenance.ProvenanceEventRecord;
import org.apache.nifi.reporting.ReportingContext;
import org.elasticsearch.ElasticsearchTimeoutException;
import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.client.transport.NoNodeAvailableException;
import org.elasticsearch.node.NodeClosedException;
import org.elasticsearch.transport.ReceiveTimeoutTransportException;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

public class ElasticSearchProvenanceReportingTask extends AbstractElasticSearchReportingTask {



    @Override
    public final List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        final List<PropertyDescriptor> descriptors = new ArrayList<>();
        descriptors.addAll(InsertPropertyDescriptors.getProperties());

        return Collections.unmodifiableList(descriptors);
    }

    @Override
    public void onTrigger(ReportingContext context) {
        // Create the client if one does not already exist
        createElasticsearchClient(context);
        final int batchSize = context.getProperty(InsertPropertyDescriptors.BATCH_SIZE).asInteger();
        final String index = context.getProperty(InsertPropertyDescriptors.INDEX).evaluateAttributeExpressions().getValue();
        final String docType = context.getProperty(InsertPropertyDescriptors.TYPE).evaluateAttributeExpressions().getValue();
        // Find the things to upload
        ComponentLog logger = getLogger();
        try {
            final long firstEventId =  loadLastEventId() + 1;
            List<ProvenanceEventRecord> provenanceEvents = context.getEventAccess().getProvenanceEvents(firstEventId, batchSize);

            // Do the upload
            final BulkRequestBuilder bulk = esClient.get().prepareBulk();
            ObjectMapper mapper = new ObjectMapper();

            for (ProvenanceEventRecord event : provenanceEvents) {
                // TODO Need to include hostname or some other node identifer within this id
                final String id = "" + event.getEventId();
                bulk.add(esClient.get().prepareIndex(index, docType, id)
                        .setSource(eventToBytes(mapper, event)));
            }

            final BulkResponse response = bulk.execute().actionGet();
            if (response.hasFailures()) {
                for (final BulkItemResponse item : response.getItems()) {
                    if (item.getFailure() != null) {
                        logger.error("Failed to insert {} into Elasticsearch due to {}",
                                new Object[]{provenanceEvents.get(item.getItemId()), item.getFailure().getMessage()});
                    }
                }
            } else {
                // Record the last id we inserted.
                final long lastId = provenanceEvents.get(provenanceEvents.size()-1).getEventId();
                saveLastEventId(lastId);
            }

        } catch (NoNodeAvailableException
                | ElasticsearchTimeoutException
                | ReceiveTimeoutTransportException
                | IOException
                | NodeClosedException e) {
            logger.error("Failed to insert into Elasticsearch due to {}",
                    new Object[]{e.getLocalizedMessage()}, e);

        } catch (Exception exceptionToFail) {
            logger.error("Failed to insert {} into Elasticsearch due to {}",
                    new Object[]{exceptionToFail.getLocalizedMessage()}, exceptionToFail);

        }



    }

    private byte[] eventToBytes(ObjectMapper mapper, ProvenanceEventRecord event)
            throws JsonProcessingException {
        Map<String, Object> json = new LinkedHashMap<>();
        json.put("eventId", event.getEventId());
        json.put("eventType", event.getEventType().toString());
        json.put("eventTime", new Date(event.getEventTime()));
        json.put("eventDuration", event.getEventDuration());

        Map<String, String> attributes = new LinkedHashMap<>();
        for (Entry<String, String> x :event.getAttributes().entrySet()) {
            String key = x.getKey().replace('.', '-');
            attributes.put(key, x.getValue());
        }

        json.put("attributes", attributes);

        return mapper.writeValueAsBytes(json);
    }

    private void saveLastEventId(long lastId) {
        // TODO Implement this

    }

    private int loadLastEventId() {
        // TODO Implement this
        return 0;
    }

}
