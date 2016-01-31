/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.nifi.processors.elasticsearch;

import org.apache.commons.io.IOUtils;
import org.apache.nifi.annotation.behavior.EventDriven;
import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.elasticsearch.common.InsertPropertyDescriptors;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.logging.ProcessorLog;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.io.InputStreamCallback;
import org.elasticsearch.ElasticsearchTimeoutException;
import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;

import org.elasticsearch.client.transport.NoNodeAvailableException;
import org.elasticsearch.node.NodeClosedException;
import org.elasticsearch.transport.ReceiveTimeoutTransportException;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;


@InputRequirement(InputRequirement.Requirement.INPUT_REQUIRED)
@EventDriven
@Tags({"elasticsearch", "insert", "update", "write", "put"})
@CapabilityDescription("Writes the contents of a FlowFile to Elasticsearch")
public class PutElasticsearch extends AbstractElasticsearchProcessor {

    static final Relationship REL_SUCCESS = new Relationship.Builder().name("success")
            .description("All FlowFiles that are written to Elasticsearch are routed to this relationship").build();

    static final Relationship REL_FAILURE = new Relationship.Builder().name("failure")
            .description("All FlowFiles that cannot be written to Elasticsearch are routed to this relationship").build();

    static final Relationship REL_RETRY = new Relationship.Builder().name("retry")
            .description("A FlowFile is routed to this relationship if the database cannot be updated but attempting the operation again may succeed")
            .build();

    @Override
    public Set<Relationship> getRelationships() {
        final Set<Relationship> relationships = new HashSet<>();
        relationships.add(REL_SUCCESS);
        relationships.add(REL_FAILURE);
        relationships.add(REL_RETRY);
        return Collections.unmodifiableSet(relationships);
    }

    @Override
    public final List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        final List<PropertyDescriptor> descriptors = new ArrayList<>(InsertPropertyDescriptors.getProperties());

        return Collections.unmodifiableList(descriptors);
    }


    @Override
    public void onTrigger(final ProcessContext context, final ProcessSession session) throws ProcessException {
        // Create the client if one does not already exist
        createElasticsearchClient(context);
        final int batchSize = context.getProperty(InsertPropertyDescriptors.BATCH_SIZE).asInteger();
        final String index = context.getProperty(InsertPropertyDescriptors.INDEX).evaluateAttributeExpressions().getValue();
        final String id_attribute = context.getProperty(InsertPropertyDescriptors.ID_ATTRIBUTE).getValue();
        final String docType = context.getProperty(InsertPropertyDescriptors.TYPE).evaluateAttributeExpressions().getValue();

        final List<FlowFile> flowFiles = session.get(batchSize);
        if (flowFiles.isEmpty()) {
            return;
        }

        final ProcessorLog logger = getLogger();

        try {
            final BulkRequestBuilder bulk = esClient.get().prepareBulk();
            for (FlowFile file : flowFiles) {
                final String id = file.getAttribute(id_attribute);
                if (id == null) {
                    getLogger().error("no value in identifier attribute {}", new Object[]{id_attribute});
                    session.transfer(file, REL_FAILURE);
                }
                session.read(file, new InputStreamCallback() {
                    @Override
                    public void process(final InputStream in) throws IOException {
                        String json = IOUtils.toString(in, StandardCharsets.UTF_8)
                                .replace("\r\n", " ").replace('\n', ' ').replace('\r', ' ');
                        bulk.add(esClient.get().prepareIndex(index, docType, id)
                                .setSource(json.getBytes(StandardCharsets.UTF_8)));
                    }
                });
            }

            final BulkResponse response = bulk.execute().actionGet();
            if (response.hasFailures()) {
                for (final BulkItemResponse item : response.getItems()) {
                    final FlowFile flowFile = flowFiles.get(item.getItemId());
                    if (item.isFailed()) {
                        logger.error("Failed to insert {} into Elasticsearch due to {}",
                                new Object[]{flowFile, item.getFailure()});
                        session.transfer(flowFile, REL_FAILURE);

                    } else {
                        session.transfer(flowFile, REL_SUCCESS);

                    }
                }
            } else {
                for (final FlowFile flowFile : flowFiles) {
                    session.transfer(flowFile, REL_SUCCESS);
                }
            }


        } catch (NoNodeAvailableException
                | ElasticsearchTimeoutException
                | ReceiveTimeoutTransportException
                | NodeClosedException exceptionToRetry) {
            logger.error("Failed to insert into Elasticsearch due to {}",
                    new Object[]{exceptionToRetry.getLocalizedMessage()}, exceptionToRetry);
            session.transfer(flowFiles, REL_RETRY);
            context.yield();

        } catch (Exception exceptionToFail) {
            logger.error("Failed to insert {} into Elasticsearch due to {}",
                    new Object[]{exceptionToFail.getLocalizedMessage()}, exceptionToFail);

            for (final FlowFile flowFile : flowFiles) {
                session.transfer(flowFile, REL_FAILURE);
            }
            context.yield();
        }
    }
}
