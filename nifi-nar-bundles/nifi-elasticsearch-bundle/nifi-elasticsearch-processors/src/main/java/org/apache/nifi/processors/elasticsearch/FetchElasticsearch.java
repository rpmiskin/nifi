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

import org.apache.nifi.annotation.behavior.EventDriven;
import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.behavior.WritesAttribute;
import org.apache.nifi.annotation.behavior.WritesAttributes;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.elasticsearch.common.CommonPropertyDescriptors;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.logging.ProcessorLog;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.io.OutputStreamCallback;
import org.apache.nifi.processor.util.StandardValidators;
import org.elasticsearch.ElasticsearchParseException;
import org.elasticsearch.ElasticsearchTimeoutException;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.client.transport.NoNodeAvailableException;
import org.elasticsearch.node.NodeClosedException;
import org.elasticsearch.transport.ReceiveTimeoutTransportException;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;


@InputRequirement(InputRequirement.Requirement.INPUT_REQUIRED)
@EventDriven
@Tags({"elasticsearch", "fetch", "read"})
@CapabilityDescription("Retrieves a document from Elasticsearch using the specified connection properties and the identifier of the document to retrieve")
@WritesAttributes({
        @WritesAttribute(attribute = "filename", description = "The document identifier"),
        @WritesAttribute(attribute = "es.index", description = "The Elasticsearch index containing the document"),
        @WritesAttribute(attribute = "es.type", description = "The Elasticsearch document type")
})
public class FetchElasticsearch extends AbstractElasticsearchProcessor {

    public static final Relationship REL_SUCCESS = new Relationship.Builder().name("success")
            .description("All FlowFiles that are read from Elasticsearch are routed to this relationship").build();

    public static final Relationship REL_FAILURE = new Relationship.Builder().name("failure")
            .description("All FlowFiles that cannot be read from Elasticsearch are routed to this relationship").build();

    public static final Relationship REL_RETRY = new Relationship.Builder().name("retry")
            .description("A FlowFile is routed to this relationship if the document cannot be fetched but attempting the operation again may succeed")
            .build();

    public static final Relationship REL_NOT_FOUND = new Relationship.Builder().name("not found")
            .description("A FlowFile is routed to this relationship if the specified document does not exist in the Elasticsearch cluster")
            .build();

    public static final PropertyDescriptor DOC_ID_ATTRIBUTE = new PropertyDescriptor.Builder()
            .name("Document Identifier Attribute")
            .description("The name of the attribute containing the document identifier for each FlowFile")
            .required(true)
            .expressionLanguageSupported(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    public static final PropertyDescriptor INDEX = new PropertyDescriptor.Builder()
            .name("Index")
            .description("The name of the index to read from")
            .required(true)
            .expressionLanguageSupported(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    public static final PropertyDescriptor TYPE = new PropertyDescriptor.Builder()
            .name("Type")
            .description("The type of this document (used by Elasticsearch for indexing and searching)")
            .required(true)
            .expressionLanguageSupported(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();


    @Override
    public Set<Relationship> getRelationships() {
        final Set<Relationship> relationships = new HashSet<>();
        relationships.add(REL_SUCCESS);
        relationships.add(REL_FAILURE);
        relationships.add(REL_RETRY);
        relationships.add(REL_NOT_FOUND);
        return Collections.unmodifiableSet(relationships);
    }

    @Override
    public final List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        final List<PropertyDescriptor> descriptors = new ArrayList<>(CommonPropertyDescriptors.getProperties());
        descriptors.add(DOC_ID_ATTRIBUTE);
        descriptors.add(INDEX);
        descriptors.add(TYPE);

        return Collections.unmodifiableList(descriptors);
    }


    @Override
    public void onTrigger(final ProcessContext context, final ProcessSession session) throws ProcessException {

        FlowFile flowFile = session.get();
        if (flowFile == null) {
            return;
        }

        // Create the client if one does not already exist
        createElasticsearchClient(context);

        final String index = context.getProperty(INDEX).evaluateAttributeExpressions(flowFile).getValue();
        final String docId = context.getProperty(DOC_ID_ATTRIBUTE).evaluateAttributeExpressions(flowFile).getValue();
        final String docType = context.getProperty(TYPE).evaluateAttributeExpressions(flowFile).getValue();

        final ProcessorLog logger = getLogger();
        try {

            logger.debug("Fetching {}/{}/{} from Elasticsearch", new Object[]{index, docType, docId});
            final GetResponse getResponse =
                    esClient.get().prepareGet(index, docType, docId).execute().actionGet();

            if (getResponse == null || !getResponse.isExists()) {
                logger.warn("Failed to read {}/{}/{} from Elasticsearch: Document not found",
                        new Object[]{index, docType, docId});

                // We couldn't find the document, so penalize it and send it to "not found"
                flowFile = session.penalize(flowFile);
                session.transfer(flowFile, REL_NOT_FOUND);
            } else {
                flowFile = session.putAttribute(flowFile, "filename", docId);
                flowFile = session.putAttribute(flowFile, "es.index", index);
                flowFile = session.putAttribute(flowFile, "es.type", docType);
                flowFile = session.write(flowFile, new OutputStreamCallback() {
                    @Override
                    public void process(OutputStream out) throws IOException {
                        out.write(getResponse.getSourceAsString().getBytes(StandardCharsets.UTF_8));
                    }
                });
                logger.debug("Elasticsearch document " + docId + " fetched, routing to success");
                session.transfer(flowFile, REL_SUCCESS);
            }
        } catch (NoNodeAvailableException nne) {
            logger.error("Failed to read {} from Elasticsearch: No Node Available {}", new Object[]{nne}, nne);
            session.transfer(flowFile, REL_RETRY);
            context.yield();

        } catch (ElasticsearchTimeoutException ete) {
            logger.error("Failed to read {} from Elasticsearch : Timeout to {}", new Object[]{ete}, ete);
            session.transfer(flowFile, REL_RETRY);
            context.yield();

        } catch (ReceiveTimeoutTransportException rtt) {
            logger.error("Failed to read {} from Elasticsearch: ReceiveTimeoutTransportException to {}", new Object[]{rtt}, rtt);
            session.transfer(flowFile, REL_FAILURE);
            context.yield();
        } catch (ElasticsearchParseException esp) {
            logger.error("Failed to read {} from Elasticsearch Parse Exception {}", new Object[]{esp}, esp);
            session.transfer(flowFile, REL_FAILURE);
            context.yield();

        } catch (NodeClosedException nce) {
            logger.error("Failed to read {} from Elasticsearch: Node Closed Exception {}", new Object[]{nce}, nce);
            session.transfer(flowFile, REL_RETRY);
            context.yield();
        } catch (Exception e) {
            logger.error("Failed to read {} from Elasticsearch: Unknown Exception {}", new Object[]{e}, e);
            session.transfer(flowFile, REL_FAILURE);
            context.yield();
        }
    }
}
