package org.apache.nifi.elasticsearch.common;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.expression.AttributeExpression;
import org.apache.nifi.processor.util.StandardValidators;

public final class InsertPropertyDescriptors {
    public static final PropertyDescriptor ID_ATTRIBUTE = new PropertyDescriptor.Builder()
            .name("Identifier Attribute")
            .description("The name of the attribute containing the identifier for each FlowFile")
            .required(true)
            .expressionLanguageSupported(false)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    public static final PropertyDescriptor INDEX = new PropertyDescriptor.Builder()
            .name("Index")
            .description("The name of the index to insert into")
            .required(true)
            .expressionLanguageSupported(true)
            .addValidator(StandardValidators.createAttributeExpressionLanguageValidator(
                    AttributeExpression.ResultType.STRING, true))
            .build();

    public static final PropertyDescriptor TYPE = new PropertyDescriptor.Builder()
            .name("Type")
            .description("The type of this document (used by Elasticsearch for indexing and searching)")
            .required(true)
            .expressionLanguageSupported(true)
            .addValidator(StandardValidators.createAttributeExpressionLanguageValidator(
                    AttributeExpression.ResultType.STRING, true))
            .build();

    public static final PropertyDescriptor BATCH_SIZE = new PropertyDescriptor.Builder()
            .name("Batch Size")
            .description("The preferred number of FlowFiles to put to the database in a single transaction")
            .required(true)
            .addValidator(StandardValidators.POSITIVE_INTEGER_VALIDATOR)
            .defaultValue("100")
            .build();

    public static List<PropertyDescriptor> getProperties() {
        List<PropertyDescriptor> list = new ArrayList<>(CommonPropertyDescriptors.getProperties());
        list.add(ID_ATTRIBUTE);
        list.add(INDEX);
        list.add(TYPE);
        list.add(BATCH_SIZE);
        return Collections.unmodifiableList(list);
    }

}
