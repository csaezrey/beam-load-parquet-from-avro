package org.example.functions;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.beam.sdk.transforms.DoFn;

import static org.example.utils.Constants.COLON;

public class PrintFunction  extends DoFn<GenericRecord, Void> {

    @ProcessElement
    public void processElement(ProcessContext context){
        GenericRecord record = context.element();
        for (Schema.Field field : record.getSchema().getFields()) {
            String fieldKey = field.name();
            System.out.println(fieldKey+COLON+record.get(fieldKey).toString());
        }
    }

}
