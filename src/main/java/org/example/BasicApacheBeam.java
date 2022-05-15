package org.example;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.FileIO;
import org.apache.beam.sdk.io.parquet.ParquetIO;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.io.AvroIO;
import org.example.functions.EncryptFunction;
import org.example.functions.PrintFunction;
import org.example.utils.Options;

public class BasicApacheBeam {
    public static void main(String args[]){

        //Read from args
        PipelineOptionsFactory.register(Options.class);
        Options options = PipelineOptionsFactory.fromArgs(args).withValidation().as(Options.class);

        Schema.Parser schemaParser = new Schema.Parser();
        Schema schema = schemaParser.parse(options.getSchema());

        //Create Pipeline
        Pipeline pipeline=Pipeline.create();

        //Extract
        PCollection<GenericRecord> inputData = pipeline.apply(AvroIO.readGenericRecords(options.getSchema()).from(options.getSource()));

        //Transform (Uppercase)
        EncryptFunction transform = new EncryptFunction(options.getSchema(), options.getFieldEncrypt());
        PCollection<GenericRecord> transformData =  inputData.apply(ParDo.of(transform));

        //Only print result
        transformData.apply(ParDo.of(new PrintFunction()));

        //Load
        transformData.apply(FileIO.<GenericRecord>write().via(ParquetIO.sink(schema)).to(options.getTarget()).withNumShards(1));

        //Execute steps
        pipeline.run();


    }

}