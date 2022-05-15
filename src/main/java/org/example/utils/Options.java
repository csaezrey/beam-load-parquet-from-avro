package org.example.utils;

import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;

public interface Options extends PipelineOptions {

    @Description("Source")
    @Default.String("C:\\beam_result\\input.avro")
    String getSource();
    void setSource(String source);

    @Description("Target")
    @Default.String("C:\\beam_result\\output-")
    String getTarget();
    void setTarget(String target);

    @Description("Sufix")
    @Default.String(".parquet")
    String getSufix();
    void setSufix(String sufix);

    @Description("FieldEncrypt")
    @Default.String("name")
    String getFieldEncrypt();
    void setFieldEncrypt(String fieldEncrypt);

    @Description("Schema")
    @Default.String("{\n" +
            " \"namespace\"    : \"example.avro\",\n" +
            " \"type\"         : \"record\",\n" +
            " \"name\"         : \"User\",\n" +
            " \"fields\"       : [\n" +
            "     {\"name\": \"name\"            , \"type\": \"string\"},\n" +
            "     {\"name\": \"lastname\"            , \"type\": [\"string\",\"null\"]},\n" +
            "     {\"name\": \"number\" , \"type\": \"string\"}\n" +
            " ]\n" +
            "}")
    String getSchema();
    void setSchema(String schema);


}
