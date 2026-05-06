package com.mashnet.core.models;

import com.fasterxml.jackson.annotation.JsonProperty;

public class ComputationSchema {

    @JsonProperty("schema_id")
    public String schemaId;

    @JsonProperty("operation")
    public String operation;

    @JsonProperty("input_source")
    public String inputSource;

    @JsonProperty("output_sink")
    public String outputSink;

    public ComputationSchema() {}

    public ComputationSchema(String schemaId, String operation, String inputSource, String outputSink) {
        this.schemaId = schemaId;
        this.operation = operation;
        this.inputSource = inputSource;
        this.outputSink = outputSink;
    }
}