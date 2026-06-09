package com.mashnet.core.models;

import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.List;
import java.util.Map;

public class ComputationSchema {

    @JsonProperty("schema_id")
    public String schemaId;

    @JsonProperty("input_sources")
    public Map<String, String> inputSources; // Наприклад: {"input-A": "NodePy-1", "input-B": "NodePy-2"}

    @JsonProperty("output_sink")
    public String outputSink;

    @JsonProperty("pipeline_stages")
    public List<PipelineStage> pipelineStages;

    public ComputationSchema() {}

    public ComputationSchema(String schemaId, Map<String, String> inputSources, String outputSink, List<PipelineStage> pipelineStages) {
        this.schemaId = schemaId;
        this.inputSources = inputSources;
        this.outputSink = outputSink;
        this.pipelineStages = pipelineStages;
    }

    public static class PipelineStage {
        @JsonProperty("stage_id")
        public String stageId;

        @JsonProperty("operation")
        public String operation;

        @JsonProperty("parameters")
        public Map<String, Object> parameters;

        public PipelineStage() {}

        public PipelineStage(String stageId, String operation, Map<String, Object> parameters) {
            this.stageId = stageId;
            this.operation = operation;
            this.parameters = parameters;
        }
    }
}