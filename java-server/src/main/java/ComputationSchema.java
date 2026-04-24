import com.fasterxml.jackson.annotation.JsonProperty;

public class ComputationSchema {

    @JsonProperty("schema_id")
    public String schemaId;

    @JsonProperty("operation")
    public String operation; // Наприклад: "ADD", "MULTIPLY", "PROCESS_IMAGE"

    @JsonProperty("input_source")
    public String inputSource; // Звідки беремо дані (наприклад, "port:7001" або "db_url")

    @JsonProperty("output_sink")
    public String outputSink; // Куди відправляємо результат

    // Порожній конструктор потрібен для Jackson
    public ComputationSchema() {}

    public ComputationSchema(String schemaId, String operation, String inputSource, String outputSink) {
        this.schemaId = schemaId;
        this.operation = operation;
        this.inputSource = inputSource;
        this.outputSink = outputSink;
    }
}