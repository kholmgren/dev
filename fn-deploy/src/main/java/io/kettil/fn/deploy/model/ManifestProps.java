package io.kettil.fn.deploy.model;

import com.fasterxml.jackson.annotation.JsonAnyGetter;
import com.fasterxml.jackson.annotation.JsonAnySetter;
import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.Data;

import java.io.File;
import java.util.LinkedHashMap;
import java.util.Map;

@Data
public class ManifestProps {
    private final Map<String, Object> any = new LinkedHashMap<>();

    private String runtime;

    private File location;

    @JsonProperty("class-name")
    private String className;

    @Data
    public static class StreamProps {
        @JsonProperty("in-topic")
        private String inTopic;

        @JsonProperty("in-group")
        private String inGroup;

        @JsonProperty("out-topic")
        private String outTopic;
    }

    @JsonProperty("stream")
    private final StreamProps stream = new StreamProps();

    @JsonAnyGetter
    public Map<String, Object> getAny() {
        return any;
    }

    @JsonAnySetter
    public void putAny(String propertyKey, Object value) {
        this.any.put(propertyKey, value);
    }
}
