package io.kettil.fn;

import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;
import org.springframework.validation.annotation.Validated;

import javax.validation.constraints.NotNull;

@Component
@ConfigurationProperties(prefix = "fn")
@Validated
@Data
public class FnProperties {
    @NotNull
    private String location;

    @NotNull
    private String name;
}

