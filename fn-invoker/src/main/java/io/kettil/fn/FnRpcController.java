package io.kettil.fn;

import org.springframework.cloud.function.context.FunctionCatalog;
import org.springframework.cloud.function.context.FunctionProperties;
import org.springframework.http.MediaType;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RestController;

import java.util.function.Function;

@RestController
public class FnRpcController {
    private final FunctionCatalog catalog;
    private final FunctionProperties functionProperties;

    public FnRpcController(FunctionCatalog functionCatalog, FunctionProperties functionProperties) {
        this.catalog = functionCatalog;
        this.functionProperties = functionProperties;
    }

    @PostMapping(consumes = MediaType.APPLICATION_JSON_VALUE, produces = MediaType.APPLICATION_JSON_VALUE)
    public Object invoke(@RequestBody Object value) {
        Function<Object, Object> objectFunction = catalog.lookup(functionProperties.getDefinition());
        return objectFunction.apply(value);
    }
}
