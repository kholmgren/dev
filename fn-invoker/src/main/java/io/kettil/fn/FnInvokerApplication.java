package io.kettil.fn;

import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.web.bind.annotation.RestController;

@SpringBootApplication
public class FnInvokerApplication {
    //docker run -it -e storage_positions_type=MEMORY -e storage_records_type=MEMORY -p 6565:6565 bsideup/liiklus

    //curl http://localhost:8080 -XPOST -H "Content-Type: application/json" -d "33"

    public static void main(String[] args) {
        SpringApplication.run(FnInvokerApplication.class);
    }
}
