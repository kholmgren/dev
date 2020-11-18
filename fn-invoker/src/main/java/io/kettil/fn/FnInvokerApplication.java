package io.kettil.fn;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.web.bind.annotation.RestController;

@SpringBootApplication
@RestController
public class FnInvokerApplication implements CommandLineRunner {
    //docker run -it -e storage_positions_type=MEMORY -e storage_records_type=MEMORY -p 6565:6565 bsideup/liiklus

    //curl http://localhost:8080 -XPOST -H "Content-Type: application/json" -d "33"

    public static void main(String[] args) {
        SpringApplication.run(FnInvokerApplication.class);
    }

    @Autowired
    FnProperties fnProperties;

    @Override
    public void run(String... args) throws Exception {
        System.out.println("fnProperties=" + fnProperties);
    }
}
