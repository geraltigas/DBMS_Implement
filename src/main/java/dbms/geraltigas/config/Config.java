package dbms.geraltigas.config;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.util.concurrent.ExecutorService;

@Configuration
public class Config {
    @Bean
    public ExecutorService executorService() {
        return java.util.concurrent.Executors.newFixedThreadPool(10);
    }

    @Bean
    public ObjectMapper objectMapper() {
        return new ObjectMapper();
    }
}
