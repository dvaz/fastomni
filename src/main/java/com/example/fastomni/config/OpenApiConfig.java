package com.example.fastomni.config;
import io.swagger.v3.oas.models.ExternalDocumentation;
import io.swagger.v3.oas.models.OpenAPI;
import io.swagger.v3.oas.models.info.Info;
import io.swagger.v3.oas.models.info.Contact;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class OpenApiConfig {

    @Bean
    public OpenAPI customOpenAPI() {
        return new OpenAPI()
                .info(new Info()
                        .title("Fast Omni API")
                        .version("v1")
                        .description("Documentação da API Fast Omni")
                        .contact(new Contact()
                                .name("Seu Nome")
                                .email("seu@email.com")
                        )
                )
                .externalDocs(new ExternalDocumentation()
                        .description("Documentação adicional")
                        .url("https://seusite.com/docs"));
    }
}