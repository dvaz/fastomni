package com.example.fastomni.mongo;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.convert.converter.Converter;
import org.springframework.data.mongodb.core.convert.MongoCustomConversions;

@Configuration
public class GlobalMongoDateConversionConfig {

    private static final ZoneId SAO_PAULO = ZoneId.of("America/Sao_Paulo");

    @Bean
    public MongoCustomConversions mongoCustomConversions() {
        List<Converter<?, ?>> converters = new ArrayList<>();

        // ---------- ZonedDateTime ----------
        converters.add(new Converter<ZonedDateTime, Date>() {
            @Override
            public Date convert(ZonedDateTime source) {
                return (source == null) ? null : Date.from(source.toInstant());
            }
        });
        converters.add(new Converter<Date, ZonedDateTime>() {
            @Override
            public ZonedDateTime convert(Date source) {
                return (source == null) ? null : source.toInstant().atZone(SAO_PAULO);
            }
        });

        // ---------- LocalDateTime ----------
        converters.add(new Converter<LocalDateTime, Date>() {
            @Override
            public Date convert(LocalDateTime source) {
                return (source == null) ? null : Date.from(source.atZone(SAO_PAULO).toInstant());
            }
        });
        converters.add(new Converter<Date, LocalDateTime>() {
            @Override
            public LocalDateTime convert(Date source) {
                return (source == null) ? null : LocalDateTime.ofInstant(source.toInstant(), SAO_PAULO);
            }
        });

        // ---------- LocalDate ----------
        converters.add(new Converter<LocalDate, Date>() {
            @Override
            public Date convert(LocalDate source) {
                return (source == null) ? null : Date.from(source.atStartOfDay(SAO_PAULO).toInstant());
            }
        });
        converters.add(new Converter<Date, LocalDate>() {
            @Override
            public LocalDate convert(Date source) {
                return (source == null) ? null : source.toInstant().atZone(SAO_PAULO).toLocalDate();
            }
        });

        return new MongoCustomConversions(converters);
    }
}