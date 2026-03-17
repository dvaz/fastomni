package com.example.fastomni.mongo;

import org.springframework.data.mongodb.core.query.Criteria;

import java.time.*;
import java.util.Date;

/**
 * Classe usada para modelar o padrão de datas no modelo UTC para consultar no mongo visto que o mongo sempre
 * trabalha com UTC
 * exemplo:
 * query.addCriteria(
 * Criteria.where("createdAt")
 * .gte(MongoDateUtils.toDate(LocalDateTime.now()))
 * );
 * exemplo:
 * Query query = new Query();
 * query.addCriteria(
 * MongoDateUtils.between("createdAt",
 * LocalDate.of(2026, 3, 1),
 * LocalDate.of(2026, 3, 17)
 * )
 * );
 */


public class MongoDateUtils {

    private MongoDateUtils() {}

    private static final ZoneId DEFAULT_ZONE = ZoneId.of("America/Sao_Paulo");

    // ========================
    // CORE (BASE)
    // ========================

    public static Instant toInstant(LocalDateTime value, ZoneId zone) {
        if (value == null) return null;
        return value.atZone(resolveZone(zone)).toInstant();
    }

    public static Instant toInstant(LocalDateTime value) {
        return toInstant(value, DEFAULT_ZONE);
    }

    public static Instant toInstant(ZonedDateTime value) {
        return (value == null) ? null : value.toInstant();
    }

    public static Instant toInstant(LocalDate value, ZoneId zone) {
        if (value == null) return null;
        return value.atStartOfDay(resolveZone(zone)).toInstant();
    }

    public static Instant toInstant(LocalDate value) {
        return toInstant(value, DEFAULT_ZONE);
    }

    // ========================
    // DATE (sempre derivado de Instant)
    // ========================

    public static Date toDate(Instant instant) {
        return (instant == null) ? null : Date.from(instant);
    }

    public static Date toDate(LocalDateTime value) {
        return toDate(toInstant(value));
    }

    public static Date toDate(ZonedDateTime value) {
        return toDate(toInstant(value));
    }

    public static Date toDate(LocalDate value) {
        return toDate(toInstant(value));
    }

    // ========================
    // INTERVALOS
    // ========================

    public static Date startOfDay(LocalDate date) {
        return toDate(date);
    }

    public static Date endOfDay(LocalDate date) {
        if (date == null) return null;
        return toDate(
                date.atTime(LocalTime.MAX)
                        .atZone(DEFAULT_ZONE)
                        .toInstant()
        );
    }

    // ========================
    // CRITERIA HELPERS
    // ========================

    public static Criteria between(String field, LocalDate start, LocalDate end) {
        return Criteria.where(field)
                .gte(startOfDay(start))
                .lte(endOfDay(end));
    }

    public static Criteria between(String field, LocalDateTime start, LocalDateTime end) {
        return Criteria.where(field)
                .gte(toDate(start))
                .lte(toDate(end));
    }

    public static Criteria between(String field, ZonedDateTime start, ZonedDateTime end) {
        return Criteria.where(field)
                .gte(toDate(start))
                .lte(toDate(end));
    }

    public static Criteria gte(String field, Object value) {
        return Criteria.where(field).gte(toDateGeneric(value));
    }

    public static Criteria lte(String field, Object value) {
        return Criteria.where(field).lte(toDateGeneric(value));
    }

    // ========================
    // GENÉRICO (melhor reaproveitamento)
    // ========================
/*
LocalDateTime , ZonedDateTime, LocalDate, Instant, Date
 */
    private static Date toDateGeneric(Object value) {
        if (value == null) return null;

        if (value instanceof LocalDateTime ldt) return toDate(ldt);
        if (value instanceof ZonedDateTime zdt) return toDate(zdt);
        if (value instanceof LocalDate ld) return toDate(ld);
        if (value instanceof Instant i) return toDate(i);
        if (value instanceof Date d) return d;

        throw new IllegalArgumentException(
                "Tipo de data não suportado: " + value.getClass()
        );
    }

    private static ZoneId resolveZone(ZoneId zone) {
        return (zone != null) ? zone : DEFAULT_ZONE;
    }
}