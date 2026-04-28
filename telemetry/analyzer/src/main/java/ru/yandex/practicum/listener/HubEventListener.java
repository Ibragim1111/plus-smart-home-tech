package ru.yandex.practicum.listener;

import lombok.extern.slf4j.Slf4j;
import org.apache.avro.specific.SpecificRecordBase;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import ru.yandex.practicum.handler.hub.HubEventHandler;
import ru.yandex.practicum.kafka.telemetry.event.*;

import java.time.Duration;
import java.util.*;
import java.util.function.Function;
import java.util.stream.Collectors;

@Slf4j
@Component
public class HubEventListener implements Runnable {
    private final KafkaConsumer<String, HubEventAvro> hubEventConsumer;
    private final String hubEventsTopic;
    private final Map<Class<? extends SpecificRecordBase>, HubEventHandler> handlers;

    @Autowired
    public HubEventListener(KafkaConsumer<String, HubEventAvro> hubEventConsumer,
                            String hubEventsTopic,
                            Set<HubEventHandler> handlers) {
        this.hubEventConsumer = hubEventConsumer;
        this.hubEventsTopic = hubEventsTopic;
        this.handlers = handlers.stream()
                .collect(Collectors.toMap(HubEventHandler::getPayloadType, Function.identity()));
    }

    @Override
    public void run() {
        try {
            hubEventConsumer.subscribe(List.of(hubEventsTopic));
            log.info("Подписка на топик {}", hubEventsTopic);
            while (true) {
                ConsumerRecords<String, HubEventAvro> hubEventsRecords = hubEventConsumer.poll(Duration.ofMillis(1000));

                for (ConsumerRecord<String, HubEventAvro> record : hubEventsRecords) {
                    log.info("Получено сообщение {} из партиции {}, со смещением {}:",
                            record.value(), record.partition(), record.offset());

                    String hubId = record.value().getHubId();
                    Object payload = record.value().getPayload();
                    Class<?> payloadType = payload.getClass();

                    if (handlers.containsKey(payloadType)) {
                        handlers.get(payloadType).handle(payload, hubId);
                    } else {
                        throw new IllegalArgumentException("Не найден обработчик для события типа " + payloadType);
                    }
                }
            }
        } catch (WakeupException ignored) {
            // игнорируем - закрываем консьюмер и продюсер в блоке finally
        } catch (Exception e) {
            log.error("Ошибка во время обработки события хаба", e);
        } finally {
            try {
                hubEventConsumer.commitSync();
            } finally {
                log.info("Закрываем консьюмер");
                hubEventConsumer.close();
            }
        }
    }
}