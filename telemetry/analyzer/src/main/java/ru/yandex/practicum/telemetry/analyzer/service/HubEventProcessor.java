package ru.yandex.practicum.telemetry.analyzer.service;

import deserializer.HubEventDeserializer;
import jakarta.annotation.PostConstruct;
import jakarta.annotation.Resource;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.common.errors.WakeupException;
import org.springframework.stereotype.Component;
import ru.yandex.practicum.kafka.telemetry.event.HubEventAvro;
import ru.yandex.practicum.telemetry.analyzer.client.KafkaClientConfigurationImpl;
import ru.yandex.practicum.telemetry.analyzer.handler.HubEventHandler;

import java.time.Duration;
import java.util.List;
import java.util.Map;

@Slf4j
@Component
@RequiredArgsConstructor
public class HubEventProcessor {
    private final List<String> TOPICS = List.of("telemetry.hubs.v1");
    private final String GROUPID = "hub-analyzer-group";

    private final KafkaClientConfigurationImpl<HubEventAvro> client;
    private Consumer<String, HubEventAvro> consumer;

    @Resource(name = "hubEventHandlerMap")
    private final Map<String, HubEventHandler> hubEventHandlerMap;

    @PostConstruct
    public void init() {
        this.consumer = client.initConsumer(GROUPID, HubEventDeserializer.class);
    }

    public void start() {
        try {
            consumer.subscribe(TOPICS);
            log.debug("Collect all hub eventsHandler: {}", hubEventHandlerMap);
            while (true) {
                ConsumerRecords<String, HubEventAvro> records =
                        consumer.poll(Duration.ofSeconds(1));
                for (ConsumerRecord<String, HubEventAvro> record : records) {
                    log.debug("New HUB data in {}: Действие {} - {}",
                            record.key(),
                            record.value().getPayload().getClass().getSimpleName(),
                            record.value());
                    if (hubEventHandlerMap.containsKey(record.value().getPayload().getClass().getSimpleName())) {
                        log.debug("Start working with handler: {}", record.value().getPayload().getClass().getSimpleName());
                        hubEventHandlerMap.get(record.value().getPayload().getClass().getSimpleName())
                                .handle(record.value());
                    } else {
                        throw new IllegalArgumentException("Не могу найти обработчик для события :" +
                                record.value().getPayload().getClass().getSimpleName());
                    }
                }
            }
        } catch (WakeupException ignored) {
        } catch (Exception e) {
            log.error("Ошибка во время получения данных хаба", e);
        } finally {
            try {
                consumer.commitSync();
            } finally {
                log.info("Закрываем консьюмер");
                consumer.close();
            }
        }
    }
}
