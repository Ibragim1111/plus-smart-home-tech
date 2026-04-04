package ru.yandex.practicum.producer;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;
import ru.yandex.practicum.kafka.telemetry.event.HubEventAvro;
import ru.yandex.practicum.mapper.HubEventMapper;
import ru.yandex.practicum.model.hub.HubEvent;

@Component
@RequiredArgsConstructor
@Slf4j
public class HubEventProducer {

    private final KafkaTemplate<String, HubEventAvro> kafkaTemplate;
    private final HubEventMapper mapper;

    @Value("${spring.kafka.topics.hubs}")
    private String hubsTopic;

    public void send(HubEvent event) {
        HubEventAvro avroEvent = mapper.toAvro(event);
        log.info("Sending hub event to topic {}: {}", hubsTopic, avroEvent);
        kafkaTemplate.send(hubsTopic, event.getHubId(), avroEvent)
                .whenComplete((result, ex) -> {
                    if (ex == null) {
                        log.debug("Hub event sent successfully: {}", avroEvent);
                    } else {
                        log.error("Failed to send hub event: {}", avroEvent, ex);
                    }
                });
    }
}
