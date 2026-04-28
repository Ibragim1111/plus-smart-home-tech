package ru.yandex.practicum.listener;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import ru.yandex.practicum.handler.snapshot.SnapshotHandler;
import ru.yandex.practicum.kafka.telemetry.event.SensorsSnapshotAvro;

import java.time.Duration;
import java.util.List;

@Slf4j
@Component
public class SnapshotListener {
    private final KafkaConsumer<String, SensorsSnapshotAvro> snapshotsConsumer;
    private final String snapshotsTopic;
    private final SnapshotHandler snapshotHandler;

    @Autowired
    public SnapshotListener(KafkaConsumer<String, SensorsSnapshotAvro> snapshotsConsumer,
                            String snapshotsTopic,
                            SnapshotHandler snapshotHandler) {
        this.snapshotsConsumer = snapshotsConsumer;
        this.snapshotsTopic = snapshotsTopic;
        this.snapshotHandler = snapshotHandler;
    }

    public void start() {
        try {
            snapshotsConsumer.subscribe(List.of(snapshotsTopic));
            while (true) {
                ConsumerRecords<String, SensorsSnapshotAvro> snapshotsRecords =
                        snapshotsConsumer.poll(Duration.ofMillis(1000));

                for (ConsumerRecord<String, SensorsSnapshotAvro> record : snapshotsRecords) {
                    log.info("Получено сообщение {}\nтипа {}\nиз топика {}\nиз партиции {}\nсо смещением {}",
                            record.value(),
                            record.value().getClass().getSimpleName(),
                            snapshotsTopic,
                            record.partition(),
                            record.offset());
                    SensorsSnapshotAvro snapshot = record.value();
                    snapshotHandler.handle(snapshot);
                }
            }
        } catch (WakeupException ignored) {
        } catch (Exception e) {
            log.error("Ошибка во время обработки снапшота", e);
        } finally {
            try {
                snapshotsConsumer.commitSync();
            } finally {
                log.info("Закрываем консьюмер");
                snapshotsConsumer.close();
            }
        }
    }
}
