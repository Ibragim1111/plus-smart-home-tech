package ru.yandex.practicum;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.properties.ConfigurationPropertiesScan;
import org.springframework.context.ConfigurableApplicationContext;
import ru.yandex.practicum.listener.HubEventListener;
import ru.yandex.practicum.listener.SnapshotListener;

@SpringBootApplication
@ConfigurationPropertiesScan
public class Analyzer {
    public static void main(String[] args) {
        ConfigurableApplicationContext context = SpringApplication.run(Analyzer.class, args);
        HubEventListener hubEventListener = context.getBean(HubEventListener.class);
        SnapshotListener snapshotListener = context.getBean(SnapshotListener.class);

        Thread hubEventsThread = new Thread(hubEventListener);
        hubEventsThread.setName("HubEventListenerThread");
        hubEventsThread.start();

        snapshotListener.start();
    }
}
