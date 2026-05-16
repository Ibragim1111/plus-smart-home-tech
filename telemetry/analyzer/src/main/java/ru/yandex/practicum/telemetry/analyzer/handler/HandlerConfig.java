package ru.yandex.practicum.telemetry.analyzer.handler;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

@Configuration
public class HandlerConfig {

    @Bean
    public Map<String, HubEventHandler> hubEventHandlerMap(Set<HubEventHandler> handlers) {
        Map<String, HubEventHandler> map = handlers.stream()
                .collect(Collectors.toMap(
                        HubEventHandler::getEvent,
                        Function.identity()
                ));

        System.out.println("=== HandlerConfig: Creating hubEventHandlerMap ===");
        System.out.println("Total handlers found: " + handlers.size());
        map.forEach((key, value) -> {
            System.out.println("  Key: '" + key + "' -> Handler: " + value.getClass().getSimpleName());
        });
        System.out.println("================================================");

        return map;
    }
}
