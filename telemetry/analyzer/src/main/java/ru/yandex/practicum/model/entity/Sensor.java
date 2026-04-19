package ru.yandex.practicum.model.entity;

import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.Table;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Entity
@Table(name = "sensors")
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class Sensor {
    @Id
    private String id;
    private String hubId;
    private String deviceType;
    private Long registeredAt;
}