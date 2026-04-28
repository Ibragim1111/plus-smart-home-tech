package ru.yandex.practicum.repository;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;
import ru.yandex.practicum.model.DeviceAction;

@Repository
public interface ActionRepository extends JpaRepository<DeviceAction, Long> {
}