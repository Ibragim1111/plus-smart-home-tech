package ru.yandex.practicum.repository;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;
import ru.yandex.practicum.model.Device;

import java.util.Collection;
import java.util.Optional;

@Repository
public interface DeviceRepository extends JpaRepository<Device, String> {
    boolean existsByIdInAndHubId(Collection<String> ids, String hubId);

    Optional<Device> findByIdAndHubId(String id, String hubId);
}