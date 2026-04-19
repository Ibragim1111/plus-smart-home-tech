-- Создание таблиц для Analyzer

-- Таблица датчиков
CREATE TABLE IF NOT EXISTS sensors (
                                       id VARCHAR(255) PRIMARY KEY,
    hub_id VARCHAR(255) NOT NULL,
    device_type VARCHAR(50) NOT NULL,
    registered_at BIGINT NOT NULL
    );

-- Таблица сценариев
CREATE TABLE IF NOT EXISTS scenarios (
                                         id BIGSERIAL PRIMARY KEY,
                                         hub_id VARCHAR(255) NOT NULL,
    name VARCHAR(255) NOT NULL,
    created_at BIGINT NOT NULL,
    UNIQUE(hub_id, name)
    );

-- Таблица условий сценариев
CREATE TABLE IF NOT EXISTS conditions (
                                          id BIGSERIAL PRIMARY KEY,
                                          scenario_id BIGINT NOT NULL REFERENCES scenarios(id) ON DELETE CASCADE,
    sensor_id VARCHAR(255) NOT NULL,
    type VARCHAR(50) NOT NULL,
    operation VARCHAR(50) NOT NULL,
    value INT NOT NULL
    );

-- Таблица действий сценариев
CREATE TABLE IF NOT EXISTS actions (
                                       id BIGSERIAL PRIMARY KEY,
                                       scenario_id BIGINT NOT NULL REFERENCES scenarios(id) ON DELETE CASCADE,
    sensor_id VARCHAR(255) NOT NULL,
    type VARCHAR(50) NOT NULL,
    value INT
    );

-- Индексы для быстрого поиска
CREATE INDEX idx_sensors_hub_id ON sensors(hub_id);
CREATE INDEX idx_scenarios_hub_id ON scenarios(hub_id);
CREATE INDEX idx_conditions_scenario_id ON conditions(scenario_id);
CREATE INDEX idx_actions_scenario_id ON actions(scenario_id);