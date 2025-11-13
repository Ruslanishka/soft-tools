-- Создание базы данных для анализа фермерских хозяйств
-- Задание 21: Фермерские хозяйства, Урожай, Цены — расчёт прогнозируемого дохода

-- Таблица фермерских хозяйств
CREATE TABLE IF NOT EXISTS farms (
    farm_id VARCHAR(10) PRIMARY KEY,
    farm_name VARCHAR(150) NOT NULL,
    crop_type VARCHAR(50) NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Таблица урожая
CREATE TABLE IF NOT EXISTS harvest (
    farm_id VARCHAR(10) REFERENCES farms(farm_id),
    harvest_tonnes NUMERIC(10, 2) CHECK (harvest_tonnes >= 0),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Таблица цен на рынке
CREATE TABLE IF NOT EXISTS market_prices (
    crop_type VARCHAR(50) PRIMARY KEY,
    price_per_tonne NUMERIC(12, 2) CHECK (price_per_tonne >= 0),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Индексы для оптимизации запросов
CREATE INDEX IF NOT EXISTS idx_harvest_farm_id ON harvest(farm_id);
CREATE INDEX IF NOT EXISTS idx_farms_crop_type ON farms(crop_type);

-- Представление для расчёта прогнозируемого дохода
CREATE OR REPLACE VIEW farm_predicted_income AS
SELECT 
    f.farm_id,
    f.farm_name,
    f.crop_type,
    h.harvest_tonnes,
    mp.price_per_tonne,
    (h.harvest_tonnes * mp.price_per_tonne) AS predicted_income
FROM farms f
LEFT JOIN harvest h ON f.farm_id = h.farm_id
LEFT JOIN market_prices mp ON f.crop_type = mp.crop_type
WHERE h.harvest_tonnes IS NOT NULL 
  AND mp.price_per_tonne IS NOT NULL
ORDER BY predicted_income DESC NULLS LAST;