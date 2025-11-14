-- Витрина данных для анализа зарплат IT в Европе
-- Вариант задания №21
DROP VIEW IF EXISTS it_salary_datamart;

CREATE VIEW it_salary_datamart AS
SELECT 
    city,
    position,
    experience_years,
    main_technology,
    salary_eur
FROM stg_it_salary
WHERE 
    city IS NOT NULL 
    AND position IS NOT NULL
    AND experience_years IS NOT NULL
    AND main_technology IS NOT NULL
    AND salary_eur IS NOT NULL;

-- Комментарий
COMMENT ON VIEW it_salary_datamart IS 
'Аналитическая витрина зарплат IT-специалистов в Европе. Готова для дашборда в Superset.';