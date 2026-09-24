-- Проверочные запросы к модели «звезда» в PostgreSQL.

-- Качество загрузки: ожидается source_rows = fact_rows = 10000, missing_rows = 0.
SELECT * FROM view_load_quality;

-- Число строк по таблицам.
SELECT * FROM view_row_counts ORDER BY table_name;

-- Топ-10 товаров по количеству продаж (проверка звезды напрямую в PostgreSQL).
SELECT p.product_name, p.category,
       SUM(f.quantity)             AS items,
       ROUND(SUM(f.total_price),2) AS revenue
FROM fact_sales f
JOIN dim_product p ON p.product_id = f.product_id
GROUP BY p.product_name, p.category
ORDER BY items DESC
LIMIT 10;
