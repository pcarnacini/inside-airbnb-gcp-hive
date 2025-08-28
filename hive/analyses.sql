USE inside_airbnb;

-- 1. Média de preço por tipo de quarto e bairro - OK!
SELECT 
    l.room_type, 
    l.neighbourhood_cleansed, 
    ROUND(AVG(CAST(c.price AS DOUBLE)), 2) AS avg_price, 
    COUNT(DISTINCT l.id) AS total_listings
FROM listings l
JOIN calendar c ON l.id = c.listing_id
WHERE c.price IS NOT NULL 
  AND CAST(c.price AS DOUBLE) > 0
  AND l.room_type IS NOT NULL 
  AND l.neighbourhood_cleansed IS NOT NULL
GROUP BY l.room_type, l.neighbourhood_cleansed
ORDER BY avg_price DESC
LIMIT 50;

-- 2. Disponibilidade diária total e percentual - OK!
SELECT 
    calendar_date, 
    COUNT(*) AS total_listings,
    SUM(CASE WHEN available = 'true' THEN 1 ELSE 0 END) AS available_listings,
    ROUND(SUM(CASE WHEN available = 'true' THEN 1 ELSE 0 END) / COUNT(*) * 100, 2) AS pct_available
FROM calendar
WHERE calendar_date IS NOT NULL
GROUP BY calendar_date
ORDER BY calendar_date
LIMIT 30;

-- 3. Top 10 listings com mais reviews e média de preço - OK!
SELECT 
    l.id, 
    l.name, 
    COUNT(r.id) AS num_reviews,
    ROUND(AVG(CAST(c.price AS DOUBLE)), 2) AS avg_price
FROM listings l
LEFT JOIN reviews r ON l.id = r.listing_id
JOIN calendar c ON l.id = c.listing_id
WHERE c.price IS NOT NULL
GROUP BY l.id, l.name
HAVING COUNT(r.id) > 0
ORDER BY num_reviews DESC
LIMIT 10;

-- 4. Top 5 reviewers mais ativos - OK!
SELECT 
    reviewer_id, 
    reviewer_name, 
    COUNT(*) AS num_reviews,
    MIN(review_date) AS first_review, 
    MAX(review_date) AS last_review
FROM reviews
WHERE reviewer_id IS NOT NULL 
  AND review_date IS NOT NULL
GROUP BY reviewer_id, reviewer_name
ORDER BY num_reviews DESC
LIMIT 5;

-- 5. Distribuição de tipos de propriedade por bairro - OK!
SELECT 
    l.neighbourhood_cleansed,
    l.property_type,
    COUNT(DISTINCT l.id) AS total_listings
FROM listings l
WHERE l.neighbourhood_cleansed IS NOT NULL 
  AND l.neighbourhood_cleansed != ''
  AND l.property_type IS NOT NULL 
  AND l.property_type != ''
GROUP BY l.neighbourhood_cleansed, l.property_type
ORDER BY total_listings DESC
LIMIT 50;

-- 6. Preço médio por número de acomodações e tipo de quarto - OK!
SELECT 
    CAST(l.accommodates AS INT) AS accommodates_num, 
    l.room_type, 
    ROUND(AVG(CAST(c.price AS DOUBLE)), 2) AS avg_price,
    COUNT(DISTINCT l.id) AS total_listings
FROM listings l
JOIN calendar c ON l.id = c.listing_id
WHERE c.price IS NOT NULL 
  AND c.price != ''
  AND CAST(c.price AS DOUBLE) > 0
  AND l.accommodates IS NOT NULL
  AND l.accommodates != ''
  AND l.room_type IS NOT NULL
  AND l.room_type != ''
GROUP BY CAST(l.accommodates AS INT), l.room_type
ORDER BY avg_price DESC;

-- 8. Listings com poucas reviews - OK!
SELECT 
    l.id, 
    l.name, 
    CAST(l.number_of_reviews AS INT) AS total_reviews,
    ROUND(AVG(CAST(c.price AS DOUBLE)), 2) AS avg_price
FROM listings l
JOIN calendar c ON l.id = c.listing_id
WHERE l.number_of_reviews IS NOT NULL 
  AND l.number_of_reviews != ''
  AND l.name IS NOT NULL 
  AND l.name != ''
  AND c.price IS NOT NULL
  AND c.price != ''
GROUP BY l.id, l.name, l.number_of_reviews
ORDER BY total_reviews ASC
LIMIT 20;

-- 9. Estatísticas por host - OK!
SELECT 
    l.host_id,
    l.host_name,
    ROUND(AVG(CAST(c.price AS DOUBLE)), 2) AS avg_price_per_host,
    COUNT(DISTINCT l.id) AS total_listings,
    ROUND(AVG(CAST(l.review_scores_rating AS DOUBLE)), 2) AS avg_rating
FROM listings l
JOIN calendar c ON l.id = c.listing_id
WHERE l.host_id IS NOT NULL 
  AND l.host_id != ''
  AND c.price IS NOT NULL
  AND c.price != ''
  AND CAST(c.price AS DOUBLE) > 0
GROUP BY l.host_id, l.host_name
HAVING COUNT(DISTINCT l.id) > 1  -- Hosts com mais de 1 listing
ORDER BY avg_rating DESC
LIMIT 10;

-- 10. Estatísticas básicas gerais - OK!
SELECT
    COUNT(DISTINCT l.id) AS total_listings,
    ROUND(AVG(CAST(c.price AS DOUBLE)), 2) AS avg_price,
    ROUND(MIN(CAST(c.price AS DOUBLE)), 2) AS min_price,
    ROUND(MAX(CAST(c.price AS DOUBLE)), 2) AS max_price,
    COUNT(DISTINCT l.room_type) AS unique_room_types,
    COUNT(DISTINCT l.neighbourhood_cleansed) AS unique_neighbourhoods,
    COUNT(DISTINCT l.host_id) AS unique_hosts
FROM listings l
JOIN calendar c ON l.id = c.listing_id
WHERE c.price IS NOT NULL 
  AND c.price != '';

-- 11. Top bairros por preço médio - OK!
SELECT 
    l.neighbourhood_cleansed,
    COUNT(DISTINCT l.id) AS total_listings,
    ROUND(AVG(CAST(c.price AS DOUBLE)), 2) AS avg_price,
    ROUND(MIN(CAST(c.price AS DOUBLE)), 2) AS min_price,
    ROUND(MAX(CAST(c.price AS DOUBLE)), 2) AS max_price
FROM listings l
JOIN calendar c ON l.id = c.listing_id
WHERE l.neighbourhood_cleansed IS NOT NULL 
  AND l.neighbourhood_cleansed != ''
  AND c.price IS NOT NULL 
  AND c.price != ''
  AND CAST(c.price AS DOUBLE) > 0
GROUP BY l.neighbourhood_cleansed
HAVING COUNT(DISTINCT l.id) >= 5  -- Bairros com pelo menos 5 listings
ORDER BY avg_price DESC
LIMIT 20;

-- 12. Análise de reviews por mês - OK!
SELECT 
    SUBSTR(review_date, 1, 7) AS review_month,
    COUNT(*) AS total_reviews
FROM reviews
WHERE review_date IS NOT NULL 
  AND review_date RLIKE '^[0-9]{4}-[0-9]{2}-[0-9]{2}'
GROUP BY SUBSTR(review_date, 1, 7)
ORDER BY review_month DESC
LIMIT 24;