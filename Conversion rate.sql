1. Первый варинат.

WITH funnel_stages AS (
    SELECT 
        'page_view' as stage,
        269792 as count,
        1 as stage_order
    UNION ALL SELECT 'first_visit', 257314, 2
    UNION ALL SELECT 'scroll', 138098, 3
    UNION ALL SELECT 'view_item', 61252, 4
    UNION ALL SELECT 'add_to_cart', 12545, 5
    UNION ALL SELECT 'purchase', 4419, 6
),
conversion_rates AS (
    SELECT 
        curr.stage,
        curr.count as current_count,
        prev.count as previous_count,
        ROUND(CAST(curr.count AS FLOAT) / 
              CAST(prev.count AS FLOAT) * 100, 2) as conversion_from_previous,
        ROUND(CAST(curr.count AS FLOAT) / 
              CAST(FIRST_VALUE(curr.count) OVER (ORDER BY curr.stage_order) AS FLOAT) * 100, 2) as conversion_from_start
    FROM funnel_stages curr
    LEFT JOIN funnel_stages prev 
        ON curr.stage_order = prev.stage_order + 1
)
SELECT 
    stage,
    current_count,
    previous_count,
    conversion_from_previous as conversion_rate_pct,
    100 - conversion_from_previous as drop_off_rate_pct,
    conversion_from_start as total_conversion_pct
FROM conversion_rates
WHERE previous_count IS NOT NULL
ORDER BY stage;

Этот запрос:

Создает временную таблицу с данными о каждом этапе воронки
Рассчитывает:

Конверсию между последовательными этапами
Процент отсева на каждом этапе
Общую конверсию от начала воронки



Результат будет выглядеть примерно так:

stage        | current_count | previous_count | conversion_rate_pct | drop_off_rate_pct | total_conversion_pct
-------------|--------------|----------------|-------------------|------------------|--------------------
first_visit  | 257314       | 269792        | 95.40            | 4.60             | 95.40
scroll       | 138098       | 257314        | 53.70            | 46.30            | 51.20
view_item    | 61252        | 138098        | 44.40            | 55.60            | 22.70
add_to_cart  | 12545        | 61252         | 20.50            | 79.50            | 4.65
purchase     | 4419         | 12545         | 35.20            | 64.80            | 1.70


2. Второй вариант.
WITH funnel_steps AS (
    -- Подсчет количества событий для каждого шага воронки
    SELECT 
        COUNT(DISTINCT CASE WHEN event_type = 'page_view' THEN user_id END) as page_views,
        COUNT(DISTINCT CASE WHEN event_type = 'first_visit' THEN user_id END) as first_visits,
        COUNT(DISTINCT CASE WHEN event_type = 'scroll' THEN user_id END) as scrolls,
        COUNT(DISTINCT CASE WHEN event_type = 'view_item' THEN user_id END) as item_views,
        COUNT(DISTINCT CASE WHEN event_type = 'add_to_cart' THEN user_id END) as cart_adds,
        COUNT(DISTINCT CASE WHEN event_type = 'purchase' THEN user_id END) as purchases
    FROM user_events
    WHERE date_trunc('month', event_date) = '2024-01-01' -- Предполагаем, что данные за январь 2024
),
conversion_rates AS (
    -- Расчет конверсии между последовательными шагами
    SELECT
        'Page View → First Visit' as step,
        ROUND(CAST(first_visits AS FLOAT) / NULLIF(page_views, 0) * 100, 1) as conversion_rate,
        page_views as total_users,
        first_visits as converted_users,
        page_views - first_visits as dropped_users
    FROM funnel_steps
    
    UNION ALL
    
    SELECT
        'First Visit → Scroll' as step,
        ROUND(CAST(scrolls AS FLOAT) / NULLIF(first_visits, 0) * 100, 1) as conversion_rate,
        first_visits as total_users,
        scrolls as converted_users,
        first_visits - scrolls as dropped_users
    FROM funnel_steps
    
    UNION ALL
    
    SELECT
        'Scroll → View Item' as step,
        ROUND(CAST(item_views AS FLOAT) / NULLIF(scrolls, 0) * 100, 1) as conversion_rate,
        scrolls as total_users,
        item_views as converted_users,
        scrolls - item_views as dropped_users
    FROM funnel_steps
    
    UNION ALL
    
    SELECT
        'View Item → Add to Cart' as step,
        ROUND(CAST(cart_adds AS FLOAT) / NULLIF(item_views, 0) * 100, 1) as conversion_rate,
        item_views as total_users,
        cart_adds as converted_users,
        item_views - cart_adds as dropped_users
    FROM funnel_steps
    
    UNION ALL
    
    SELECT
        'Add to Cart → Purchase' as step,
        ROUND(CAST(purchases AS FLOAT) / NULLIF(cart_adds, 0) * 100, 1) as conversion_rate,
        cart_adds as total_users,
        purchases as converted_users,
        cart_adds - purchases as dropped_users
    FROM funnel_steps
)
-- Финальная выборка с общей конверсией
SELECT 
    step,
    conversion_rate,
    total_users,
    converted_users,
    dropped_users,
    ROUND(CAST(converted_users AS FLOAT) / 
        (SELECT NULLIF(page_views, 0) FROM funnel_steps) * 100, 1) as total_funnel_conversion
FROM conversion_rates
ORDER BY total_funnel_conversion DESC;

Этот SQL-код:

1. Создает CTE `funnel_steps` для подсчета уникальных пользователей на каждом этапе воронки

2. Создает CTE `conversion_rates` для расчета:
   - Конверсии между последовательными шагами
   - Количества пользователей на каждом этапе
   - Количества отвалившихся пользователей

3. В финальной выборке считает:
   - Конверсию между шагами
   - Абсолютные значения для каждого шага
   - Общую конверсию от начала воронки

Код предполагает наличие таблицы `user_events` со столбцами:
- user_id
- event_type
- event_date

Результатом будет таблица с полным анализом воронки, 
включая все проценты конверсии и абсолютные значения на каждом этапе.
