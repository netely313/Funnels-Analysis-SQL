-- =============================================================================
-- TASK 1: GET UNIQUE EVENTS PER USER (FIRST OCCURRENCE ONLY)
-- =============================================================================
-- Purpose: Extract all columns from raw_events table but only for the first 
-- occurrence of each event per user (based on earliest timestamp)

SELECT e.*  -- Select all columns from the main events table
FROM raw_events e
INNER JOIN (
    -- Subquery to find the earliest timestamp for each user-event combination
    SELECT 
        user_pseudo_id,
        event_name,
        MIN(event_timestamp) as first_timestamp  -- Get the earliest occurrence
    FROM raw_events
    GROUP BY user_pseudo_id, event_name  -- Group by user and event type
) first_events
    -- Join conditions to match user, event, and exact timestamp
    ON e.user_pseudo_id = first_events.user_pseudo_id 
    AND e.event_name = first_events.event_name
    AND e.event_timestamp = first_events.first_timestamp
ORDER BY user_pseudo_id, event_name;

-- =============================================================================
-- TASK 2: FUNNEL ANALYSIS WITH CONVERSION RATES BY COUNTRY
-- =============================================================================
-- Purpose: Create a funnel overview showing user counts and conversion 
-- percentages for key events across India, Canada, US, and overall

WITH unique_user_events AS (
    -- CTE 1: Get unique user-event combinations with first occurrence timestamp
    -- This eliminates duplicate events for the same user
    SELECT DISTINCT
        user_pseudo_id,
        event_name,
        country,
        -- Window function to get first timestamp for each user-event combo
        MIN(event_timestamp) OVER (PARTITION BY user_pseudo_id, event_name) as first_event_time
    FROM raw_events
    WHERE country IN ('India', 'Canada', 'United States')  -- Filter for target countries
),

page_view_baseline AS (
    -- CTE 2: Calculate baseline users (page_view) for conversion rate calculation
    -- This serves as the denominator for percentage calculations
    
    -- Get page_view users by country
    SELECT 
        country,
        COUNT(DISTINCT user_pseudo_id) as baseline_users
    FROM unique_user_events
    WHERE event_name = 'page_view'  -- Use page_view as funnel entry point
    GROUP BY country
    
    UNION ALL
    
    -- Add overall baseline (all countries combined)
    SELECT 
        'All Countries' as country,
        COUNT(DISTINCT user_pseudo_id) as baseline_users
    FROM unique_user_events
    WHERE event_name = 'page_view'
),

event_counts AS (
    -- CTE 3: Count unique users for each event by country
    
    -- Count users by event and country
    SELECT 
        event_name,
        country,
        COUNT(DISTINCT user_pseudo_id) as event_users
    FROM unique_user_events
    WHERE event_name IN ('page_view', 'first_visit', 'scroll', 'view_item', 'add_to_cart', 'purchase')
    GROUP BY event_name, country
    
    UNION ALL
    
    -- Add overall counts (all countries combined)
    SELECT 
        event_name,
        'All Countries' as country,
        COUNT(DISTINCT user_pseudo_id) as event_users
    FROM unique_user_events
    WHERE event_name IN ('page_view', 'first_visit', 'scroll', 'view_item', 'add_to_cart', 'purchase')
    GROUP BY event_name
)

-- Final SELECT: Pivot data to show country columns and calculate conversion rates
SELECT 
    ec.event_name,
    
    -- Use conditional aggregation to pivot country data into columns
    SUM(CASE WHEN ec.country = 'India' THEN ec.event_users END) as number_event_India,
    SUM(CASE WHEN ec.country = 'Canada' THEN ec.event_users END) as number_event_Canada,
    SUM(CASE WHEN ec.country = 'United States' THEN ec.event_users END) as number_event_US,
    SUM(CASE WHEN ec.country = 'All Countries' THEN ec.event_users END) as general_number_events,
    
    -- Calculate conversion percentages (event users / page_view users * 100)
    ROUND(100.0 * SUM(CASE WHEN ec.country = 'All Countries' THEN ec.event_users END) / 
          MAX(CASE WHEN pb.country = 'All Countries' THEN pb.baseline_users END), 2) as perc_general,
    ROUND(100.0 * SUM(CASE WHEN ec.country = 'India' THEN ec.event_users END) / 
          MAX(CASE WHEN pb.country = 'India' THEN pb.baseline_users END), 2) as perc_India,
    ROUND(100.0 * SUM(CASE WHEN ec.country = 'Canada' THEN ec.event_users END) / 
          MAX(CASE WHEN pb.country = 'Canada' THEN pb.baseline_users END), 2) as perc_Canada,
    ROUND(100.0 * SUM(CASE WHEN ec.country = 'United States' THEN ec.event_users END) / 
          MAX(CASE WHEN pb.country = 'United States' THEN pb.baseline_users END), 2) as perc_US

FROM event_counts ec
CROSS JOIN page_view_baseline pb  -- Cross join to get baseline for percentage calculation
WHERE ec.event_name IN ('page_view', 'first_visit', 'scroll', 'view_item', 'add_to_cart', 'purchase')
GROUP BY ec.event_name  -- Group by event to aggregate country data
ORDER BY general_number_events DESC;  -- Order by total event volume