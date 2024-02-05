-- Selects user sessions with interactions by month
SELECT 
    userId AS `userId`,
    `Feb 2023`,
    `Mar 2023`,
    `Apr 2023`,
    `May 2023`,
    `June 2023`,
    `July 2023`,
    `Aug 2023`,
    `Sep 2023`,
    `Oct 2023`,
    `Nov 2023`,
    `Dec 2023`,
    `Jan 2024`,
    `Feb 2024`
FROM (
    -- Subquery to calculate monthly sessions with interactions for each user
    SELECT 
        userId,
        sumIf(`Sessions With Interactions`, month = toDateTime('2023-02-01')) AS `Feb 2023`,
        sumIf(`Sessions With Interactions`, month = toDateTime('2023-03-01')) AS `Mar 2023`,
        -- Continue for each month up to Feb 2024
        sumIf(`Sessions With Interactions`, month = toDateTime('2024-02-01')) AS `Feb 2024`
    FROM (
        -- Subquery to aggregate data by month and userId, focusing on interactions
        SELECT 
            toStartOfMonth(toDateTime(`ts`)) AS `month`,
            userId,
            countIf(DISTINCT sessionId, eventType LIKE '%click%') AS `Sessions With Interactions`
        FROM (
            -- Base data selection, joining with creator registrations
            SELECT 
                userId,
                sessionId,
                ts,
                eventType
            FROM analytics.creator a
            LEFT JOIN (
                -- Joining to filter for registered creators only
                SELECT userId
                FROM work.creator_registrations
            ) AS b ON a.userId = b.userId
            WHERE 
                ts::DATE >= '2023-02-01' AND
                ts::DATE <= '2024-02-28' AND
                eventType LIKE '%click%'
        ) AS `virtual_table`
        GROUP BY 
            userId,
            `month`
        HAVING 
            countIf(DISTINCT sessionId, eventType LIKE '%click%') > 0
    ) 
    GROUP BY userId
) AS `virtual_table`
LIMIT 1000;
