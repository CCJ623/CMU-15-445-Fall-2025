WITH PlayerAwarded AS (
    SELECT a.teamID,
        a.yearID,
        a.lgID,
        COUNT(DISTINCT a.playerID) AS awards_players_count
    FROM appearances AS a
        JOIN awardsplayers AS ap ON a.playerID = ap.playerID
        AND a.yearID = ap.yearID
        AND a.lgID = ap.lgID
    GROUP BY a.teamID,
        a.yearID,
        a.lgID
    HAVING awards_players_count > 5
),
ManagerAwarded AS (
    SELECT m.teamID,
        m.yearID,
        m.lgID,
        COUNT(DISTINCT m.playerID) AS awards_managers_count
    FROM managers AS m
        JOIN awardsmanagers AS am ON m.playerID = am.playerID
        AND m.yearID = am.yearID
        AND m.lgID = am.lgID
    GROUP BY m.teamID,
        m.yearID,
        m.lgID
),
EVENT AS (
    SELECT pa.teamID,
        pa.yearID,
        pa.lgID
    FROM PlayerAwarded AS pa
        JOIN ManagerAwarded AS ma ON pa.teamID = ma.teamID
        AND pa.yearID = ma.yearID
        AND pa.lgID = ma.lgID
)
SELECT l.league AS league,
    t.name AS team_name,
    COUNT(DISTINCT e.yearID) AS distinct_years
FROM EVENT AS e
    JOIN leagues AS l ON e.lgID = l.lgID
    JOIN teams AS t ON e.teamID = t.teamID
    AND e.yearID = t.yearID
    AND e.lgID = t.lgID
WHERE l.active = 'Y'
GROUP BY l.league,
    t.name
HAVING distinct_years > 1
ORDER BY distinct_years DESC,
    t.name ASC;