SELECT p.nameGiven,
    a.teamID,
    COUNT(ap.yearID) AS award_year_count
FROM people AS p
    JOIN appearances AS a ON p.playerID = a.playerID
    JOIN awardsplayers AS ap ON a.playerID = ap.playerID
    AND a.yearID = ap.yearID
    JOIN leagues AS l ON a.lgID = l.lgID
WHERE ap.awardID = 'Gold Glove'
    AND ap.yearID > 1999
    AND l.active = 'Y'
    AND a.G_batting > (
        -- average batting of a team since 1999
        SELECT AVG(inner_a.G_batting)
        FROM appearances as inner_a
        WHERE inner_a.teamID = a.teamID
            AND inner_a.yearID > 1999
    )
GROUP BY p.nameGiven,
    a.teamID
ORDER BY award_year_count DESC,
    p.nameGiven ASC
LIMIT 10;