SELECT p.nameFirst || ' (' || p.nameGiven || ') ' || p.nameLast AS name,
    MAX(a.HR) AS max_hr_appearance
FROM appearances AS a
    JOIN people AS p ON a.playerID == p.playerID
WHERE a.playerID IN (
        SELECT cp.playerID
        FROM collegeplaying AS cp
            JOIN schools AS s ON cp.schoolID = s.schoolID
        WHERE s.state = 'PA'
    )
GROUP BY a.playerID,
    p.nameFirst,
    p.nameLast,
    p.nameGiven
ORDER BY max_hr_appearance DESC,
    p.nameFirst ASC
LIMIT 10;