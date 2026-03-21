WITH HallOfFamePlayers AS (
    SELECT p.nameFirst || ' (' || p.nameGiven || ') ' || p.nameLast AS hof_player_name,
        p.playerID
    FROM people AS p
        JOIN halloffame AS hof ON p.playerID = hof.playerID
    WHERE hof.inducted = 'Y'
)
SELECT hofp.hof_player_name AS hof_player_name,
    team_mate.teammate_name AS earliest_teammate_name,
    team_mate.teammage_year AS earliest_teammate_year
FROM HallOfFamePlayers AS hofp,
    LATERAL(
        SELECT p.nameFirst || ' (' || p.nameGiven || ') ' || p.nameLast AS teammate_name,
            a2.yearID AS teammage_year
        FROM appearances as a1 -- a1 is hall of fame player
            JOIN appearances as a2 ON a1.yearID = a2.yearID -- a2 is teammates
            AND a1.teamID = a2.teamID
            JOIN people AS p ON p.playerID = a2.playerID
        WHERE a1.playerID = hofp.playerID
            AND a1.playerID != a2.playerID
        ORDER BY teammage_year ASC,
            teammate_name ASC
        LIMIT 1
    ) AS team_mate
ORDER BY hof_player_name ASC
LIMIT 10;