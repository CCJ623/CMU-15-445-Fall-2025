INSERT INTO teams (
        yearID,
        lgID,
        teamID,
        franchID,
        teamRank,
        attendance
    )
SELECT t.yearID,
    t.lgID,
    t.franchID,
    NULL,
    t.teamRank,
    t.attendance
FROM teams AS t
WHERE t.yearID = 2024 
ON CONFLICT (yearID, lgID, teamID) DO
UPDATE
SET attendance = -1;
