DROP VIEW IF EXISTS closeness;
CREATE VIEW closeness AS 
	SELECT *
	,CASE WHEN delta >= 0 THEN True
	ELSE False
	END AS winning
	,CASE WHEN time_remaining IS NULL THEN NULL
	WHEN game_over = True THEN NULL
	ELSE ROUND(1/SQRT((ABS(delta^5)/100000) * ((time_remaining/100) +.1) + 1), 2) 
	END AS closeness	

FROM 
	(SELECT g.id
	 	,COUNT(l.bet_id) AS bet_count
		,g.away_team
		,g.away_score
		,g.home_team
		,g.home_score
	 	,g.game_over
	 	,g.game_start
		,g.time_remaining
		,l.team
	 	,l.won
		,l.points
	 	,l.wager_type
		,CASE WHEN l.wager_type = 'over' THEN (g.away_score + g.home_score) - l.points
	 	WHEN l.wager_type = 'under' THEN l.points - (g.away_score + g.home_score)
	 	WHEN l.team = g.away_team THEN l.points + (g.away_score - g.home_score)
		WHEN l.team = g.home_team THEN l.points + (g.home_score - g.away_score)
		ELSE g.away_score + g.home_score
		END AS delta
		,SUM(o.risk) as risk
		,SUM(o.win) as win
	FROM bet_lines l
	LEFT OUTER JOIN
		games g
	ON g.id = l.game_id
	LEFT OUTER JOIN
		open_bets o
	ON l.bet_id = o.id

	GROUP BY g.id
		,g.away_team
		,g.away_score
		,g.home_team
		,g.home_score
	 	,g.time_remaining
	 	,g.game_start
	 	,g.game_over
	 	,l.won
		,l.team
		,l.points
	 	,l.wager_type
		,CASE WHEN l.team = g.away_team THEN l.points + (g.away_score - g.home_score)
		WHEN l.team = g.home_team THEN l.points + (g.home_score - g.away_score)
		ELSE g.away_score + g.home_score END
	 ) AS bet_line_info
WHERE game_over = False and id <> 999 and game_start < CURRENT_TIMESTAMP


