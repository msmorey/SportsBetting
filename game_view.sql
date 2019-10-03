SELECT *
	,CASE WHEN delta >= 0 THEN True
	ELSE False
	END AS winning
	,CASE WHEN time_remaining = NULL THEN NULL
	ELSE 1 - ABS((delta/(risk+win))
	

FROM 
	(SELECT g.id
	 	,COUNT(l.bet_id) AS bet_count
		,g.away_team
		,g.away_score
		,g.home_team
		,g.home_score
		,g.time_remaining
		,l.team
	 	,l.won
		,l.points
		,CASE WHEN l.team = g.away_team THEN l.points + (g.away_score - g.home_score)
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
	 	,l.won
		,l.team
		,l.points
		,CASE WHEN l.team = g.away_team THEN l.points + (g.away_score - g.home_score)
		WHEN l.team = g.home_team THEN l.points + (g.home_score - g.away_score)
		ELSE g.away_score + g.home_score END
	 ) AS bet_line_info
ORDER BY winning

