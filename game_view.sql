SELECT *
	,CASE WHEN delta >= 0 THEN True
	ELSE False
	END AS winning
	, --CASE WHEN time_remaining = NULL THEN NULL
-- 	WHEN game_over = True THEN NULL
--	ELSE 
	ROUND(1/(((ABS(delta^3)/(200 + delta^2)) * SQRT(time_remaining+.1))+(1+time_remaining)), 2) 
	AS closeness
-- 	delta 	0 -> 100
-- 			∞ -> 0
-- 	time_remaining
-- 			5 -> unchanged
-- 			60 -> /5
	
	

FROM 
	(SELECT g.id
	 	,COUNT(l.bet_id) AS bet_count
		,g.away_team
		,g.away_score
		,g.home_team
		,g.home_score
	 	,g.game_over
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
	 	,g.time_remaining
	 	,g.game_over
	 	,l.won
		,l.team
		,l.points
		,CASE WHEN l.team = g.away_team THEN l.points + (g.away_score - g.home_score)
		WHEN l.team = g.home_team THEN l.points + (g.home_score - g.away_score)
		ELSE g.away_score + g.home_score END
	 ) AS bet_line_info
ORDER BY ABS(delta)

