from cassandra.cluster import Session

#---------Query de puntos por equipo-----
def get_total_points_by_team(session: Session):
    """
    Consulta que calcula la suma de puntos totales por equipo en Cassandra.
    """
    try:
        query = """
        SELECT team, SUM(total_points) AS total_points_sum
        FROM nfl_team_points
        GROUP BY team;
        """
        rows = session.execute(query)
        return [{"team": row.team, "total_points_sum": row.total_points_sum} for row in rows]
    except Exception as e:
        raise Exception(f"Error al ejecutar la consulta en Cassandra: {str(e)}")
#------------Query de Stadisticas de equipos--------------
def get_team_stats_by_season(session: Session, team: str):
    """
    Consulta que obtiene estadísticas de un equipo específico por temporada.
    """
    try:
        query = """
        SELECT season, home_wins, home_losses, away_wins, away_losses, wins, losses, ties
        FROM nfl_team_stats
        WHERE team = %s;
        """
        rows = session.execute(query, [team])
        return [
            {
                "season": row.season,
                "home_wins": row.home_wins,
                "home_losses": row.home_losses,
                "away_wins": row.away_wins,
                "away_losses": row.away_losses,
                "wins": row.wins,
                "losses": row.losses,
                "ties": row.ties,
            }
            for row in rows
        ]
    except Exception as e:
        raise Exception(f"Error al ejecutar la consulta en Cassandra: {str(e)}")   

#-------Query de yardas --------
def get_team_yards_by_season(session, team: str, season: int):
    query = """
    SELECT team, season, SUM(yards_gained) as total_yards
    FROM yards
    WHERE team = %s AND season = %s
    GROUP BY team, season;
    """
    result = session.execute(query, (team, season))
    return result
#--------Query de Touchdowns--------
def get_team_touchdowns_by_season(session, team: str, season: int):
    query = """
    SELECT team, season, 
           SUM(receiving_td) as total_receiving_td, 
           SUM(run_td) as total_run_td, 
           SUM(pass_td) as total_pass_td,
           SUM(receiving_td + run_td + pass_td) as total_touchdowns
    FROM Touchdown
    WHERE team = %s AND season = %s
    GROUP BY team, season;
    """
    result = session.execute(query, (team, season))
    return result
