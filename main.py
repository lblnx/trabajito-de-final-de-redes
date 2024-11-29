from fastapi import FastAPI, File, UploadFile, HTTPException
from cassandra.cluster import Cluster
from pymongo import MongoClient, collection
import pydgraph
import pandas as pd
from datetime import datetime
from io import StringIO
from Querys import get_total_points_by_team
from Querys import get_team_stats_by_season
from Querys import get_team_yards_by_season
from Querys import get_team_touchdowns_by_season
#-----------Imports de Querys_Mongo------------
from Querys_Mongo import obtener_estadisticas_generales
from Querys_Mongo import obtener_estadisticas_equipos_por_temporada
from Querys_Mongo import obtener_intercepciones_por_temporada
from Querys_Mongo import obtener_pases_incompletos_por_temporada
app = FastAPI()

# ================================
# Configuración de Cassandra
# ================================
cluster = Cluster(contact_points=["127.0.0.1"])
cassandra_session = cluster.connect()
cassandra_session.set_keyspace("nfl")

# ================================
# Configuración de MongoDB
# ================================
mongo_client = MongoClient("mongodb://localhost:27017/")
mongo_db = mongo_client["nfl"]
mongo_collection_pass = mongo_db["pass"]
mongo_collection_personal_inf = mongo_db["personal_inf"]
mongo_collection_season = mongo_db["season"]
mongo_collection_stats = mongo_db["stats"]
mongo_collection_td = mongo_db["td"]
mongo_collection_yards = mongo_db["yards"]
# ================================
# Configuración de Dgraph
# ================================
def dgraph_client():
    stub = pydgraph.DgraphClientStub("127.0.0.1:9080")
    return pydgraph.DgraphClient(stub)

dgraph_client_instance = dgraph_client()
# ================================
# Ruta para Poblar Datos de pases en MongoDB
# ================================
@app.post("/poblar-pass")
async def poblar_pass(file: UploadFile = File(...)):
    try:
        # Leer el archivo CSV cargado
        df_pass = pd.read_csv(file.file)
    except Exception as e:
        return {"error": f"Error al leer el archivo CSV: {str(e)}"}

    # Validar columnas requeridas
    columnas_requeridas = [
 'player_id',
 'season',
 'pass_attempts',
 'complete_pass',
 'incomplete_pass',
 'pass_td',
 'interception',
 'receptions',
    ]

    # Asegurarse de que las columnas requeridas estén en el DataFrame
    columnas_disponibles = list(df_pass.columns)
    columnas_faltantes = [col for col in columnas_requeridas if col not in columnas_disponibles]

    if columnas_faltantes:
        return {"error": f"El archivo CSV no tiene las columnas requeridas: {columnas_faltantes}"}

    # Convertir DataFrame a lista de diccionarios
    try:
        # Filtrar las columnas disponibles en el DataFrame para que solo se incluyan las requeridas
        mongo_data = df_pass[columnas_requeridas].to_dict(orient="records")
        mongo_collection_pass.insert_many(mongo_data)
    except Exception as e:
        return {"error": f"Error al insertar datos en MongoDB: {str(e)}"}

    return {"message": "Datos de pases añadidos correctamente a MongoDB"}
#-----Estadsiticas de  intercepciones por temporada--------
@app.get("/intercepciones-temporada")
async def obtener_intercepciones(temporada: str = None):
    try:
        # Realizamos la consulta para obtener las intercepciones por temporada
        estadisticas = obtener_intercepciones_por_temporada(mongo_collection_pass)

        # Filtrar las estadísticas si se pasó el parámetro de temporada
        if temporada:
            estadisticas = [item for item in estadisticas if item["temporada"] == temporada]

        # Si no se encontraron estadísticas
        if not estadisticas:
            return {"error": "No se encontraron estadísticas de intercepciones para la temporada proporcionada."}

        return {"intercepciones_temporada": estadisticas}
    
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error al obtener las intercepciones por temporada: {str(e)}")
#-------- Estadsiticas de pases incompletos por temporada --------
@app.get("/pases-incompletos-temporada")
async def obtener_pases_incompletos(temporada: str = None):
    try:
        # Realizamos la consulta para obtener los pases incompletos por temporada
        estadisticas = obtener_pases_incompletos_por_temporada(mongo_collection_pass)

        # Filtrar las estadísticas si se pasó el parámetro de temporada
        if temporada:
            estadisticas = [item for item in estadisticas if item["temporada"] == temporada]

        # Si no se encontraron estadísticas
        if not estadisticas:
            return {"error": "No se encontraron estadísticas de pases incompletos para la temporada proporcionada."}

        return {"pases_incompletos_temporada": estadisticas}
    
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error al obtener los pases incompletos por temporada: {str(e)}")
# ================================
# Ruta para Poblar Datos de informacion personal en MongoDB
# ================================
@app.post("/poblar-personal_inf")
async def poblar_personal_inf(file: UploadFile = File(...)):
    try:
        # Leer el archivo CSV cargado
        df_personal_inf = pd.read_csv(file.file)
    except Exception as e:
        return {"error": f"Error al leer el archivo CSV: {str(e)}"}

    # Validar columnas requeridas
    columnas_requeridas_equipos = [
        'player_id',
        'player_name',
    'height',
    'weight',
    'college',
    'age'
    ]

    # Asegurarse de que las columnas requeridas estén en el DataFrame
    columnas_disponibles = list(df_personal_inf.columns)
    columnas_faltantes = [col for col in columnas_requeridas_equipos if col not in columnas_disponibles]

    if columnas_faltantes:
        return {"error": f"El archivo CSV no tiene las columnas requeridas: {columnas_faltantes}"}

    # Convertir DataFrame a lista de diccionarios
    try:
        # Filtrar las columnas disponibles en el DataFrame para que solo se incluyan las requeridas
        mongo_data = df_personal_inf[columnas_requeridas_equipos].to_dict(orient="records")
        mongo_collection_personal_inf.insert_many(mongo_data)
    except Exception as e:
        return {"error": f"Error al insertar datos en MongoDB: {str(e)}"}

    return {"message": "Datos personales añadidos correctamente a MongoDB"}
#------Query estadisticas genrales------------
@app.get("/estadisticas-generales")
async def estadisticas_generales():
    try:
        stats = obtener_estadisticas_generales(mongo_collection_personal_inf)
        if stats:
            return {"statistics": stats}
        else:
            return {"error": "No se encontraron datos para calcular estadísticas."}
    
    except Exception as e:
        return {"error": f"Error al calcular estadísticas: {str(e)}"}
# ================================
# Ruta para Poblar Datos de temporada en MongoDB
# ================================
@app.post("/poblar-season")
async def poblar_season(file: UploadFile = File(...)):
    try:
        # Leer el archivo CSV cargado
        df_season = pd.read_csv(file.file)
    except Exception as e:
        return {"error": f"Error al leer el archivo CSV: {str(e)}"}

    # Validar columnas requeridas
    columnas_requeridas_equipos = [
        'player_id',
    'season',
    'team',
    'position'
    ]

    # Asegurarse de que las columnas requeridas estén en el DataFrame
    columnas_disponibles = list(df_season.columns)
    columnas_faltantes = [col for col in columnas_requeridas_equipos if col not in columnas_disponibles]

    if columnas_faltantes:
        return {"error": f"El archivo CSV no tiene las columnas requeridas: {columnas_faltantes}"}

    # Convertir DataFrame a lista de diccionarios
    try:
        # Filtrar las columnas disponibles en el DataFrame para que solo se incluyan las requeridas
        mongo_data = df_season[columnas_requeridas_equipos].to_dict(orient="records")
        mongo_collection_season.insert_many(mongo_data)
    except Exception as e:
        return {"error": f"Error al insertar datos en MongoDB: {str(e)}"}

    return {"message": "Datos de temporadas añadidos correctamente a MongoDB"}
#----Datos de equipo por temporada-----
@app.get("/estadisticas-equipos-temporada")
async def estadisticas_equipos_temporada():
    try:
        # Llamar a la función que obtiene las estadísticas de los equipos por temporada
        stats = obtener_estadisticas_equipos_por_temporada(mongo_collection_season)

        # Si las estadísticas existen, devolverlas como respuesta
        if stats:
            return {"estadisticas": stats}
        else:
            return {"message": "No se encontraron estadísticas para los equipos por temporada."}

    except Exception as e:
        return {"error": f"Error al obtener estadísticas de equipos por temporada: {str(e)}"}

# ================================
# Ruta para Poblar datos de estadisticas en MongoDB
# ================================
@app.post("/poblar-stats")
async def poblar_stats(file: UploadFile = File(...)):
    try:
        # Leer el archivo CSV cargado
        df_stats = pd.read_csv(file.file)
    except Exception as e:
        return {"error": f"Error al leer el archivo CSV: {str(e)}"}

    # Validar columnas requeridas
    columnas_requeridas_equipos = [
        'player_id',
    'season',
    'games',
    'wins',
    'losses',
    'ties'
    ]

    # Asegurarse de que las columnas requeridas estén en el DataFrame
    columnas_disponibles = list(df_stats.columns)
    columnas_faltantes = [col for col in columnas_requeridas_equipos if col not in columnas_disponibles]

    if columnas_faltantes:
        return {"error": f"El archivo CSV no tiene las columnas requeridas: {columnas_faltantes}"}

    # Convertir DataFrame a lista de diccionarios
    try:
        # Filtrar las columnas disponibles en el DataFrame para que solo se incluyan las requeridas
        mongo_data = df_stats[columnas_requeridas_equipos].to_dict(orient="records")
        mongo_collection_stats.insert_many(mongo_data)
    except Exception as e:
        return {"error": f"Error al insertar datos en MongoDB: {str(e)}"}

    return {"message": "Datos de estadisticas añadidos correctamente a MongoDB"}

# ================================
# Ruta para Poblar touchdown en MongoDB
# ================================
@app.post("/poblar-td")
async def poblar_td(file: UploadFile = File(...)):
    try:
        # Leer el archivo CSV cargado
        df_td = pd.read_csv(file.file)
    except Exception as e:
        return {"error": f"Error al leer el archivo CSV: {str(e)}"}

    # Validar columnas requeridas
    columnas_requeridas_equipos = [
        'player_id',
    'season',
    'pass_td',
    'total_tds',
    'reception_td'
    ]

    # Asegurarse de que las columnas requeridas estén en el DataFrame
    columnas_disponibles = list(df_td.columns)
    columnas_faltantes = [col for col in columnas_requeridas_equipos if col not in columnas_disponibles]

    if columnas_faltantes:
        return {"error": f"El archivo CSV no tiene las columnas requeridas: {columnas_faltantes}"}

    # Convertir DataFrame a lista de diccionarios
    try:
        # Filtrar las columnas disponibles en el DataFrame para que solo se incluyan las requeridas
        mongo_data = df_td[columnas_requeridas_equipos].to_dict(orient="records")
        mongo_collection_td.insert_many(mongo_data)
    except Exception as e:
        return {"error": f"Error al insertar datos en MongoDB: {str(e)}"}

    return {"message": "Datos de td añadidos correctamente a MongoDB"}
# ================================
# Ruta para Poblar yardas en MongoDB
# ================================
@app.post("/poblar-yards")
async def poblar_yards(file: UploadFile = File(...)):
    try:
        # Leer el archivo CSV cargado
        df_yards = pd.read_csv(file.file)
    except Exception as e:
        return {"error": f"Error al leer el archivo CSV: {str(e)}"}

    # Validar columnas requeridas
    columnas_requeridas_equipos = [
        'player_id',
    'season',
    'total_yards',
    'receiving_air_yards',
    'yards_after_catch',
    'rushing_yards',
    'passing_yards',
    'passing_air_yards',
    'ypg'
    ]

    # Asegurarse de que las columnas requeridas estén en el DataFrame
    columnas_disponibles = list(df_yards.columns)
    columnas_faltantes = [col for col in columnas_requeridas_equipos if col not in columnas_disponibles]

    if columnas_faltantes:
        return {"error": f"El archivo CSV no tiene las columnas requeridas: {columnas_faltantes}"}

    # Convertir DataFrame a lista de diccionarios
    try:
        # Filtrar las columnas disponibles en el DataFrame para que solo se incluyan las requeridas
        mongo_data = df_yards[columnas_requeridas_equipos].to_dict(orient="records")
        mongo_collection_yards.insert_many(mongo_data)
    except Exception as e:
        return {"error": f"Error al insertar datos en MongoDB: {str(e)}"}

    return {"message": "Datos de yards añadidos correctamente a MongoDB"}
# ================================
# Ruta para Poblar Datos de pases en Cassandra
# ================================
@app.post("/poblar-passes")
async def poblar_passes(file: UploadFile = File(...)):
    try:
        # Leer el archivo CSV cargado
        contents = await file.read()
        csv_data = StringIO(contents.decode("utf-8"))
        df_passes_1 = pd.read_csv(csv_data)
    except Exception as e:
        return {"error": f"Error al leer el archivo CSV: {str(e)}"}

    # Validar columnas requeridas
    columnas_requeridas = [
        'team',
        'season',
        'pass_attempts',
        'complete_pass',
        'incomplete_pass',
        'interception',
        'receptions',
    ]
    columnas_faltantes = [col for col in columnas_requeridas if col not in df_passes_1.columns]

    if columnas_faltantes:
        return {"error": f"El archivo CSV no tiene las columnas requeridas: {columnas_faltantes}"}

    # Crear la tabla si no existe
    create_table_query = """
    CREATE TABLE IF NOT EXISTS nfl_passes (
        team TEXT,
        season INT,
        pass_attempts INT,
        incomplete_pass INT,
        interception INT,
        receptions INT,
        PRIMARY KEY (team, season)
    );
    """
    try:
        cassandra_session.execute(create_table_query)
    except Exception as e:
        return {"error": f"Error al crear la tabla en Cassandra: {str(e)}"}

    # Insertar datos en Cassandra
    try:
        for _, row in df_passes_1.iterrows():
            insert_query = """
            INSERT INTO nfl_passes (team, season, pass_attempts, incomplete_pass, interception, receptions)
            VALUES (%s, %s, %s, %s, %s, %s);
            """
            cassandra_session.execute(insert_query, (
                row['team'],
                int(row['season']),
                int(row['pass_attempts']),
                int(row['incomplete_pass']),
                int(row['interception']),
                int(row['receptions']),
            ))
    except Exception as e:
        return {"error": f"Error al insertar datos en Cassandra: {str(e)}"}

    return {"message": "Datos de pases añadidos correctamente a Cassandra"}
# ================================
# Ruta para Poblar puntos en Cassandra
# ================================
@app.post("/poblacion-points")
async def poblacion_points(file: UploadFile = File(...)):
    try:
        # Leer el archivo CSV cargado
        contents = await file.read()
        csv_data = StringIO(contents.decode("utf-8"))
        df_points = pd.read_csv(csv_data)
    except Exception as e:
        return {"error": f"Error al leer el archivo CSV: {str(e)}"}

    # Validar columnas requeridas
    columnas_requeridas = [
        'team',
        'season',
        'total_points',
        'td_points',
        'xp_points',
        'fg_points',
    ]
    columnas_faltantes = [col for col in columnas_requeridas if col not in df_points.columns]

    if columnas_faltantes:
        return {"error": f"El archivo CSV no tiene las columnas requeridas: {columnas_faltantes}"}

    # Crear la tabla si no existe
    create_table_query = """
    CREATE TABLE IF NOT EXISTS nfl_team_points (
        team TEXT,
        season TEXT,
        td_points INT,
        xp_points INT,
        fg_points INT,
        total_points INT,
        PRIMARY KEY (team, season) -- Partition Key: team, Clustering Key: season

    );
    """
    try:
        cassandra_session.execute(create_table_query)
    except Exception as e:
        return {"error": f"Error al crear la tabla en Cassandra: {str(e)}"}    

    # Insertar datos en Cassandra
    try:
        for _, row in df_points.iterrows():
            insert_query = """
            INSERT INTO nfl_team_points (team, season, total_points, td_points, xp_points, fg_points)
            VALUES (%s, %s, %s, %s, %s, %s);
            """
            cassandra_session.execute(insert_query, (
                row['team'],
                str(row['season']),
                int(row['total_points']),
                int(row['td_points']),
                int(row['xp_points']),
                int(row['fg_points']),
            ))
    except Exception as e:
        return {"error": f"Error al insertar datos en Cassandra: {str(e)}"}

    return {"message": "Datos de puntos añadidos correctamente a Cassandra"}
#Query Cassandra points
@app.get("/puntos-totales-por-equipo")
async def puntos_totales_por_equipo():
    try:
        resultados = get_total_points_by_team(cassandra_session)
    except Exception as e:
        return {"error": str(e)}

    return {"data": resultados}


# ================================
# Ruta para Poblar stats en Cassandra
# ================================
@app.post("/poblacion-stats")
async def poblacion_stats(file: UploadFile = File(...)):
    try:
        # Leer el archivo CSV cargado
        contents = await file.read()
        csv_data = StringIO(contents.decode("utf-8"))
        df_stats = pd.read_csv(csv_data)
    except Exception as e:
        return {"error": f"Error al leer el archivo CSV: {str(e)}"}

    # Validar columnas requeridas
    columnas_requeridas = [
        'team',
        'season',
        'home_wins',
        'home_losses',
        'home_ties',
        'away_wins',
        'away_losses',
        'away_ties',
        'wins',
        'losses',
        'ties'
    ]
    columnas_faltantes = [col for col in columnas_requeridas if col not in df_stats.columns]

    if columnas_faltantes:
        return {"error": f"El archivo CSV no tiene las columnas requeridas: {columnas_faltantes}"}

    # Crear la tabla si no existe
    create_table_query = """
    CREATE TABLE IF NOT EXISTS nfl_team_stats (
        team TEXT,
        season TEXT,
        home_wins INT,
        home_losses INT,
        home_ties INT,
        away_wins INT,
        away_losses INT,
        away_ties INT,
        wins INT,
        losses INT,
        ties INT,
        PRIMARY KEY (team, season) -- Partition Key: team, Clustering Key: season

    );
    """
    try:
        cassandra_session.execute(create_table_query)
    except Exception as e:
        return {"error": f"Error al crear la tabla en Cassandra: {str(e)}"}

    # Insertar datos en Cassandra
    try:
        for _, row in df_stats.iterrows():
            insert_query = """
            INSERT INTO nfl_team_stats (team, season, home_wins, home_losses, home_ties, away_wins, away_losses, away_ties, wins, losses, ties)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);
            """
            cassandra_session.execute(insert_query, (
                row['team'],
                str(row['season']),
                int(row['home_wins']),
                int(row['home_losses']),
                int(row['home_ties']),
                int(row['away_wins']),
                int(row['away_losses']),
                int(row['away_ties']),
                int(row['wins']),
                int(row['losses']),
                int(row['ties']),
            ))
    except Exception as e:
        return {"error": f"Error al insertar datos en Cassandra: {str(e)}"}

    return {"message": "Datos de estadisticas añadidos correctamente a Cassandra"}
#query
@app.get("/stats-por-equipo/{team}")
async def stats_por_equipo(team: str):
    """
    Ruta para obtener estadísticas de un equipo específico por temporada.
    """
    try:
        resultados = get_team_stats_by_season(cassandra_session, team)
    except Exception as e:
        return {"error": str(e)}

    if not resultados:
        return {"message": f"No se encontraron estadísticas para el equipo {team}"}

    return {"data": resultados}
    
# ================================
# Ruta para Poblar touchdown en Cassandra
# ================================
@app.post("/poblacion-touchdown")
async def poblacion_touchdown(file: UploadFile = File(...)):
    try:
        # Leer el archivo CSV cargado
        contents = await file.read()
        csv_data = StringIO(contents.decode("utf-8"))
        df_touchdown = pd.read_csv(csv_data)
    except Exception as e:
        return {"error": f"Error al leer el archivo CSV: {str(e)}"}

    # Validar columnas requeridas
    columnas_requeridas = [
        'team',
        'season',
        'receiving_td',
        'run_td',
        'pass_td',
        'field_goal_attempt',
        'extra_point_attempt'
    ]
    columnas_faltantes = [col for col in columnas_requeridas if col not in df_touchdown.columns]

    if columnas_faltantes:
        return {"error": f"El archivo CSV no tiene las columnas requeridas: {columnas_faltantes}"}

    # Crear la tabla si no existe
    create_table_query = """
    CREATE TABLE IF NOT EXISTS Touchdown (
        team TEXT,
        season INT,
        receiving_td INT,
        run_td INT,
        pass_td INT,
        field_goal_attempt INT,
        extra_point_attempt INT,
        PRIMARY KEY (team, season) -- Partition Key: team, Clustering Key: season
    );
    """
    try:
        cassandra_session.execute(create_table_query)
    except Exception as e:
        return {"error": f"Error al crear la tabla en Cassandra: {str(e)}"}

    # Insertar datos en Cassandra
    try:
        for _, row in df_touchdown.iterrows():
            insert_query = """
            INSERT INTO Touchdown (team, season, receiving_td, run_td, pass_td, field_goal_attempt, extra_point_attempt)
            VALUES (%s, %s, %s, %s, %s, %s, %s);
            """
            cassandra_session.execute(insert_query, (
                row['team'],
                int(row['season']),
                int(row['receiving_td']),
                int(row['run_td']),
                int(row['pass_td']),
                int(row['field_goal_attempt']),
                int(row['extra_point_attempt']),
            ))
    except Exception as e:
        return {"error": f"Error al insertar datos en Cassandra: {str(e)}"}

    return {"message": "Datos de touchdowns añadidos correctamente a Cassandra"}
# Ruta para obtener estadísticas de touchdowns por equipo y temporada
@app.get("/get-team-touchdowns")
async def get_team_touchdowns(team: str, season: int):
    try:
        # Llamar a la función de consulta
        result = get_team_touchdowns_by_season(cassandra_session, team, season)
        
        # Verificar si hay resultados
        if result:
            return {
                "team": team,
                "season": season,
                "total_receiving_td": result[0].total_receiving_td,
                "total_run_td": result[0].total_run_td,
                "total_pass_td": result[0].total_pass_td,
                "total_touchdowns": result[0].total_touchdowns
            }
        else:
            return {"message": "No se encontraron datos para el equipo y la temporada especificados."}
    except Exception as e:
        return {"error": f"Error al ejecutar la consulta: {str(e)}"}

# ================================
# Ruta para Poblar equipos en Cassandra
# ================================
@app.post("/poblacion-team")
async def poblacion_team(file: UploadFile = File(...)):
    try:
        # Leer el archivo CSV cargado
        contents = await file.read()
        csv_data = StringIO(contents.decode("utf-8"))
        df_team = pd.read_csv(csv_data)
    except Exception as e:
        return {"error": f"Error al leer el archivo CSV: {str(e)}"}

    # Validar columnas requeridas
    columnas_requeridas = [
        'team',
        'season'
    ]
    columnas_faltantes = [col for col in columnas_requeridas if col not in df_team.columns]

    if columnas_faltantes:
        return {"error": f"El archivo CSV no tiene las columnas requeridas: {columnas_faltantes}"}

    # Crear la tabla si no existe
    create_table_query = """
    CREATE TABLE IF NOT EXISTS Team (
        team TEXT,
        season INT,
        PRIMARY KEY (team, season) -- Partition Key: team, Clustering Key: season
    );
    """
    try:
        cassandra_session.execute(create_table_query)
    except Exception as e:
        return {"error": f"Error al crear la tabla en Cassandra: {str(e)}"}

    # Insertar datos en Cassandra
    try:
        for _, row in df_team.iterrows():
            insert_query = """
            INSERT INTO Team (team, season)
            VALUES (%s, %s);
            """
            cassandra_session.execute(insert_query, (
                row['team'],
                int(row['season'])
            ))
    except Exception as e:
        return {"error": f"Error al insertar datos en Cassandra: {str(e)}"}

    return {"message": "Datos de Team añadidos correctamente a Cassandra"}
# ================================
# Ruta para Poblar yards en Cassandra
# ================================
@app.post("/poblacion-yards")
async def poblacion_yards(file: UploadFile = File(...)):
    try:
        # Leer el archivo CSV cargado
        contents = await file.read()
        csv_data = StringIO(contents.decode("utf-8"))
        df_yards = pd.read_csv(csv_data)
    except Exception as e:
        return {"error": f"Error al leer el archivo CSV: {str(e)}"}

    # Validar columnas requeridas
    columnas_requeridas = [
        'team',
        'season',
        'yps',
        'air_yards',
        'passing_yards',
        'receiving_yards',
        'yards_after_catch',
        'rushing_yards',
        'yards_gained'
    ]
    columnas_faltantes = [col for col in columnas_requeridas if col not in df_yards.columns]

    if columnas_faltantes:
        return {"error": f"El archivo CSV no tiene las columnas requeridas: {columnas_faltantes}"}

    # Crear la tabla si no existe
    create_table_query = """
    CREATE TABLE IF NOT EXISTS Yards (
        team TEXT,
        season INT,
        yps FLOAT,
        air_yards INT,
        passing_yards INT,
        receiving_yards INT,
        yards_after_catch INT,
        rushing_yards INT,
        yards_gained INT,
        PRIMARY KEY (team, season) -- Partition Key: team, Clustering Key: season
    );
    """
    try:
        cassandra_session.execute(create_table_query)
    except Exception as e:
        return {"error": f"Error al crear la tabla en Cassandra: {str(e)}"}

    # Insertar datos en Cassandra
    try:
        for _, row in df_yards.iterrows():
            insert_query = """
            INSERT INTO Yards (team, season, yps, air_yards, passing_yards, receiving_yards, 
                               yards_after_catch, rushing_yards, yards_gained)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s);
            """
            cassandra_session.execute(insert_query, (
                row['team'],
                int(row['season']),
                float(row['yps']),
                int(row['air_yards']),
                int(row['passing_yards']),
                int(row['receiving_yards']),
                int(row['yards_after_catch']),
                int(row['rushing_yards']),
                int(row['yards_gained'])
            ))
    except Exception as e:
        return {"error": f"Error al insertar datos en Cassandra: {str(e)}"}

    return {"message": "Datos de Yards añadidos correctamente a Cassandra"}
@app.get("/get-team-yards")
async def get_team_yards(team: str, season: str):
    try:
        # Llamar a la función de la consulta
        result = get_team_yards_by_season(cassandra_session, team, season)
        
        # Verificar si hay resultados
        if result:
            return {"team": team, "season": season, "total_yards": result[0].total_yards}
        else:
            return {"message": "No se encontraron datos para el equipo y la temporada especificados."}
    except Exception as e:
        return {"error": f"Error al ejecutar la consulta: {str(e)}"}