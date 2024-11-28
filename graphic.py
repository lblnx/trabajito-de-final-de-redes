import httpx
import os

# Ruta del servidor local
BASE_URL = "http://127.0.0.1:8000"

# Función para poblar los datos
def poblar_passes():
    url = f"{BASE_URL}/poblar-passes"
    file_path = "cvs/Cassandra/Cassandra_passes.csv"  # Ruta predefinida
    with open(file_path, "rb") as f:
        files = {"file": f}
        response = httpx.post(url, files=files)
    return response.json()

def poblar_team_points():
    url = f"{BASE_URL}/poblacion-points"
    file_path = "cvs/Cassandra/Cassandra_points.csv"  # Ruta predefinida
    with open(file_path, "rb") as f:
        files = {"file": f}
        response = httpx.post(url, files=files)
    return response.json()

def poblar_team_stats():
    url = f"{BASE_URL}/poblacion-stats"
    file_path = "cvs/Cassandra/Cassandra_stats.csv"  # Ruta predefinida
    with open(file_path, "rb") as f:
        files = {"file": f}
        response = httpx.post(url, files=files)
    return response.json()

def poblar_touchdown():
    url = f"{BASE_URL}/poblacion-touchdown"
    file_path = "cvs/Cassandra/Cassandra_td.csv"  # Ruta predefinida
    with open(file_path, "rb") as f:
        files = {"file": f}
        response = httpx.post(url, files=files)
    return response.json()

def poblar_team():
    url = f"{BASE_URL}/poblacion-team"
    file_path = "cvs/Cassandra/Cassandra_team.csv"  # Ruta predefinida
    with open(file_path, "rb") as f:
        files = {"file": f}
        response = httpx.post(url, files=files)
    return response.json()

def poblar_yards():
    url = f"{BASE_URL}/poblacion-yards"
    file_path = "cvs/Cassandra/Cassandra_yards.csv"  # Ruta predefinida
    with open(file_path, "rb") as f:
        files = {"file": f}
        response = httpx.post(url, files=files)
    return response.json()

def borrar_datos(tabla, team=None, season=None):
    url = f"{BASE_URL}/borrar-datos?tabla={tabla}"
    
    # Si se proporciona team y season, los agregamos a la URL
    if team and season:
        url += f"&team={team}&season={season}"
    
    # Realizamos la solicitud DELETE
    response = httpx.delete(url)
    return response.json()

if __name__ == "__main__":
    # Solicitar al usuario qué acción realizar
    print("Opciones:")
    print("1. Poblar datos de pases")
    print("2. Poblar puntos del equipo")
    print("3. Poblar estadísticas del equipo")
    print("4. Poblar touchdowns")
    print("5. Poblar teams")
    print("6. Poblar yardas")
    print("7. Borrar datos")
    choice = input("Selecciona una opción (1/2/3/4/5/6/7): ")

    if choice == "1":
        result = poblar_passes()
    elif choice == "2":
        result = poblar_team_points()
    elif choice == "3":
        result = poblar_team_stats()
    elif choice == "4":
        result = poblar_touchdown()
    elif choice == "5":
        result = poblar_team()
    elif choice == "6":
        result = poblar_yards()
    elif choice == "7":
        tabla = input("Ingresa el nombre de la tabla (nfl_passes, nfl_team_points, nfl_team_stats, Touchdown, Team, Yards): ")
        team = input("Ingresa el equipo (deja vacío si no aplica): ") or None
        season = input("Ingresa la temporada (deja vacío si no aplica): ") or None

        result = borrar_datos(tabla, team, season)
    else:
        result = {"error": "Opción no válida"}

    print(result)
