import requests
from tabulate import tabulate

API_URL = "http://127.0.0.1:8000"  

# Función de menú interactivo
def menu():
    while True:
        print("\nBienvenido al sistema de estadísticas NFL")
        print("Seleccione una opción para obtener estadísticas:")

        # Sección de consultas desde MongoDB
        print("\nConsultas desde MongoDB:")
        print("1. Estadísticas Generales de los Jugadores")
        print("2. Estadísticas de Todos los Equipos por Temporada")  
        print("3. Intercepciones por Temporada")  
        print("4. Pases Incompletos por Temporada")  

        # Sección de consultas desde Cassandra
        print("\nConsultas desde Cassandra:")
        print("5. Total de Puntos por Equipo")
        print("6. Estadísticas de un Equipo por Temporada")
        print("7. Yardas Totales por Equipo y Temporada")
        print("8. Touchdowns por Equipo y Temporada")

        print("0. Salir")

        try:
            opcion = int(input("Elija una opción (0-8): "))
            if opcion == 1:
                response = requests.get(f"{API_URL}/estadisticas-generales")
                if response.status_code == 200:
                    data = response.json()
                    if isinstance(data, dict) and "statistics" in data:
                        stats = data["statistics"]
                        print("\nEstadísticas Generales de los Jugadores:")
                        headers = ["Estadística", "Valor"]
                        stats_table = [
                            ["Promedio de Altura", stats.get("Promedio de Altura", "N/A")],
                            ["Promedio de Peso", stats.get("Promedio de Peso", "N/A")],
                            ["Promedio de Edad", stats.get("Promedio de Edad", "N/A")],
                            ["Altura Máxima", stats.get("Altura Máxima", "N/A")],
                            ["Altura Mínima", stats.get("Altura Mínima", "N/A")],
                            ["Peso Máximo", stats.get("Peso Máximo", "N/A")],
                            ["Peso Mínimo", stats.get("Peso Mínimo", "N/A")],
                        ]
                        print(tabulate(stats_table, headers, tablefmt="grid"))
                    else:
                        print("Error: La respuesta no contiene las estadísticas esperadas.")
                else:
                    print("Error al obtener estadísticas generales.")
            elif opcion == 5:
                response = requests.get(f"{API_URL}/puntos-totales-por-equipo")
                if response.status_code == 200:
                    data = response.json()
                    if isinstance(data, dict) and "data" in data:
                        print("\nTotal de Puntos por Equipo:")
                        headers = ["Equipo", "Total de Puntos"]
                        stats = [[item["team"], item["total_points_sum"]] for item in data["data"]]
                        print(tabulate(stats, headers, tablefmt="grid"))
                    else:
                        print("Error: Los datos no están en el formato esperado.")
                else:
                    print("Error al obtener total de puntos por equipo.")
            elif opcion == 2:  # Estadísticas de Todos los Equipos por Temporada
                response = requests.get(f"{API_URL}/estadisticas-equipos-temporada")
                if response.status_code == 200:
                    try:
                        data = response.json()
                        if isinstance(data, list):
                            print(f"\nEstadísticas de Todos los Equipos por Temporada:")
                            headers = ["Temporada", "Equipo", "Victorias en Casa", "Derrotas en Casa", "Victorias Fuera", "Derrotas Fuera", "Total Victorias", "Total Derrotas", "Empates"]
                            stats = [
                                [item["season"], item["team"], item["home_wins"], item["home_losses"], item["away_wins"], item["away_losses"], item["wins"], item["losses"], item["ties"]]
                                for item in data
                            ]
                            print(tabulate(stats, headers, tablefmt="grid"))
                        elif isinstance(data, dict) and "data" in data:
                            print(f"\nEstadísticas de Todos los Equipos por Temporada:")
                            headers = ["Temporada", "Equipo", "Victorias en Casa", "Derrotas en Casa", "Victorias Fuera", "Derrotas Fuera", "Total Victorias", "Total Derrotas", "Empates"]
                            stats = [
                                [item["season"], item["team"], item["home_wins"], item["home_losses"], item["away_wins"], item["away_losses"], item["wins"], item["losses"], item["ties"]]
                                for item in data["data"]
                            ]
                            print(tabulate(stats, headers, tablefmt="grid"))
                        else:
                            print("Error: Los datos no están en el formato esperado. La respuesta debería ser una lista o un diccionario con la clave 'data'.")
                    except ValueError as e:
                        print(f"Error al procesar los datos: {e}")
                else:
                    print("Error al obtener estadísticas de equipos por temporada.")
            elif opcion == 3:  # Intercepciones por Temporada
                response = requests.get(f"{API_URL}/intercepciones-temporada")
                if response.status_code == 200:
                    try:
                        data = response.json()
                        if isinstance(data, dict) and "intercepciones_temporada" in data:
                            intercepciones = data["intercepciones_temporada"]
                            print("\nIntercepciones por Temporada:")
                            headers = ["Temporada", "Total Intercepciones"]
                            stats = []
                            for item in intercepciones:
                                temporada = item.get("temporada", "N/A")
                                totalIntercepciones = item.get("totalIntercepciones", "N/A")
                                stats.append([temporada, totalIntercepciones])
                            print(tabulate(stats, headers, tablefmt="grid"))
                        else:
                            print("Error: Los datos no contienen la clave 'intercepciones_temporada'.")
                    except ValueError as e:
                        print(f"Error al procesar los datos: {e}")
                else:
                    print("Error al obtener intercepciones por temporada.")
            elif opcion == 4:  # Pases Incompletos por Temporada
                response = requests.get(f"{API_URL}/pases-incompletos-temporada")
                if response.status_code == 200:
                    try:
                        data = response.json()
                        if isinstance(data, dict) and "pases_incompletos_temporada" in data:
                            pases_incompletos = data["pases_incompletos_temporada"]
                            print("\nPases Incompletos por Temporada:")
                            headers = ["Temporada", "Total Pases Incompletos"]
                            stats = []
                            for item in pases_incompletos:
                                temporada = item.get("temporada", "N/A")
                                totalPasesIncompletos = item.get("totalPasesIncompletos", "N/A")
                                stats.append([temporada, totalPasesIncompletos])
                            print(tabulate(stats, headers, tablefmt="grid"))
                        else:
                            print("Error: Los datos no contienen la clave 'pases_incompletos_temporada'.")
                    except ValueError as e:
                        print(f"Error al procesar los datos: {e}")
                else:
                    print("Error al obtener pases incompletos por temporada.")
            elif opcion == 6:  # Estadísticas de un Equipo por Temporada
                team = input("Ingrese el nombre del equipo: ")
                response = requests.get(f"{API_URL}/stats-por-equipo/{team}")
                if response.status_code == 200:
                    data = response.json()
                    if isinstance(data, dict) and "data" in data:
                        print(f"\nEstadísticas de {team} por Temporada:")
                        headers = ["Temporada", "Victorias en Casa", "Derrotas en Casa", "Victorias Fuera", "Derrotas Fuera", "Total Victorias", "Total Derrotas", "Empates"]
                        stats = [
                            [item["season"], item["home_wins"], item["home_losses"], item["away_wins"], item["away_losses"], item["wins"], item["losses"], item["ties"]]
                            for item in data["data"]
                        ]
                        print(tabulate(stats, headers, tablefmt="grid"))
                    else:
                        print("Error: Los datos no están en el formato esperado.")
                else:
                    print("Error al obtener estadísticas de equipo por temporada.")
            elif opcion == 7:  # Yardas Totales por Equipo y Temporada
                team = input("Ingrese el nombre del equipo: ")
                season = input("Ingrese la temporada (por ejemplo, 2022): ")
                response = requests.get(f"{API_URL}/get-team-yards/{team}/{season}")
                if response.status_code == 200:
                    try:
                        data = response.json()
                        if isinstance(data, dict) and "total_yards" in data:
                            print(f"\nYardas Totales por {team} en la Temporada {season}:")
                            print(f"Yardas Totales: {data['total_yards']}")
                        else:
                            print("Error: Los datos no contienen la clave 'yardas_totales'.")
                    except ValueError as e:
                        print(f"Error al procesar los datos: {e}")
                else:
                    print("Error al obtener yardas totales por equipo y temporada.")
            
            elif opcion == 8:  # Touchdowns Totales por Equipo y Temporada
                team = input("Ingrese el nombre del equipo: ")
                season = input("Ingrese la temporada (por ejemplo, 2022): ")
                response = requests.get(f"{API_URL}/get-team-touchdowns/{team}/{season}")
                if response.status_code == 200:
                    try:
                        data = response.json()
                        if isinstance(data, dict) and "total_receiving_td" in data:
                            print(f"\nTouchdowns Totales por {team} en la Temporada {season}:")
                            print(f"Touchdowns Totales: {data['total_receiving_td']}")
                        else:
                            print("Error: Los datos no contienen la clave 'total_receiving_td'.")
                    except ValueError as e:
                        print(f"Error al procesar los datos: {e}")
                else:
                    print("Error al obtener touchdowns totales por equipo y temporada.")

            elif opcion == 0:
                print("Saliendo del sistema...")
                break
            else:
                print("Opción no válida, por favor intente nuevamente.")
        except ValueError:
            print("Por favor, ingrese un número válido.")

if __name__ == "__main__":
    menu()
