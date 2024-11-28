import curses
import pandas as pd
import csv

# Función para inicializar los colores
def inicializar_colores():
    curses.start_color()
    curses.init_pair(1, curses.COLOR_WHITE, curses.COLOR_BLACK)  # Texto blanco, fondo azul
    curses.init_pair(2, curses.COLOR_WHITE, curses.COLOR_BLACK)  # Texto negro, fondo amarillo
    curses.init_pair(3, curses.COLOR_WHITE, curses.COLOR_BLACK)  # Texto verde, fondo negro

# Función para centrar texto
def centrar_texto(stdscr, texto, fila):
    max_x = curses.COLS  # Ancho de la terminal
    inicio = (max_x // 2) - (len(texto) // 2)  # Calcular el inicio para centrar
    stdscr.addstr(fila, inicio, texto)

# Función para mostrar el menú principal
def mostrar_menu(stdscr):
    curses.curs_set(0)  # Ocultar el cursor
    stdscr.clear()  # Limpiar pantalla

    # Inicializamos los colores
    inicializar_colores()

    menu = [
        "1. Jugadores",
        "2. Equipos",
        "3. Jugadores & Equipos",
        "4. Salir"
    ]
    
    current_row = 0

    while True:
        # Centrar el título
        centrar_texto(stdscr, "Menú Principal", 0)

        for idx, item in enumerate(menu):
            if idx == current_row:
                stdscr.addstr(idx + 2, 0, item, curses.color_pair(2) | curses.A_BOLD)  # Resaltar la opción seleccionada
            else:
                stdscr.addstr(idx + 2, 0, item, curses.color_pair(1))  # Opción normal

        key = stdscr.getch()

        if key == curses.KEY_DOWN:
            current_row = (current_row + 1) % len(menu)
        elif key == curses.KEY_UP:
            current_row = (current_row - 1) % len(menu)
        elif key == 10:  # Enter
            if current_row == 0:
                seleccionar_jugadores(stdscr)
            elif current_row == 1:
                stdscr.clear()
                stdscr.addstr(0, 0, "Seleccionó Equipos. (en construcción)")
                stdscr.refresh()
                stdscr.getch()
            elif current_row == 2:
                stdscr.clear()
                stdscr.addstr(0, 0, "Seleccionó Jugadores & Equipos. (en construcción)")
                stdscr.refresh()
                stdscr.getch()
            elif current_row == 3:
                break  # Salir

        stdscr.refresh()

# Función para seleccionar jugadores y sus stats
def seleccionar_jugadores(stdscr):
    stdscr.clear()
    centrar_texto(stdscr, "Seleccionar Jugador", 0)
    
    options = [
        "1. Stats",
        "2. Yardas",
        "3. Pase",
        "4. Touchdown"
    ]

    current_row = 0
    while True:
        for idx, option in enumerate(options):
            if idx == current_row:
                stdscr.addstr(idx + 2, 0, option, curses.color_pair(3) | curses.A_BOLD)
            else:
                stdscr.addstr(idx + 2, 0, option, curses.color_pair(1))

        key = stdscr.getch()

        if key == curses.KEY_DOWN:
            current_row = (current_row + 1) % len(options)
        elif key == curses.KEY_UP:
            current_row = (current_row - 1) % len(options)
        elif key == 10:  # Enter
            if current_row == 0:
                mostrar_stats_jugadores(stdscr)
            elif current_row == 1:
                mostrar_stats_jugadores(stdscr)
            elif current_row == 2:
                mostrar_stats_jugadores(stdscr)
            elif current_row == 3:
                mostrar_stats_jugadores(stdscr)
        stdscr.refresh()

# Función para mostrar las estadísticas de los jugadores
def mostrar_stats_jugadores(stdscr):
    # Datos simulados de jugadores
    jugadores = [
        [1, "Juan Pérez", "500 yardas, 3 touchdowns"],
        [2, "Carlos Gómez", "600 yardas, 5 touchdowns"],
        [3, "Luis Fernández", "300 yardas, 1 touchdown"],
    ]
    
    exportar_a_csv(jugadores, "jugadores_stats.csv")
    stdscr.clear()
    centrar_texto(stdscr, "Datos exportados a 'jugadores_stats.csv'. Ahora convertimos a DataFrame:", 0)
    stdscr.refresh()
    stdscr.getch()  # Esperar a que el usuario presione una tecla
    df = convertir_a_df("jugadores_stats.csv")
    stdscr.clear()
    centrar_texto(stdscr, "Jugadores Stats:", 0)
    for i, row in df.iterrows():
        stdscr.addstr(i + 1, 0, f"{row['ID']} - {row['Nombre']} - {row['Estadísticas']}")
    stdscr.refresh()
    stdscr.getch()  # Esperar que el usuario cierre la pantalla

# Función para manejar la exportación a CSV
def exportar_a_csv(datos, nombre_archivo):
    with open(nombre_archivo, mode='w', newline='') as file:
        writer = csv.writer(file)
        writer.writerow(["ID", "Nombre", "Estadísticas"])  # Encabezados
        for row in datos:
            writer.writerow(row)

# Función para leer el CSV y convertirlo a DataFrame
def convertir_a_df(nombre_archivo):
    return pd.read_csv(nombre_archivo)

# Inicializar curses y ejecutar el menú
if __name__ == "__main__":
    curses.wrapper(mostrar_menu)
