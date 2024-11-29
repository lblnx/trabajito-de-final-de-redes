from pymongo.collection import Collection

#-------Estadisticas Generales----------------
def obtener_estadisticas_generales(mongo_collection: Collection):
    try:
        # Definir el pipeline de agregación para obtener estadísticas
        pipeline = [
            {
                "$group": {
                    "_id": None,  # No agrupamos por nada específico
                    "averageHeight": {"$avg": "$height"},
                    "averageWeight": {"$avg": "$weight"},
                    "averageAge": {"$avg": "$age"},
                    "maxHeight": {"$max": "$height"},
                    "minHeight": {"$min": "$height"},
                    "maxWeight": {"$max": "$weight"},
                    "minWeight": {"$min": "$weight"}
                }
            }
        ]
        
        # Ejecutar la consulta de agregación
        result = list(mongo_collection.aggregate(pipeline))

        # Si el resultado no está vacío, formateamos y devolvemos las estadísticas
        if result:
            stats = result[0]  # Como estamos usando `_id: None`, solo habrá un resultado
            stats_formatted = {
                "Promedio de Altura": stats.get("averageHeight", "N/A"),
                "Promedio de Peso": stats.get("averageWeight", "N/A"),
                "Promedio de Edad": stats.get("averageAge", "N/A"),
                "Altura Máxima": stats.get("maxHeight", "N/A"),
                "Altura Mínima": stats.get("minHeight", "N/A"),
                "Peso Máximo": stats.get("maxWeight", "N/A"),
                "Peso Mínimo": stats.get("minWeight", "N/A")
            }
            return stats_formatted
        else:
            return {"message": "No se encontraron datos para calcular las estadísticas."}
    
    except Exception as e:
        return {"error": f"Error al calcular estadísticas: {str(e)}"}
#-------Datos de equipo por temporada---------
def obtener_estadisticas_equipos_por_temporada(mongo_collection: Collection):
    try:
        pipeline = [
            {
                "$group": {
                    "_id": {"team": "$team", "season": "$season"},  
                    "totalJugadores": {"$sum": 1},  
                    "jugadoresPorPosicion": {
                        "$push": "$position"  
                    }
                }
            },
            {
                "$project": {
                    "totalJugadores": 1,  
                    "jugadoresPorPosicion": 1,  
                    "posicionesUnicas": {"$size": {"$setUnion": ["$jugadoresPorPosicion", "$jugadoresPorPosicion"]}}  
                }
            }
        ]
        result = list(mongo_collection.aggregate(pipeline))
        if result:
            stats = []
            for item in result:
                stats.append({
                    "equipo": item["_id"]["team"],
                    "temporada": item["_id"]["season"],
                    "totalJugadores": item["totalJugadores"],
                    "totalPosicionesUnicas": item["posicionesUnicas"]
                })
            return stats
        else:
            return {"message": "No se encontraron datos para calcular las estadísticas de equipos por temporada."}
    except Exception as e:
        return {"error": f"Error al calcular estadísticas de equipos por temporada: {str(e)}"}
#-------Estadisticas de pases interceptados por temporada-------
def obtener_intercepciones_por_temporada(mongo_collection: Collection):
    try:
        pipeline = [
            {
                "$group": {
                    "_id": {"season": "$season"},  
                    "totalIntercepciones": {"$sum": "$interception"}  
                }
            },
            {
                "$project": {
                    "totalIntercepciones": 1  
                }
            }
        ]
        result = list(mongo_collection.aggregate(pipeline))

        if result:
            stats = []
            for item in result:
                stats.append({
                    "temporada": item["_id"]["season"],
                    "totalIntercepciones": item["totalIntercepciones"]
                })
            return stats
        else:
            return {"message": "No se encontraron datos para calcular las intercepciones por temporada."}
    
    except Exception as e:
        return {"error": f"Error al calcular las intercepciones por temporada: {str(e)}"}
#-------- Estadsiticas de pases incompletos por temporada --------
def obtener_pases_incompletos_por_temporada(mongo_collection: Collection):
    try:
        # Definir el pipeline de agregación
        pipeline = [
            {
                "$group": {
                    "_id": {"season": "$season"}, 
                    "totalPasesIncompletos": {"$sum": "$incomplete_pass"} 
                }
            },
            {
                "$project": {
                    "totalPasesIncompletos": 1  
                }
            }
        ]
        result = list(mongo_collection.aggregate(pipeline))

        if result:
            stats = []
            for item in result:
                stats.append({
                    "temporada": item["_id"]["season"],
                    "totalPasesIncompletos": item["totalPasesIncompletos"]
                })
            return stats
        else:
            return {"message": "No se encontraron datos para calcular los pases incompletos por temporada."}
    
    except Exception as e:
        return {"error": f"Error al calcular los pases incompletos por temporada: {str(e)}"}
