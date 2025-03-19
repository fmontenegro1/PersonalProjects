import pandas as pd
import json
from datetime import datetime

# Ruta al archivo JSON
json_path = 'c:/Users/Fmontenegro/Desktop/conversations.json'  # Cambia esto por la ruta correcta

# Cargar el JSON
with open(json_path, 'r', encoding='utf-8') as file:
    data = json.load(file)
# Verificamos si 'data' es una lista
if not isinstance(data, list):
    raise ValueError("El JSON no tiene la estructura esperada: se esperaba una lista.")

#funcion que vamos a utilizar mas adelante para la columna fecha
def convert_timestamp(timestamp):
    if timestamp is not None:
        return datetime.fromtimestamp(timestamp).strftime('%Y-%m-%d %H:%M:%S')
    return None


# Inicializamos una lista para almacenar los resultados
all_parts_list = []

# Recorremos cada elemento de la lista 'data'
for item in data:
    # Validamos que el elemento sea un diccionario
    if not isinstance(item, dict):
        continue

    # Obtenemos el título, el mapeo y la fecha de creación
    title = item.get('title')
    mapping = item.get('mapping', {})
    create_time = item.get('create_time')  # Obtenemos 'created_at'

    # Verificamos que 'mapping' sea un diccionario
    if not isinstance(mapping, dict):
        continue

    # Recorremos los mensajes en el mapeo
    for value in mapping.values():
        message = value.get('message')  # Obtiene el mensaje
        if not isinstance(message, dict):  # Validamos que 'message' sea un diccionario
            continue

        content = message.get('content', {})  # Obtiene el contenido
        parts = content.get('parts', [])  # Obtiene las partes

        # Extraemos texto si los elementos de 'parts' son diccionarios
        extracted_parts = []
        for part in parts:
            if isinstance(part, str):
                extracted_parts.append(part)
            elif isinstance(part, dict):
                extracted_parts.append(part.get('text', ''))  # Extrae 'text' si existe

        # Filtramos partes vacías y unimos el texto
        extracted_text = ' '.join(filter(None, extracted_parts))

        if extracted_text:  # Si hay texto válido, lo agregamos a la lista
            all_parts_list.append({
                'title': title,
                'parts': extracted_text,
                'create_time': convert_timestamp(create_time)  # Incluimos 'created_at'
            })

# Convertimos la lista a un DataFrame
df = pd.DataFrame(all_parts_list)

# Agrupar por título y reorganizar en pares
result = (
    df.groupby(["title", "create_time"])["parts"]
    .apply(lambda group: pd.DataFrame({
        "pregunta": group.iloc[::2].reset_index(drop=True),  # Filas impares
        "respuesta": group.iloc[1::2].reset_index(drop=True)  # Filas pares
    }))
    .reset_index(level=[0, 1])
)

result.head(50)