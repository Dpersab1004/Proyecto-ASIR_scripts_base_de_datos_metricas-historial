#!/usr/bin/env python3
import pymysql
import re
from datetime import datetime
import sys

# Configuración de la base de datos (actualiza según tu entorno)
try:
    db = pymysql.connect(
        host="192.168.1.138",         # IP del servidor MySQL
        user="nagios_user",           # Usuario de MySQL
        password="usuario",           # Contraseña de MySQL
        database="nagios_history",    # Base de datos destino
        port=3306                     # Puerto de MySQL
    )
    cursor = db.cursor()
    print("✅ Conexión a MySQL establecida correctamente con nagios_user.")
except pymysql.MySQLError as err:
    print(f"❌ Error al conectar a MySQL: {err}")
    sys.exit(1)

# Ruta del archivo de log de Nagios
log_file = "/usr/local/nagios/var/nagios.log"

# Expresión regular para capturar HOST ALERT y SERVICE ALERT.
# Asegúrate de que toda la expresión quede en UNA única línea.
log_pattern = re.compile(r"\[(\d+)\]\s+(HOST ALERT|SERVICE ALERT):\s+(.*?);(.*?);(.*?);(.*?);(.*?)$")

def process_log():
    """
    Recorre el archivo de log e inserta EN LA BASE DE DATOS
    aquellos eventos cuyo timestamp corresponda al día actual.
    Retorna el número de eventos insertados.
    """
    today = datetime.now().date()  # Fecha actual
    new_events = 0

    try:
        with open(log_file, "r") as f:
            for line in f:
                line = line.strip()  # Elimina espacios en blanco al inicio y al final
                match = log_pattern.match(line)
                if match:
                    # Extrae los grupos de la regex
                    timestamp_unix, event_type, host, service, state_type, attempt, details = match.groups()
                    try:
                        timestamp = datetime.fromtimestamp(int(timestamp_unix))
                    except ValueError as e:
                        print(f"❌ Error al convertir el timestamp '{timestamp_unix}': {e}")
                        continue

                    # Inserta solamente si el evento es del día actual
                    if timestamp.date() == today:
                        cursor.execute(
                            "INSERT INTO events (timestamp, event_type, host, service, state, details) VALUES (%s, %s, %s, %s, %s, %s)",
                            (timestamp, event_type, host, service, state_type, details)
                        )
                        new_events += 1
        db.commit()
    except FileNotFoundError:
        print(f"❌ Error: No se encontró el archivo de log {log_file}.")
    except Exception as e:
        print(f"❌ Error procesando el log: {e}")

    return new_events

# Inicio del proceso
print("🟢 Iniciando procesamiento de eventos del día actual...")
events_today = process_log()

if events_today > 0:
    print(f"✅ Se han insertado {events_today} eventos del día actual en la base de datos.")
else:
    print("⚠️ No se han encontrado alertas nuevas del día actual. Se cierra la conexión.")

# Cerrar la conexión a la base de datos
cursor.close()
db.close()
print("🔄 Conexión con MySQL cerrada.")
