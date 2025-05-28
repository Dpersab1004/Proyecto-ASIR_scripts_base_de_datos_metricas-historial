#!/usr/bin/env python3
import pymysql
import re
from datetime import datetime
import sys

# Configuración de la base de datos con nagios_user
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

# Expresión regular robusta para capturar HOST ALERT y SERVICE ALERT
log_pattern = re.compile(r"\[(\d+)\]\s+(HOST ALERT|SERVICE ALERT):\s+(.*?);(.*?);(.*?);(.*?);(.*?)$")

def process_old_history():
    """
    Lee el log completo e inserta cada línea que coincida con la regex
    en la base de datos.
    """
    inserted_events = 0
    try:
        with open(log_file, "r") as f:
            for line in f:
                line = line.strip()  # Elimina espacios al inicio y final
                match = log_pattern.match(line)
                if match:
                    timestamp_unix, event_type, host, service, state_type, attempt, details = match.groups()
                    try:
                        timestamp = datetime.fromtimestamp(int(timestamp_unix))
                    except ValueError as e:
                        print(f"❌ Error al convertir el timestamp '{timestamp_unix}': {e}")
                        continue

                    cursor.execute(
                        """
                        INSERT INTO events (timestamp, event_type, host, service, state, details)
                        VALUES (%s, %s, %s, %s, %s, %s)
                        """,
                        (timestamp, event_type, host, service, state_type, details)
                    )
                    inserted_events += 1
                else:
                    # Imprime las líneas que no coinciden para depuración
                    print("No coincide:", line)
        db.commit()
    except FileNotFoundError:
        print(f"❌ Error: No se encontró el archivo de log {log_file}.")
    except Exception as e:
        print(f"❌ Error procesando el log: {e}")
    return inserted_events

# Inicio del proceso
print("🟢 Iniciando registro de eventos históricos desde el log de Nagios...")
historical_count = process_old_history()

print(f"✅ Se han insertado {historical_count} eventos históricos en la base de datos.")

cursor.close()
db.close()
print("🔄 Conexión con MySQL cerrada.")
