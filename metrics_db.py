#!/usr/bin/env python3
import subprocess
import pymysql
from datetime import datetime

# ------------------------------------
# Configuración de MySQL
# ------------------------------------
try:
    db = pymysql.connect(
        host="192.168.1.138",         # IP o hostname de tu servidor MySQL
        user="nagios_user",                  # Usuario de MySQL
        password="usuario",      # Contraseña de MySQL
        database="nagios_db",         # Base de datos en la que se insertarán los datos
        port=3306                     # Puerto de MySQL
    )
    cursor = db.cursor()
    print("✅ Conexión a MySQL establecida correctamente.")
except pymysql.MySQLError as err:
    print(f"❌ Error al conectar a MySQL: {err}")
    exit()

# ------------------------------------
# Función para parsear la salida de los scripts
# ------------------------------------
def parse_output(output, service_label):
    """
    Se espera que la salida tenga alguno de estos formatos:
      "OK: <Mensaje descriptivo> [| <datos de performance>]"
      "OK - <Mensaje descriptivo> [| <datos de performance>]"
      "CRITICAL: <Mensaje descriptivo> [| <datos de performance>]"
      "CRITICAL - <Mensaje descriptivo> [| <datos de performance>]"
    
    Devuelve:
      status      : "OK", "CRITICAL", etc. (truncado a 20 caracteres si es necesario)
      message     : El mensaje descriptivo
      duration    : "N/A" (no se extrae de la salida)
      attempt     : "1/1" por defecto o "3/3" si status es CRITICAL
      status_info : La combinación del mensaje y, si existe, los datos de performance.
    """
    # Separa la parte de performance si existe
    parts = output.split(" | ")
    raw_message = parts[0].strip()
    perf_data = parts[1].strip() if len(parts) > 1 else ""

    # Primero, chequeamos si la salida empieza con "OK - " o "CRITICAL - "
    if raw_message.startswith("OK - "):
        status = "OK"
        message = raw_message[len("OK - "):].strip()
    elif raw_message.startswith("CRITICAL - "):
        status = "CRITICAL"
        message = raw_message[len("CRITICAL - "):].strip()
    elif ":" in raw_message:
        # Usamos el delimitador ":" si no se cumple lo anterior.
        status, sep, message = raw_message.partition(":")
        status = status.strip()
        message = message.strip()
    else:
        # Por defecto, toma la primera palabra como estado y el resto como mensaje.
        tokens = raw_message.split()
        status = tokens[0][:20]  # truncamos a 20 caracteres
        message = raw_message[len(status):].strip(" -")
    
    # Truncamos el estado a 20 caracteres si supera ese límite.
    if len(status) > 20:
        status = status[:20]
    
    duration = "N/A"
    if status.upper() == "CRITICAL":
        attempt = "3/3"
    else:
        attempt = "1/1"
    
    status_info = f"{message} | {perf_data}" if perf_data else message
    
    return status, message, duration, attempt, status_info

# ------------------------------------
# Definición de las métricas a ejecutar
# Cada ítem es un diccionario que contiene:
#   host      : Nombre del host (ej. "docker-server")
#   container : Nombre del contenedor ("mysql-container", "apache-container", o "host" para chequeos del sistema)
#   script    : Ruta completa del script de chequeo
#   service   : Etiqueta (ej. "CPU Usage", "Block I/O", etc.)
# ------------------------------------
metrics = [
    # Métricas para contenedor MySQL
    {"host": "docker-server", "container": "mysql-container", "script": "/usr/local/nagios/libexec/check_mycontainer_status.sh", "service": "Status"},
    {"host": "docker-server", "container": "mysql-container", "script": "/usr/local/nagios/libexec/check_container_cpu_usage.sh", "service": "CPU Usage"},
    {"host": "docker-server", "container": "mysql-container", "script": "/usr/local/nagios/libexec/check_container_memory_local.sh", "service": "Memory Usage"},
    {"host": "docker-server", "container": "mysql-container", "script": "/usr/local/nagios/libexec/check_container_network_usage.sh", "service": "Network Usage"},
    {"host": "docker-server", "container": "mysql-container", "script": "/usr/local/nagios/libexec/check_container_restarts.sh", "service": "Restart Count"},
    {"host": "docker-server", "container": "mysql-container", "script": "/usr/local/nagios/libexec/check_container_uptime_ps.sh", "service": "Uptime"},
    # Métricas para contenedor Apache
    {"host": "docker-server", "container": "apache-container", "script": "/usr/local/nagios/libexec/check_container_block_io_tls.sh", "service": "Block I/O"},
    {"host": "docker-server", "container": "apache-container", "script": "/usr/local/nagios/libexec/check_container_cpu_usage.sh", "service": "CPU Usage"},
    {"host": "docker-server", "container": "apache-container", "script": "/usr/local/nagios/libexec/check_container_health.sh", "service": "Health Status"},
    {"host": "docker-server", "container": "apache-container", "script": "/usr/local/nagios/libexec/check_container_status.sh", "service": "Status"},
    # Métrica de red (para el host, o para la interfaz de red del servidor)
    {"host": "docker-server", "container": "host", "script": "/usr/local/nagios/libexec/check_net_usage.sh", "service": "Network Usage (Host)"}
]

# ------------------------------------
# Ejecución de los chequeos y recopilación de datos
# ------------------------------------
datos = []  # Lista que contendrá las tuplas a insertar en MySQL
for metric in metrics:
    host = metric["host"]
    container = metric["container"]
    script = metric["script"]
    service_label = metric["service"]

    print(f"DEBUG: Procesando métrica '{service_label}' para contenedor '{container}' usando el script '{script}'")
    try:
        # Si container es distinto a "host", se pasa el argumento; si es "host", se ejecuta sin argumentos.
        if container.lower() != "host":
            comando = [script, container]
        else:
            comando = [script]

        resultado = subprocess.run(comando, capture_output=True, text=True)
        salida = resultado.stdout.strip()
        
        print(f"DEBUG: Salida obtenida: '{salida}'")
        
        if not salida:
            print(f"⚠️ Sin salida para {service_label} ({script} {container})")
            continue
        
        status, message, duration, attempt, status_info = parse_output(salida, service_label)
        last_check = datetime.now()  # Fecha/hora actual
        
        datos.append((host, service_label, status, last_check, duration, attempt, status_info))
        print(f"✅ Métrica '{service_label}' registrada: {salida}")
        
    except Exception as e:
        print(f"❌ Error al ejecutar {script} para {service_label}: {e}")

# ------------------------------------
# Inserción de los datos en la base de datos
# ------------------------------------
if datos:
    try:
        insert_query = """
            INSERT INTO nagios_metrics (host, service, status, last_check, duration, attempt, status_info)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
        """
        cursor.executemany(insert_query, datos)
        db.commit()
        print("✅ Todos los datos se han insertado en la base de datos.")
    except Exception as e:
        print(f"❌ Error al insertar datos en MySQL: {e}")
else:
    print("⚠️ No se encontraron métricas para insertar.")

# Cierra la conexión con MySQL
cursor.close()
db.close()
print("🔄 Conexión con MySQL cerrada.")
