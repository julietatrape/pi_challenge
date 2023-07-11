import logging
import sql
import sqlalchemy as sa
from common_utils import MyClass


# Creación de una instancia de MyClass
nuevas_filas = MyClass()

# Actualización del archivo scheduler.xml con variables de entorno
logging.info(f"Actualizando el scheduler {nuevas_filas.scheduler}...")
nuevas_filas.actualizar_scheduler()
logging.info(f"{nuevas_filas.scheduler} actualizado con éxito!")

# Apertura de la conexión a la BD
conn = nuevas_filas.crear_conexion()

# Creación de las tablas de la base de datos
for tabla in [nuevas_filas.base_table, nuevas_filas.staging_table]:
    try: 
    # Drop para que el código no falle si las tablas existen
        conn.execute(sa.text(sql.drop_table.format(tabla)))
    # Creación de las tablas
        logging.info(f"Creando la tabla {tabla}...")
        conn.execute(sa.text(sql.create_table.format(tabla)))
        logging.info(f"Tabla {tabla} creada con éxito!")
    except sa.exc.ProgrammingError as e:
        logging.error(str(e))

# Cierre de la conexión a la BD
conn.close()