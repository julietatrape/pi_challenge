import logging
import sql
import pandas as pd
import sqlalchemy as sa
from common_utils import MyClass


# Creación de una instancia de MyClass
nuevas_filas = MyClass()

# Apertura de la conexión a la BD
conn = nuevas_filas.crear_conexion()

# Cleanup de tablas por si quedaron datos de corridas anteriores
for tabla in [nuevas_filas.incoming_data_table, nuevas_filas.staging_table]:
    nuevas_filas.truncar_tabla(tabla)

# Lectura de la tabla proveniente de la URL
logging.info(f"Leyendo datos desde la fuente...")
nuevos_datos = pd.read_csv(nuevas_filas.file_url)
nuevos_datos["FECHA_COPIA"] = pd.to_datetime(nuevos_datos["FECHA_COPIA"])
nuevos_datos.to_sql(nuevas_filas.incoming_data_table, conn, if_exists='append')
logging.info(f"Datos importados exitosamente hacia la tabla {nuevas_filas.incoming_data_table}")

# Inserción de los nuevos datos provienientes de la URL en la tabla staging
# Hay que hacer commit ya que sqlalchemy solo hace autocommit cuando la query empieza con insert, delete, etc
# Pero como usamos CTE no empieza con esas keywords
transaction = conn.begin()
try:
    logging.info(f"Insertando nuevos registros en la tabla {nuevas_filas.staging_table}...")
    result_insert_into_stg = conn.execute(sa.text(sql.insert_into_stg.format(nuevas_filas.staging_table)))
    transaction.commit()
    rows_inserted = result_insert_into_stg.rowcount
    logging.info(f"Reguistros insertados: {rows_inserted} -> Tabla {nuevas_filas.staging_table}")
except sa.exc.ProgrammingError as e:
    logging.error(str(e))

# Merge
transaction = conn.begin()
try:
    logging.info(f"Merge {nuevas_filas.staging_table} -> {nuevas_filas.base_table}...")
    result_merge_with_base = conn.execute(sa.text(sql.merge_with_base.format(nuevas_filas.base_table,nuevas_filas.staging_table)))
    transaction.commit()
    rows_merged = result_merge_with_base.rowcount
    logging.info(f"Registros insertados/actualizados: {rows_merged} -> Tabla {nuevas_filas.base_table}")
except sa.exc.ProgrammingError as e:
    logging.error(str(e))

# Cleanup de tablas para asegurar que queden limpias para futuras cargas
for tabla in [nuevas_filas.incoming_data_table, nuevas_filas.staging_table]:
    nuevas_filas.truncar_tabla(tabla)

# Cierre de la conexión a la BD
conn.close()