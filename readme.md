# 1. Requisitos
Luego de clonar el repositorio, deben instalarse las librerías de Python necesarias utilizando la siguiente línea de código:
```
pip install -r requirements.txt
```
También debe instalarse el driver ODBC Driver 17 for SQLServer, el cual puede descargarse de la siguiente URL:
<https://learn.microsoft.com/en-us/sql/connect/odbc/download-odbc-driver-for-sql-server?view=sql-server-ver16>

# 2. Archivos útiles 
## 2.1. Settings
Desde este archivo se podrán gestionar distintos parámetros tales como el servidor, la base de datos, la url del source y los nombres de las distintas tablas. Luego, los scripts del ETL llamarán a los parámetros de *settings.yaml* sin necesidad de hardcodear dichas variables. Esto permite modificar rápidamente los valores de los parámetros en caso de ser necesario.

## 2.2. Archivos de logs
Al correr los scripts se crearán dos archivos que almacenan los logs:
* *run_last.log*: almacena únicamente los logs de la última corrida.
* *run_history.log*: almacena todos los logs desde la primera corrida.

# 3. Proceso de ETL
## 3.1. Pasos iniciales
Para inicializar el ambiente, se debe correr primero el script *initial_steps.py*. Dicho script se corre una única vez y su finalidad es actualizar el scheduler y crear las tablas necesarias en la base de datos.

### 3.1.1. Actualización del scheduler
El método **actualizar_scheduler()** actualiza el archivo *scheduler.xml* con las variables de entorno propias de la máquina donde se está corriendo el código. Dicho archivo deberá importarse como tarea en el programador de tareas de Windows. Una vez hecho esto, el script weekly_load.py correrá automáticamente todos los días lunes a las 5 AM.

### 3.1.2. Creación de tablas
Se crea una tabla de staging donde se efectuarán transformaciones antes de hacer el merge y una tabla base que será la tabla final que contenga los datos unificados de las distintas corridas semanales.
Se decidió crear una tabla de staging y no hacer el merge directamente con la tabla proviniente del source para evitar posibles errores de merge por registros duplicados desde el origen. En este approach, en la tabla de staging se aplica una transformación para dejarla libre de duplicados. Esto nos asegura que a la hora de hacer el merge no haya claves de merge duplicadas ya que si eso sucediera, la ETL fallaría y se detendrían las actualizaciones hasta que se borren los registros duplicados.

## 3.2. Corrida semanal
Una vez inicializado el ambiente se puede proceder con la carga semanal de las nuevas filas. Este proceso corre todos los días lunes a las 5 AM.

### 3.2.1. Cleanup
Se borran todos los registros de la tabla con registros del source y de la tabla de staging para evitar errores.

### 3.2.2. Lectura de los datos provinientes del source
Se leen los datos de la url y se vuelcan en una tabla intermedia que cuenta con datos crudos (sin transformaciones ni enriquecimiento).

### 3.2.3. Inserción de datos en la tabla staging
Se insertan los datos de la tabla intermedia en la tabla de staging. En este proceso se hace un enriquecimiento de los datos, colocando la fecha actual, y se eliminan registros duplicados que podrían venir desde la fuente.
Los registros se consideran duplicados cuando tienen igual ID, MUESTRA y RESULTADO. En caso de que eso suceda en esta etapa, se conserva el registro con mayor valor de MUESTRA. 

### 3.2.4. Merge contra la tabla base
Se hace un merge de la tabla de staging contra la tabla base utilizando como clave de merge a los campos ID, MUESTRA y RESULTADO. Utilizando este tipo de operación se realizan dos acciones a la vez:
* **Insert:** se insertan registros completamente nuevos, es decir, cuyos ID, MUESTRA y RESULTADO no existen previamente.
* **Update:** se actualizan registros existentes, es decir, cuyos ID, MUESTRA y RESULTADO existen previamente. En este caso se actualizan todos los demás campos con los datos entrantes con el fin de conservar los valores más recientes.

### 3.2.5. Tests
Se realiza un test de completitud para asegurar que se hayan afectado la misma cantidad de registros que hay en el source. En caso de que no haya coincidencia, los logs arrojan un warrning.

### 3.2.6. Cleanup
Se borran todos los registros de la tabla con registros del source y de la tabla de staging para evitar errores.
