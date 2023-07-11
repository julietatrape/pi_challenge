# Staging table
Es necesario utilizar una staging table para asegurar que a la hora de hacer el merge no haya claves de merge duplicadas. Si eso sucediera, la ETL fallaría y se detendrían las actualizaciones hasta que se borren los registros duplicados.

- Explicar cada script
- Hablar de los requirements. pip install -r requirements.txt
- Hablar de los logs.

# Scheduler
Cargar el scheduler al programador de tarea de windows. Tener la precaución de modificar los paths del ejecutable de python y del archivo.
Explicar que se escribe solito cuanod ejeutamos initial_Steps.