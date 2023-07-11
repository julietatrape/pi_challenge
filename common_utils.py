import subprocess
import xml.etree.ElementTree as ET
import logging
import yaml
import sqlalchemy as sa
from sqlalchemy.exc import SQLAlchemyError
import sql

class MyClass():
    def __init__(self) -> None:
        # Configuración de logging
        logging.basicConfig(level=logging.DEBUG,
            format='%(asctime)s [%(levelname)s] %(message)s',
            handlers=[
                logging.FileHandler('logs_last.log', 'w+'),
                logging.FileHandler('logs_history.log', 'a'),
                logging.StreamHandler()
            ])

        # Declaración de variables definidas en el archivo settings.yaml
        with open(r'settings.yaml') as file:
            variables_dict = yaml.load(file, Loader=yaml.FullLoader)

        self.scheduler = variables_dict["scheduler"]

        self.db_server = variables_dict["db_properties"]["server"]
        self.db_name = variables_dict["db_properties"]["name"]
        self.db_driver = variables_dict["db_properties"]["driver"]

        self.base_table = variables_dict["nuevas_filas"]["base_table"]
        self.staging_table = variables_dict["nuevas_filas"]["staging_table"]
        self.incoming_data_table = variables_dict["nuevas_filas"]["incoming_data_table"]
        self.file_url = variables_dict["nuevas_filas"]["file_url"]


    def actualizar_scheduler(self):
        # Definición del directorio local y de la localización del ejecutable de python
        python_ex_location = subprocess.run("which python", capture_output=True, text=True).stdout.rstrip()
        current_location = subprocess.run("pwd", capture_output=True, text=True).stdout.rstrip()

        # Convesión de paths de Linux a path de Windows
        python_ex_location_wdw = python_ex_location.replace("/c","C:").replace("/","\\")
        current_location_wdw = current_location.replace("/c","C:").replace("/","\\")

        # Modificación de las variables anteriores dentro del archivo xml
        tree = ET.parse(self.scheduler)
        root = tree.getroot()
        namespace = {"ns": "http://schemas.microsoft.com/windows/2004/02/mit/task"}

        # Búsqueda y actualización de Command
        command = root.find(".//ns:Command", namespace)
        command.text = python_ex_location_wdw+".exe"

        # Búsqueda y actualización de WorkingDirectory
        working_directory = root.find(".//ns:WorkingDirectory", namespace)
        working_directory.text = current_location_wdw
        
        # Guardado del archivo xml con los cambios
        tree.write(self.scheduler, encoding="UTF-16", xml_declaration=True)


    def crear_conexion(self):
        logging.info(f"Creando la conexión a la base de datos...")
        logging.info(f"SERVIDOR: {self.db_server} | DRIVER: {self.db_driver} | BASE DE DATOS: {self.db_name}")

        try:
            engine = sa.create_engine(f"mssql+pyodbc://{self.db_server}/{self.db_name}?driver={self.db_driver}")
            self.conn = engine.connect()
            logging.info(f"Conexión exitosa!")
            return self.conn
        except SQLAlchemyError as e:
            logging.error(str(e))


    def truncar_tabla(self, nombre_tabla:str):
        try:
            logging.info(f"Truncando la tabla {nombre_tabla}...") 
            self.conn.execute(sa.text(sql.truncate_table.format(nombre_tabla,nombre_tabla)))
            logging.info(f"Tabla {nombre_tabla} truncada con éxito!")
        except sa.exc.ProgrammingError as e:
            logging.error(str(e))