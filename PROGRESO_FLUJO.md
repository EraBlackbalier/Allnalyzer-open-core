# Flujo actual

1. Carga de CSV/XLSX.
2. Validacion basica en RAM.
3. Conversión a Parquet en disco.
4. Extraccion de esquema.
5. Muestra de 5 filas.
6. Registro de metadata en PostgreSQL local.
7. Confirmacion visual del esquema.

# Como levantar el proyecto

1. Instala las dependencias con `pip install -r requirements.txt`.
2. Crea la base de datos local en PostgreSQL con el nombre `Allnalyzer`.
3. Crea las tablas con `python -m src.database.init_db`.
4. Inicia la app con `python run_server.py`.
5. Abre `http://127.0.0.1:5000` en el navegador.

# Como usarlo

1. Sube un archivo CSV o XLSX desde la pantalla principal.
2. Espera a que el sistema valide el archivo y lo convierta a Parquet.
3. Revisa el esquema detectado y la muestra de 5 filas.
4. Confirma si el esquema corresponde al archivo subido.
5. Usa el enlace de regreso para subir otro archivo si hace falta.