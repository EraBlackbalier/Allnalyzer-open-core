# Endpoints activos

## web_app.py

- `GET /`: muestra el formulario de subida.
- `POST /upload`: procesa el archivo y muestra esquema + preview.
- `POST /confirm-schema`: registra la confirmacion visual del esquema.

## src/api/routes.py

- `POST /api/upload`: procesa archivo, convierte a Parquet y registra metadata.
- `GET /api/files`: lista archivos del usuario local.
- `GET /api/files/<file_id>`: devuelve metadata de un archivo.
- `GET /api/files/<file_id>/history`: devuelve historial numerico.