import os
import logging
import uuid
from flask import Flask, request, render_template, redirect, url_for, session
from flask_cors import CORS
from werkzeug.utils import secure_filename

from src.core.parser import RAW_DIR, process_and_convert_to_parquet
from src.database.config import db_config
from src.database.service import FileMetadataService

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


UPLOAD_FOLDER = str(RAW_DIR)
ALLOWED_EXTENSIONS = {"csv", "xls", "xlsx"}

os.makedirs(UPLOAD_FOLDER, exist_ok=True)

app = Flask(__name__)
app.config["UPLOAD_FOLDER"] = UPLOAD_FOLDER
app.secret_key = os.getenv("FLASK_SECRET_KEY", "allnalyzer-dev-secret")

CORS(app, resources={r"/api/*": {"origins": "*"}})

try:
    db_config.create_all_tables()
    logger.info("Base de datos inicializada correctamente")
except Exception as e:
    logger.warning(f"Advertencia al inicializar BD: {str(e)}")


def _store_last_result(result: dict) -> None:
    session["last_result"] = result


def _load_last_result() -> dict:
    return session.get("last_result", {})


def _build_result_context(base_result: dict, schema_confirmed: bool = False, info_message: str = "") -> dict:
    context = dict(base_result)
    context["schema_confirmed"] = schema_confirmed
    context["info_message"] = info_message
    return context


def allowed_file(filename: str) -> bool:
    return "." in filename and filename.rsplit('.', 1)[1].lower() in ALLOWED_EXTENSIONS


@app.route('/')
def index():
    return render_template('upload.html')


@app.route('/upload', methods=['POST'])
def upload():
    if 'file' not in request.files:
        return redirect(url_for('index'))

    file = request.files['file']
    if file.filename == '':
        return redirect(url_for('index'))

    if file and allowed_file(file.filename):
        filename = secure_filename(file.filename)
        file_token = uuid.uuid4().hex
        extension = os.path.splitext(filename)[1].lower()
        save_path = os.path.join(app.config['UPLOAD_FOLDER'], f"{file_token}{extension}")
        # Guardar el fichero subido en disco
        file.save(save_path)

        # Llamar a la función que procesa el archivo y lo convierte a Parquet
        result = process_and_convert_to_parquet(save_path, file_token=file_token)
        result['original_name'] = filename

        # Paso 7: registrar metadata del parquet en PostgreSQL local.
        if result.get('status') == 'success':
            try:
                db_session = db_config.get_session()
                try:
                    file_record = FileMetadataService.register_file(
                        session=db_session,
                        user_id=1,
                        name=result.get('original_name', filename),
                        local_path=result.get('parquet_path', ''),
                        schema_json=result.get('schema', {}),
                    )
                    result['file_id'] = str(file_record.id)
                finally:
                    db_session.close()
            except Exception as db_exc:
                logger.error(f"No se pudo registrar metadata en Postgres: {str(db_exc)}")
                result['file_id'] = None

        # Guardar el resultado para reutilizarlo cuando el usuario vuelva a cargar la vista.
        _store_last_result(result)

        # Mostrar los resultados en una plantilla simple
        return render_template('result.html', result=_build_result_context(result))

    return redirect(url_for('index'))


if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000, debug=True)
