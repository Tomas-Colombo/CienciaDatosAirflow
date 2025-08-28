from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.http.operators.http import HttpOperator
from airflow.providers.http.hooks.http import HttpHook
from datetime import datetime, timedelta
import pandas as pd
import os
import json
import time
import logging
from requests.exceptions import ConnectionError, HTTPError
logging.getLogger("airflow.hooks.base").setLevel(logging.ERROR)

POKEMON_LIMIT = 1000
OUTPUT_PATH = "/tmp/pokemon_data/pokemon_base.csv"
POKEMON_DATA_PATH = "/tmp/pokemon_data/pokemon_data.json"
SPECIES_DATA_PATH = "/tmp/pokemon_data/species_data.json"
MERGED_DATA_PATH = "/tmp/pokemon_data/pokemon_merged.csv"

default_args = {
    'owner': 'pablo',
    'start_date': datetime.today() - timedelta(days=1),
    'retries': 1,
    'retry_delay': timedelta(minutes=2),
    'email_on_failure': False,
    'depends_on_past': False,
}

# Tarea A: /pokemon/{id}
def download_pokemon_data(**kwargs):
    import os
    import json
    import time
    import logging
    from airflow.providers.http.hooks.http import HttpHook

    # Obtener la lista de Pokémon desde el XCom de fetch_pokemon_list
    ti = kwargs['ti']
    results = json.loads(ti.xcom_pull(task_ids='fetch_pokemon_list'))['results']
    os.makedirs(os.path.dirname(POKEMON_DATA_PATH), exist_ok=True)

    # Si el archivo pokemon_data.json ya existe, lo cargamos para evitar repetir descargas
    if os.path.exists(POKEMON_DATA_PATH):
        with open(POKEMON_DATA_PATH, 'r') as f:
            pokemon_data = json.load(f)

        # Creamos un set con los nombres ya descargados para comparación rápida
        done_names = {p['name'] for p in pokemon_data}
        logging.info(f"[INFO] Ya existen {len(done_names)} pokémon descargados.")
    else:
        pokemon_data = []
        done_names = set()

    # Inicializamos el hook HTTP para hacer las requests a la pokeapi
    hook = HttpHook(http_conn_id='pokeapi', method='GET')

    try:
        # Iteramos sobre los Pokémon disponibles
        for i, entry in enumerate(results):
            name = entry['name']

            # Si ya lo descargamos antes, lo salteamos
            if name in done_names:
                continue

            url = entry['url']
            pokemon_id = url.strip('/').split('/')[-1]
            endpoint = f"/pokemon/{pokemon_id}/"

            # Hacemos la request a la API
            res = hook.run(endpoint)
            pokemon = res.json()

            # Guardamos el JSON crudo en la lista
            pokemon_data.append(pokemon)
            done_names.add(name)

            # Guardado parcial cada 100 Pokémon
            if (i + 1) % 100 == 0:
                with open(POKEMON_DATA_PATH, 'w') as f:
                    json.dump(pokemon_data, f)
                logging.info(f"[INFO] {i + 1} pokémon procesados (hasta ahora {len(pokemon_data)} guardados)")

            # Para no saturar la API
            time.sleep(0.5)

    except Exception as e:
        # Si hay error, guardamos lo que se pudo descargar y relanzamos el error
        logging.error(f"[ERROR] Interrupción en pokémon: {e}")
        with open(POKEMON_DATA_PATH, 'w') as f:
            json.dump(pokemon_data, f)
        raise e

    # Guardado final completo
    with open(POKEMON_DATA_PATH, 'w') as f:
        json.dump(pokemon_data, f)

    logging.info(f"[INFO] Descarga finalizada con {len(pokemon_data)} pokémon.")


# Tarea B: /pokemon-species/{id}
def download_species_data(**kwargs):
    import os
    import json
    import time
    import logging
    from airflow.providers.http.hooks.http import HttpHook

    # Obtener lista de Pokémon desde la tarea anterior (fetch_pokemon_list)
    ti = kwargs['ti']
    results = json.loads(ti.xcom_pull(task_ids='fetch_pokemon_list'))['results']
    os.makedirs(os.path.dirname(SPECIES_DATA_PATH), exist_ok=True)

    # Si el archivo species_data.json ya existe, lo cargamos para evitar repeticiones
    if os.path.exists(SPECIES_DATA_PATH):
        with open(SPECIES_DATA_PATH, 'r') as f:
            species_data = json.load(f)

        # Creamos un set con los nombres ya descargados para comparación rápida
        done_names = {s['name'] for s in species_data}
        logging.info(f"[INFO] Ya existen {len(done_names)} species descargadas.")
    else:
        species_data = []
        done_names = set()

    # Inicializamos el hook para hacer las requests a pokeapi
    hook = HttpHook(http_conn_id='pokeapi', method='GET')

    try:
        # Iteramos sobre todos los Pokémon recibidos en la lista original
        for i, entry in enumerate(results):
            name = entry['name']

            # Si ya descargamos esta species previamente, la salteamos
            if name in done_names:
                continue

            url = entry['url']
            pokemon_id = url.strip('/').split('/')[-1]
            endpoint = f"/pokemon-species/{pokemon_id}/"

            # Hacemos la request y parseamos la respuesta
            res = hook.run(endpoint)
            species = res.json()

            # Guardamos nombre, generación e info de legendario
            species_data.append({
                'name': species['name'],
                'generation': species['generation']['name'],
                'is_legendary': species['is_legendary']
            })
            done_names.add(species['name'])

            # Cada 100 species, escribimos un backup del archivo parcial
            if (i + 1) % 100 == 0:
                with open(SPECIES_DATA_PATH, 'w') as f:
                    json.dump(species_data, f)
                logging.info(f"[INFO] {i + 1} species procesadas (hasta ahora {len(species_data)} guardadas)")

            # Dormimos medio segundo para no saturar la API
            time.sleep(0.5)

    except Exception as e:
        # Si algo falla, guardamos lo que se haya descargado hasta ahora
        logging.error(f"[ERROR] Interrupción en species: {e}")
        with open(SPECIES_DATA_PATH, 'w') as f:
            json.dump(species_data, f)
        raise e  # relanzamos el error para que Airflow marque la tarea como fallida

    # Guardado final completo por si el total no es múltiplo de 100
    with open(SPECIES_DATA_PATH, 'w') as f:
        json.dump(species_data, f)

    logging.info(f"[INFO] Descarga finalizada con {len(species_data)} species.")


# Tarea C: combinar y transformar
def merge_and_transform_data(**kwargs):
    with open(POKEMON_DATA_PATH, 'r') as f:
        pokemon_data = json.load(f)
    with open(SPECIES_DATA_PATH, 'r') as f:
        species_data = json.load(f)
    species_lookup = {
        s['name']: {'generation': s['generation'], 'is_legendary': s['is_legendary']}
        for s in species_data
    }
    tidy_records = []
    for p in pokemon_data:
        p_info = species_lookup.get(p['name'], {})  # puede quedar como None
        stats = {s['stat']['name']: s['base_stat'] for s in p.get('stats', [])}
        types = sorted(p.get('types', []), key=lambda t: t['slot'])
        tidy_records.append({
            "id": p.get("id"),
            "name": p.get("name"),
            "height": p.get("height"),
            "weight": p.get("weight"),
            "base_experience": p.get("base_experience"),
            "generation": p_info.get("generation"),
            "is_legendary": p_info.get("is_legendary", False),
            "type_1": types[0]['type']['name'] if len(types) > 0 else None,
            "type_2": types[1]['type']['name'] if len(types) > 1 else None,
            "hp": stats.get("hp"),
            "attack": stats.get("attack"),
            "defense": stats.get("defense"),
            "special-attack": stats.get("special-attack"),
            "special-defense": stats.get("special-defense"),
            "speed": stats.get("speed"),
            "grupo": "GRUPO 14"

        })
    df = pd.DataFrame(tidy_records)

    # punto 2 
    # Crear carpeta output, a menos que exista
    output_dir = "output"
    os.makedirs(output_dir, exist_ok=True)

    # Obtener ds, q es la fecha de ejecución, del contexto
    ds = kwargs['ds']

    # Definir ruta final con fecha
    output_path = f"{output_dir}/final_{ds}.csv"

    # Guardar CSV
    df.to_csv(output_path, index=False)
    print(f"[INFO] CSV guardado en: {output_path}")

def exportar_logs_reales_zip(**kwargs):
    import zipfile

    # Nombre del DAG en ejecución
    dag_id = kwargs['dag'].dag_id
    ds = kwargs['ds']

    # Carpeta de logs (donde Airflow guarda los logs de cada task)
    logs_dir = f"/usr/local/airflow/logs/{dag_id}"

    # Crear carpeta de salida si no existe
    output_dir = "output"
    os.makedirs(output_dir, exist_ok=True)

    # Nombre final del archivo ZIP
    zip_path = f"{output_dir}/logs_{ds}.zip"

    # Crear archivo ZIP
    with zipfile.ZipFile(zip_path, 'w', zipfile.ZIP_DEFLATED) as zipf:
        for root, _, files in os.walk(logs_dir):
            for file in files:
                file_path = os.path.join(root, file)
                arcname = os.path.relpath(file_path, logs_dir)  # ruta relativa dentro del ZIP
                zipf.write(file_path, arcname)

    print(f"[INFO] Logs comprimidos en: {zip_path}")
    
def enviar_correo_manual(**kwargs):
    import smtplib, ssl, mimetypes
    from email.message import EmailMessage

    ds = kwargs["ds"]
    GRUPO = "GRUPO 14"

    output_dir = "output"
    csv_path = f"{output_dir}/final_{ds}.csv"
    zip_path = f"{output_dir}/logs_{ds}.zip"

    # Validaciones básicas
    for path in (csv_path, zip_path):
        if not os.path.exists(path):
            raise FileNotFoundError(f"No existe el archivo requerido: {path}")

    # Credenciales y servidor SMTP (usa Gmail por defecto)
    smtp_user = os.environ.get("SMTP_USER")
    smtp_pass = os.environ.get("SMTP_PASSWORD")
    smtp_host = os.environ.get("SMTP_HOST", "smtp.gmail.com")
    smtp_port = int(os.environ.get("SMTP_PORT", "587"))

    if not smtp_user or not smtp_pass:
        raise RuntimeError("Faltan SMTP_USER o SMTP_PASSWORD en variables de entorno.")

    # Construir el email
    msg = EmailMessage()
    msg["Subject"] = f"Entrega {GRUPO} - {ds}"
    msg["From"] = smtp_user
    msg["To"] = "tomycolombo2009@hotmail.com"

    cuerpo = (
        f"Hola,\n\n"
        f"Adjuntamos la entrega del {GRUPO} correspondiente a la ejecución {ds}.\n\n"
        f"Incluye:\n"
        f"- CSV final: final_{ds}.csv\n"
        f"- ZIP de logs: logs_{ds}.zip\n\n"
        f"Saludos,\n{GRUPO}\n"
    )
    msg.set_content(cuerpo)

    # Adjuntar archivos
    for file_path in (csv_path, zip_path):
        ctype = mimetypes.guess_type(file_path)[0] or "application/octet-stream"
        maintype, subtype = ctype.split("/", 1)
        with open(file_path, "rb") as f:
            msg.add_attachment(
                f.read(),
                maintype=maintype,
                subtype=subtype,
                filename=os.path.basename(file_path),
            )

    # Enviar
    context = ssl.create_default_context()
    with smtplib.SMTP(smtp_host, smtp_port) as server:
        server.starttls(context=context)
        server.login(smtp_user, smtp_pass)
        server.send_message(msg)

    print(f"[INFO] Email enviado con adjuntos a cienciadedatos.frm.utn@gmail.com (ds={ds}).")






# DAG
with DAG(
    dag_id='pokemon_base_etl_parallel',
    description='DAG ETL paralelo que une data de /pokemon y /pokemon-species',
    default_args=default_args,
    schedule=None,
    catchup=False,
    tags=['pokemon', 'parallel', 'etl']
) as dag:

    fetch_pokemon_list = HttpOperator(
        task_id='fetch_pokemon_list',
        http_conn_id='pokeapi',
        endpoint=f'/pokemon?limit={POKEMON_LIMIT}',
        method='GET',
        log_response=True,
        response_filter=lambda response: response.text,
        do_xcom_push=True,
    )

    download_a = PythonOperator(
        task_id='download_pokemon_data',
        python_callable=download_pokemon_data,
    )

    download_b = PythonOperator(
        task_id='download_species_data',
        python_callable=download_species_data,
    )

    merge_transform = PythonOperator(
        task_id='merge_and_transform_data',
        python_callable=merge_and_transform_data,
    )


    export_logs = PythonOperator(
            task_id='exportar_logs_reales_zip',
            python_callable=exportar_logs_reales_zip,
        )

    enviar_mail = PythonOperator(
    task_id="enviar_correo_manual",
    python_callable=enviar_correo_manual,
    )

# cadena final
[download_a, download_b] >> merge_transform >> export_logs >> enviar_mail
