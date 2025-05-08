import os
from datetime import datetime
import psycopg2
import py7zr
import gdown
import json
import logging

# Impostazioni logging
log_file = "import_log.txt"
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[
        logging.FileHandler(log_file),
        logging.StreamHandler()
    ]
)

# Caricamento del file di configurazione globale
with open('config.json') as config_file:
    config = json.load(config_file)

# Credenziali database
conn_info = config['db']

# URL dei file da Google Drive
urls = [
    config['urls']['iliad'],
    config['urls']['tim'],
    config['urls']['vodafone'],
    config['urls']['windtre']
]

# Elenco degli schemi del database
schemas = [
    "map_iliad10km",
    "map_tim10km",
    "map_vodafone10km",
    "map_windtre10km"
]

# Directory associate ai vari operatori
directories = [
    "COPERTURE_10km_v3_ILIAD_LTE-NR_ITALIA",
    "COPERTURE_10km_v3_TIM_LTE-NR_ITALIA",
    "COPERTURE_10km_v3_VODAFONE_LTE-NR_ITALIA",
    "COPERTURE_10km_v3_WINDTRE_LTE-NR_ITALIA"
]

def create_import_log_table(cur):
    cur.execute("SELECT to_regclass('public.import_log_tbl_italia_contributi');")
    if cur.fetchone()[0] is not None:
        cur.execute("SELECT COUNT(*) FROM public.import_log_tbl_italia_contributi;")
        if cur.fetchone()[0] == 0:
            cur.execute("DROP TABLE public.import_log_tbl_italia_contributi;")
            cur.connection.commit()
        else:
            return
    cur.execute("""
        CREATE TABLE IF NOT EXISTS public.import_log_tbl_italia_contributi (
            id SERIAL PRIMARY KEY,
            schema_name TEXT,
            file_name TEXT,
            imported_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
    """)
    cur.connection.commit()

def is_file_already_imported(cur, schema_name, file_name):
    cur.execute("""
        SELECT 1 FROM public.import_log_tbl_italia_contributi 
        WHERE schema_name = %s AND file_name = %s
    """, (schema_name, file_name))
    return cur.fetchone() is not None

def log_import(cur, schema_name, file_name):
    cur.execute("""
        INSERT INTO public.import_log_tbl_italia_contributi (schema_name, file_name)
        VALUES (%s, %s)
    """, (schema_name, file_name))
    cur.connection.commit()

def download_files():
    for i in range(len(urls)):
        zip_path = f"{directories[i]}.7z"
        folder_path = directories[i]
        if os.path.exists(folder_path):
            logging.info(f"[GIÀ PRESENTE] La directory {folder_path} esiste già. Salto download.")
            continue
        if os.path.exists(zip_path):
            logging.info(f"[GIÀ PRESENTE] Il file {zip_path} è già stato scaricato.")
            continue
        logging.info(f"Scaricando il file {zip_path}...")
        gdown.download(url=urls[i], output=zip_path, fuzzy=True)
    logging.info("----------Download completato----------")

def extract_archives():
    for i in range(len(directories)):
        folder_path = f"./{directories[i]}"
        if os.path.exists(folder_path):
            logging.info(f"[GIÀ PRESENTE] La directory {folder_path} esiste già.")
            continue
        logging.info(f"Decomprimendo {directories[i]}.7z...")
        with py7zr.SevenZipFile(f'{directories[i]}.7z', mode='r') as archive:
            archive.extractall(path=folder_path)
    logging.info("----------Decompressione completata----------")

def import_csv_files():
    conn = psycopg2.connect(**conn_info)
    cur = conn.cursor()
    create_import_log_table(cur)

    for i in range(len(schemas)):
        numero_file_processati = 0
        cartella_csv = f'./{directories[i]}'
        try:
            cur.execute(f'SET search_path TO {schemas[i]};')
            logging.info(f"----------Import di {schemas[i]} avviato----------")
        except Exception as e:
            logging.error(f"Errore nel settare lo schema {schemas[i]}: {e}")
            raise

        for nome_file in os.listdir(cartella_csv):
            if nome_file.endswith('COPERTURA_DB.CSV'):
                if is_file_already_imported(cur, schemas[i], nome_file):
                    logging.info(f"[SKIPPATO] {nome_file} già importato in {schemas[i]}.")
                    continue
                numero_file_processati += 1
                percorso_file = os.path.join(cartella_csv, nome_file)
                orario_inizio = datetime.now()

                try:
                    with open(percorso_file, 'r', encoding='utf-8') as f:
                        next(f)  # Salta la prima riga (header)
                        try:
                            cur.copy_from(f, 'tbl_italia_contributi', sep=';')
                            conn.commit()  # Commit dopo l'importazione
                            log_import(cur, schemas[i], nome_file)
                            logging.info(f"[COMMIT] File {numero_file_processati} di {schemas[i]}: {nome_file} importato con successo.")
                            
                            # Cleanup: elimina il file dopo il commit
                            logging.info(f"Rimuovendo il file {nome_file} dopo commit...")
                            os.remove(percorso_file)

                        except Exception as e:
                            logging.error(f"Errore importando {nome_file}: {e}")
                            conn.rollback()
                            raise
                except Exception as e:
                    logging.error(f"Errore aprendo {nome_file}: {e}")
                    raise

                durata = round((datetime.now() - orario_inizio).total_seconds(), 2)
                logging.info(f"File {numero_file_processati} di {schemas[i]}: {nome_file} | Durata: {durata}s")

        logging.info(f"----------Import di {schemas[i]} completato----------")

    cur.close()
    conn.close()


def verify_data():
    conn = psycopg2.connect(**conn_info)
    cur = conn.cursor()
    logging.info("----------Verifica dei dati importati----------")
    for schema in schemas:
        try:
            cur.execute(f"SELECT * FROM {schema}.tbl_italia_contributi LIMIT 3;")
            rows = cur.fetchall()
            logging.info(f"\nPrime 3 righe dello schema {schema}:")
            for row in rows:
                logging.info(row)
        except Exception as e:
            logging.error(f"Errore leggendo i dati da {schema}: {e}")
            raise
    cur.close()
    conn.close()

def verify_import():
    conn = psycopg2.connect(**conn_info)
    cur = conn.cursor()
    logging.info("----------Verifica consistenza righe----------")
    for i in range(len(schemas)):
        total_rows_csv = 0
        cartella_csv = f'./{directories[i]}'

        try:
            for nome_file in os.listdir(cartella_csv):
                if nome_file.endswith('COPERTURA_DB.CSV'):
                    with open(os.path.join(cartella_csv, nome_file), 'r', encoding='utf-8') as f:
                        row_count = sum(1 for line in f) - 1
                        total_rows_csv += row_count
        except Exception as e:
            logging.error(f"Errore nel conteggio righe dei CSV per {directories[i]}: {e}")
            raise

        try:
            cur.execute(f"SELECT COUNT(*) FROM {schemas[i]}.tbl_italia_contributi;")
            total_rows_db = cur.fetchone()[0]
        except Exception as e:
            logging.error(f"Errore nel conteggio righe del DB per {schemas[i]}: {e}")
            raise

        if total_rows_csv == total_rows_db:
            logging.info(f"[OK] {schemas[i]}: {total_rows_csv} righe importate correttamente.")
        else:
            msg = f"[ERRORE] {schemas[i]}: {total_rows_csv} nei CSV vs {total_rows_db} nel DB."
            logging.error(msg)
            raise ValueError(msg)

    cur.close()
    conn.close()

def cleanup():
    for directory in directories:
        if os.path.exists(directory):
            logging.info(f"Rimuovendo la cartella {directory}...")
            os.system(f"rm -rf '{directory}'")
    for directory in directories:
        zip_file = f"{directory}.7z"
        if os.path.exists(zip_file):
            logging.info(f"Rimuovendo il file {zip_file}...")
            os.remove(zip_file)

def main():
    try:
        download_files()
        extract_archives()
        import_csv_files()
        # verify_data()
        verify_import()
        # cleanup()
    except Exception as e:
        logging.error(f"Errore durante l'esecuzione dello script: {e}")
        raise

if __name__ == '__main__':
    main()
