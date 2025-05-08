import os
from datetime import datetime
import psycopg2
import py7zr
import gdown
import json

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

# Elenco degli schemi del database su cui verranno importati i dati
schemas = ["map_iliad10km", 
           "map_tim10km", 
           "map_vodafone10km", 
           "map_windtre10km"]

# Elenco delle directory associate ai vari operatori contenenti i file CSV
directories = [
    "COPERTURE_10km_v3_ILIAD_LTE-NR_ITALIA",
    "COPERTURE_10km_v3_TIM_LTE-NR_ITALIA",
    "COPERTURE_10km_v3_VODAFONE_LTE-NR_ITALIA",
    "COPERTURE_10km_v3_WINDTRE_LTE-NR_ITALIA"
]

# Funzione per creare la tabella di log se non esiste
def create_import_log_table(cur):
    # Verifica se la tabella esiste
    cur.execute("""
        SELECT to_regclass('public.import_log_tbl_italia_contributi');
    """)
    table_exists = cur.fetchone()[0] is not None

    if table_exists:
        # Controlla se la tabella è vuota
        cur.execute("SELECT COUNT(*) FROM public.import_log_tbl_italia_contributi;")
        row_count = cur.fetchone()[0]

        if row_count == 0:
            # Elimina la tabella
            cur.execute("DROP TABLE public.import_log_tbl_italia_contributi;")
            cur.connection.commit()
        else:
            # La tabella esiste e non è vuota, non fare nulla
            return

    # Crea la tabella (nuova o non esistente)
    cur.execute("""
        CREATE TABLE IF NOT EXISTS public.import_log_tbl_italia_contributi (
            id SERIAL PRIMARY KEY,
            schema_name TEXT,
            file_name TEXT,
            imported_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
    """)
    cur.connection.commit()

# Funzione per verificare se un file è già stato importato in uno schema specifico
def is_file_already_imported(cur, schema_name, file_name):
    cur.execute("""
        SELECT 1 FROM public.import_log_tbl_italia_contributi 
        WHERE schema_name = %s AND file_name = %s
    """, (schema_name, file_name))
    return cur.fetchone() is not None

# Funzione per registrare l'importazione di un file nel log
def log_import(cur, schema_name, file_name):
    cur.execute("""
        INSERT INTO public.import_log_tbl_italia_contributi (schema_name, file_name)
        VALUES (%s, %s)
    """, (schema_name, file_name))
    cur.connection.commit()

# Funzione per scaricare i file da Google Drive se non esistono già localmente
def download_files():
    for i in range(len(urls)):
        zip_path = f"{directories[i]}.7z"
        folder_path = directories[i]

        # Controlla prima se la cartella esiste già
        if os.path.exists(folder_path):
            print(f"[GIÀ PRESENTE] La directory {folder_path} esiste già. Salto download del file .7z.")
            continue

        # Se la cartella non esiste ma il file .7z esiste già, non scaricarlo di nuovo
        if os.path.exists(zip_path):
            print(f"[GIÀ PRESENTE] Il file {zip_path} è già stato scaricato.")
            continue

        print(f"Scaricando il file {zip_path}...")
        gdown.download(url=urls[i], output=zip_path, fuzzy=True)

    print('----------Download completato----------')

# Funzione per decomprimere i file .7z se non sono già stati estratti
def extract_archives():
    for i in range(len(directories)):
        folder_path = f"./{directories[i]}"
        if os.path.exists(folder_path):
            print(f"[GIÀ PRESENTE] La directory {folder_path} esiste già.")
            continue
        print(f"Decomprimendo il file {directories[i]}.7z...")
        with py7zr.SevenZipFile(f'{directories[i]}.7z', mode='r') as archive:
            archive.extractall(path='./')
    print('----------Decompressione completata----------')

# Funzione per importare i CSV nel database e loggare ogni file importato
def import_csv_files():
    conn = psycopg2.connect(**conn_info)
    cur = conn.cursor()

    create_import_log_table(cur)

    for i in range(len(schemas)):
        numero_file_processati = 0
        cartella_csv = f'./{directories[i]}'

        try:
            cur.execute(f'SET search_path TO {schemas[i]};')
            print(f"----------Import di {schemas[i]} avviato----------")
        except Exception as e:
            print(f"Errore nel settare lo schema {schemas[i]}: {e}")
            raise

        for nome_file in os.listdir(cartella_csv):
            if nome_file.endswith('COPERTURA_DB.CSV'):

                if is_file_already_imported(cur, schemas[i], nome_file):
                    print(f"[SKIPPATO] {nome_file} è già stato importato in {schemas[i]}.")
                    continue

                numero_file_processati += 1
                percorso_file = os.path.join(cartella_csv, nome_file)
                orario_inizio = datetime.now()

                try:
                    with open(percorso_file, 'r', encoding='utf-8') as f:
                        next(f)
                        try:
                            cur.copy_from(f, 'tbl_italia_contributi', sep=';')
                            conn.commit()
                        except Exception as e:
                            print(f"Errore durante l'importazione del file {nome_file}: {e}")
                            conn.rollback()
                            raise
                except Exception as e:
                    print(f"Errore nell'apertura del file {nome_file}: {e}")
                    raise

                log_import(cur, schemas[i], nome_file)
                orario_fine = datetime.now()
                durata = round((orario_fine - orario_inizio).total_seconds(), 2)

                print(f"File {numero_file_processati} di {schemas[i]}: {nome_file} | Durata: {durata} secondi")

        print(f"----------Import di {schemas[i]} completato----------")

    cur.close()
    conn.close()

# Funzione per verificare i dati importati: visualizzazione delle prime 3 righe per ciascuno schema
def verify_data():
    conn = psycopg2.connect(**conn_info)
    cur = conn.cursor()

    print("\n----------Verifica dei dati importati----------")
    for i in range(len(schemas)):
        try:
            cur.execute(f"SELECT * FROM {schemas[i]}.tbl_italia_contributi LIMIT 3;")
            rows = cur.fetchall()
            print(f"\nPrime 3 righe dello schema {schemas[i]}:")
            for row in rows:
                print(row)
        except Exception as e:
            print(f"Errore nel leggere i dati da {schemas[i]}: {e}")
            raise

    cur.close()
    conn.close()

# Funzione per contare le righe nei CSV e confrontarle con quelle nel DB
def verify_import():
    conn = psycopg2.connect(**conn_info)
    cur = conn.cursor()

    print("\n----------Verifica consistenza righe----------")
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
            print(f"Errore nel conteggio righe dei CSV per {directories[i]}: {e}")
            raise

        try:
            cur.execute(f"SELECT COUNT(*) FROM {schemas[i]}.tbl_italia_contributi;")
            total_rows_db = cur.fetchone()[0]
        except Exception as e:
            print(f"Errore nel conteggio righe del DB per {schemas[i]}: {e}")
            raise

        if total_rows_csv == total_rows_db:
            print(f"[OK] {schemas[i]}: {total_rows_csv} righe importate correttamente.")
        else:
            error_message = f"[ERRORE] {schemas[i]}: {total_rows_csv} righe nei CSV vs {total_rows_db} nel DB."
            print(error_message)
            raise ValueError(error_message)  # Lancia un'eccezione per interrompere l'esecuzione

    cur.close()
    conn.close()


# Funzione per pulire file e cartelle
def cleanup():
    # Elimina le cartelle estratte
    for directory in directories:
        if os.path.exists(directory):
            print(f"Rimuovendo la cartella {directory}...")
            os.system(f"rm -rf '{directory}'")

    # Elimina i file zip
    for directory in directories:
        zip_file = f"{directory}.7z"
        if os.path.exists(zip_file):
            print(f"Rimuovendo il file {zip_file}...")
            os.remove(zip_file)

# Funzione principale che coordina tutte le operazioni
def main():
    try:
        download_files()
        extract_archives()
        import_csv_files()
        verify_data() 
        verify_import()
        #cleanup()
    except Exception as e:
        print(f"Errore durante l'esecuzione dello script: {e}")
        raise


if __name__ == '__main__':
    main()
