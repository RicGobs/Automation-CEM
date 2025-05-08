import os
from datetime import datetime
import psycopg2
import py7zr
import gdown
import threading

# Parametri di connessione al database PostgreSQL
conn_info = {
    'dbname': '',
    'user': '',
    'password': '',
    'host': '',
    'port': ''
}

# Elenco degli schemi del database su cui verranno importati i dati
schemas = ["map_iliad10km",
           "map_tim10km", 
           "map_vodafone10km", 
           "map_windtre10km"]

# Elenco delle directory associate ai vari operatori contenenti i file CSV
directories = ["COPERTURE_10km_v3_ILIAD_LTE-NR_ITALIA",
               "COPERTURE_10km_v3_TIM_LTE-NR_ITALIA",
               "COPERTURE_10km_v3_VODAFONE_LTE-NR_ITALIA",
               "COPERTURE_10km_v3_WINDTRE_LTE-NR_ITALIA"]

# Download dei file compressi (.7z) da Google Drive
urls = ["url_iliad", "url_tim", "url_vodafone", "url_windtre"]  # Inserire gli URL delle cartelle Drive corrispondenti
try:
    for i in range(len(urls)):
        print(f"Scaricando la cartella {directories[i]}")
        gdown.download(url=urls[i], output=f"{directories[i]}.7z", fuzzy=True)
    print('----------File scaricati----------')

except Exception as e:
    print(f"Errore in fase di scaricamento del file drive: {e}")
    raise

# Estrazione dei contenuti dagli archivi .7z
try:
    for i in range(len(directories)):
        print(f"Decomprimendo il file {directories[i]}")
        with py7zr.SevenZipFile(f'{directories[i]}.7z', mode='r') as archive:
            archive.extractall(path=f'./{directories[i]}/')
    print('----------File decompressi----------')

except Exception as e:
    print(f"Errore in fase di decompressione del file drive: {e}")
    raise

# Funzione che gestisce l'importazione dei file CSV
def import_csv_for_schema(schema, directory):
    try:
        # Connessione al database PostgreSQL
        conn = psycopg2.connect(**conn_info)
        cur = conn.cursor()

        # Impostazione dello schema su cui eseguire le operazioni
        cur.execute(f'SET search_path TO {schema};')
        print(f"----------Import di {schema} avviato----------")

        numero_file_processati = 0
        cartella_csv = f'./{directory}'

        try:
            # Scansione dei file nella directory specifica
            for nome_file in os.listdir(cartella_csv):
                if nome_file.endswith('COPERTURA_DB.CSV'):
                    numero_file_processati += 1
                    percorso_file = os.path.join(cartella_csv, nome_file)
                    orario_inizio = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

                    try:
                        # Apertura del file CSV e caricamento dei dati nel database
                        with open(percorso_file, 'r', encoding='utf-8') as f:
                            next(f)  # Salta la riga di intestazione
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

                    # Calcolo e stampa della durata dell’importazione del singolo file
                    orario_fine = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                    inizio_dt = datetime.strptime(orario_inizio, '%Y-%m-%d %H:%M:%S')
                    fine_dt = datetime.strptime(orario_fine, '%Y-%m-%d %H:%M:%S')
                    durata = round((fine_dt - inizio_dt).total_seconds(), 2)

                    print(f"File {numero_file_processati} di {schema}: {nome_file} | Inizio: {orario_inizio} | Fine: {orario_fine} | Durata: {durata} secondi")
                else:
                    # I file che non terminano con 'COPERTURA_DB.CSV' vengono ignorati
                    continue

        except Exception as e:
            print(f"Errore nella lettura della directory {cartella_csv}: {e}")
            raise

        print(f"----------Import di {schema} completato----------")

        # Verifica dei dati importati: visualizzazione delle prime 10 righe per ciascuno schema
        try:
            cur.execute(f"SELECT * FROM {schema}.tbl_italia_contributi LIMIT 10;")
            rows = cur.fetchall()
            for row in rows:
                print(row)
        except Exception as e:
            print(f"Errore nel leggere i dati da {schema}: {e}")
            raise

        # Chiusura del cursore e della connessione al database
        cur.close()
        conn.close()

    except Exception as e:
        print(f"Errore nel processo di importazione per {schema}: {e}")

# Creazione dei thread per ciascun schema
threads = []
for i in range(len(schemas)):
    thread = threading.Thread(target=import_csv_for_schema, args=(schemas[i], directories[i]))
    threads.append(thread)
    thread.start()

# Attendere che tutti i thread finiscano
for thread in threads:
    thread.join()

print("Importazione completata per tutti gli schemi.")
