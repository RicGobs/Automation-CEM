# Automation-CEM

# Importatore di Coperture Reti Mobili in PostgreSQL

Questo script Python automatizza il processo di:
1. Download di archivi `.7z` da Google Drive.
2. Decompressione degli archivi.
3. Importazione dei file CSV nei relativi schemi di un database PostgreSQL.
4. Verifica della correttezza dell'importazione confrontando il numero di righe CSV e quelle nel database.

## 📁 Struttura

- `config.json`: File di configurazione contenente URL dei file e credenziali del database.
- `script.py`: Script principale da eseguire.
- `requirements.txt`: Dipendenze Python necessarie.


## 📦 Installazione

1. Clona questo repository oppure copia i file nel tuo progetto:

   ```bash
   git clone https://github.com/RicGobs/Automation-CEM.git
   cd Automation-CEM
   ```

2. Installa le dipendenze necessarie:

   ```bash
   pip install -r requirements.txt
   ```

3. Avvia lo script per importare i dati nel database:
   ```bash
   cd move_data
   python import_drive_to_db.py
   ```