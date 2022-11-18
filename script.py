'''
Find row as this
[2022\10\13 14:21.58] Program compiled on field 1:C:\CAM3D\Temp\00362_989029_0\2.mpf
in files ProductionLog_[month].txt
and write avanzamento record on euroDB

INSERT INTO eurodb.tb_avanzamento
(codice_operatore, codice_fase, inizio_fine, numero_ordine, lotto_ordine, `timestamp`, secondi, bilancelle, carrello, multiordine)
VALUES('CNC01', 'A1', b'0', 0, '', CURRENT_TIMESTAMP, 0, 0.0, '', 1);

'''

import os, sys
import configparser
import glob
import re
from datetime import datetime

from db import EuroDB

script_dir = os.getcwd()
config = configparser.ConfigParser()
config.read(os.path.join(script_dir, "config.ini"))
pattern = re.compile("(?P<ts>\d{4}.\d{2}.\d{2} \d{2}:\d{2}\.\d{2}).*(?P<numero>\d{6})_(?P<lotto>[0-9A-Z])")
# pattern = re.compile('(?P<anno>\d{4}).(?P<mese>\d{2}).(?P<giorno>\d{2})')

logs_registrati = {}
logs_macchina = {}
count_add = 0
count_update = 0
db = EuroDB(config['mysql']['user'],config['mysql']['password'],config['mysql']['host'],config['mysql']['database'])

VERBOSE = config['info']['verbose']

def verbose(s: str) -> None:
    if VERBOSE:
        print(s)

def normalize_numero(numero: str) -> int:
    if int(numero) > 800000:
        return int(f"8{str(numero)[1:]}")
    return numero

def get_logs_macchina() -> None:
    logs_path = config['paths']['logs']
    for f in glob.glob(f"{logs_path}\\ProductionLog_*.txt"):
        parse_file(os.path.join(logs_path, f))

def parse_file(file_path) -> None:
    for line in open(file_path, 'r'):
        m = pattern.search(line) 
        if m: # [2022\10\13 14:21.58] Program compiled on field 1:C:\CAM3D\Temp\00362_989029_0\2.mpf
            orario = datetime.strptime(m.group('ts'), "%Y\\%m\\%d %H:%M.%S")
            numero = normalize_numero(m.group('numero'))
            lotto = m.group('lotto')
            ordine = f"{numero}_{lotto}"
            if ordine in logs_macchina:
                logs_macchina[ordine]['fine'] = {'ts' : orario, 'numero': numero, 'lotto': lotto}
            else:
                logs_macchina[ordine] = {'inizio' : {'ts' : orario, 'numero': numero, 'lotto': lotto}}

def get_seconds(before: datetime, after: datetime) -> int:
    if before != after: #return 1 always
        return 1
    difference = after-before
    return difference.seconds + (difference.days * 8 * 60 * 60) # considero 8 ore max al giorno


def main() -> None:
    get_logs_macchina()
    logs_registrati = db.get_log()
    for k, v in logs_macchina.items():
        if k not in logs_registrati:
            verbose(f"Non trovato {k}. Registro Inizio e fine")
            verbose(f"Dati: {v}")
            db.add_log(v['inizio']['ts'], v['inizio']['numero'], v['inizio']['lotto'], fine = False)
            count_add += 1
            if 'fine' in v: 
                s = get_seconds(v['inizio']['ts'],v['fine']['ts'])
                db.add_log(v['fine']['ts'], v['fine']['numero'], v['fine']['lotto'], fine = True, secondi= s)
                count_add += 1
        else:
            id = logs_registrati[k]['max_id']
            ts = logs_registrati[k]['ts']
            ts_macchina = v['fine']['ts']
            if ts_macchina > ts and logs_registrati[k]['rec_count'] == 2:
                db.update_log(id, v['fine']['ts'], 1)
                verbose(f"Aggiornato fine lavoro {k} id:{id}")
                count_update += 1

    db.connection.commit()
    db.connection.close()

if __name__ == '__main__':
    main()
    verbose(f"Aggiunte {count_add} registrazioni e aggiornate {count_update} registrazioni.")