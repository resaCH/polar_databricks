import sys, os
sys.path.insert(0, '/workspaces/polar_databricks/src')
import zipfile, json, numpy as np, pandas as pd
from db_loader import DatabricksLoader

zip_pfad = '/workspaces/polar_databricks/input/polar-user-data-export_31872feb-dd92-45be-a696-b0a56975877a.zip'

db = DatabricksLoader()
db.verbinden()

# Bestehende HRV-Daten laden (Duplikate vermeiden)
df_exist = db.abfrage("SELECT datum FROM workspace.polar.hrv")
bekannte = set(str(d) for d in df_exist['datum']) if not df_exist.empty else set()
print(f"Bereits in DB: {len(bekannte)} Tage")

tages_ppi = {}

with zipfile.ZipFile(zip_pfad) as zf:
    ppi_dateien = [n for n in zf.namelist() if n.startswith('ppi_')]
    print(f"PPI-Dateien: {len(ppi_dateien)}")

    for i, name in enumerate(ppi_dateien):
        print(f"  {i+1}/{len(ppi_dateien)}: {name[:50]}")
        try:
            with zf.open(name) as f:
                daten = json.loads(f.read())
            eintraege = daten if isinstance(daten, list) else [daten]
            for eintrag in eintraege:
                datum_str = eintrag.get('date', '')
                if not datum_str or datum_str in bekannte:
                    continue
                ppi_liste = []
                for geraet in eintrag.get('devicePpiSamplesList', []):
                    for probe in geraet.get('ppiSamples', []):
                        ppi_ms = probe.get('pulseLength')
                        if ppi_ms and 300 <= ppi_ms <= 2000:
                            ppi_liste.append(ppi_ms)
                if ppi_liste:
                    if datum_str not in tages_ppi:
                        tages_ppi[datum_str] = []
                    tages_ppi[datum_str].extend(ppi_liste)
        except Exception as e:
            print(f"    Fehler: {e}")
        # RAM freigeben
        import gc; gc.collect()

print(f"\nTage mit PPI-Daten: {len(tages_ppi)}")

# HRV berechnen und in Chunks schreiben
zeilen_sql = []
for datum_str, ppi_werte in tages_ppi.items():
    arr = np.array(ppi_werte[:50000], dtype=float)  # Max 50k Samples
    if len(arr) < 5:
        continue
    diffs  = np.diff(arr)
    rmssd  = round(float(np.sqrt(np.mean(diffs**2))), 2)
    sdnn   = round(float(np.std(arr, ddof=1)), 2)
    mean   = round(float(np.mean(arr)), 1)
    hr_ppi = round(60000 / mean, 1) if mean > 0 else 0
    n      = len(arr)
    zeilen_sql.append(f"('{datum_str}', {rmssd}, {sdnn}, {mean}, {hr_ppi}, {n})")

print(f"HRV-Zeilen zu schreiben: {len(zeilen_sql)}")

for i in range(0, len(zeilen_sql), 50):
    chunk = zeilen_sql[i:i+50]
    db.abfrage(f"""INSERT INTO workspace.polar.hrv
        (datum, hrv_rmssd, hrv_sdnn, ppi_mean_ms, hr_aus_ppi, anzahl_samples)
        VALUES {','.join(chunk)}""")
    print(f"  {i+len(chunk)}/{len(zeilen_sql)} geschrieben")

print("✅ HRV fertig")
db.schliessen()
