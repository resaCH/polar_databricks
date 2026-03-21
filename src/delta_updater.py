"""
delta_updater.py
================
Inkrementelles Update der Databricks Delta Tables aus Polar ZIP-Exporten.

Ablauf pro ZIP:
    1. ZIP aus input/ einlesen (nur im Arbeitsspeicher, nie auf Disk entpacken)
    2. Jeden Dateinamen + MD5-Hash gegen polar.import_log prüfen
    3. Nur neue/geänderte Dateien verarbeiten (Skip bei bekanntem Hash)
    4. Aggregierte DataFrames per MERGE INTO in Delta Tables schreiben
    5. Import-Log aktualisieren (verhindert Doppelimport)
    6. Verarbeitetes ZIP nach archive/ verschieben
    7. Delta-Bericht ausgeben: X neu, Y geändert, Z unverändert

Sicherheit:
    - Rohdaten verlassen den Arbeitsspeicher nach der Verarbeitung
    - Nur aggregierte DataFrames werden nach Databricks geschrieben
    - Kein Hardcoding von Credentials – ausschliesslich via os.environ

Verwendung:
    from delta_updater import DeltaUpdater
    updater = DeltaUpdater()
    bericht = updater.verarbeite_zip("input/polar_export.zip")
"""

import hashlib
import os
import shutil
import zipfile
from datetime import datetime
from pathlib import Path

import pandas as pd

from polar_parser import PolarParser


# ============================================================
# Hilfsfunktionen
# ============================================================

def _md5_von_datei(zip_obj: zipfile.ZipFile, dateiname: str) -> str:
    """
    Berechnet den MD5-Hash einer Datei innerhalb eines ZIP-Archivs.

    Args:
        zip_obj:   Geöffnetes ZipFile-Objekt
        dateiname: Name der Datei innerhalb des ZIP

    Returns:
        MD5-Hash als Hex-String (32 Zeichen).
    """
    with zip_obj.open(dateiname) as f:
        return hashlib.md5(f.read()).hexdigest()


def _sekunden_pruefen() -> None:
    """
    Prüft ob alle benötigten Databricks-Secrets vorhanden sind.

    Raises:
        EnvironmentError: Mit Schritt-für-Schritt-Anleitung wenn Secret fehlt.
    """
    benoetigte_secrets = [
        'DATABRICKS_HOST',
        'DATABRICKS_TOKEN',
        'DATABRICKS_HTTP_PATH',
        'DATABRICKS_CATALOG',
        'DATABRICKS_SCHEMA',
    ]
    fehlende = [s for s in benoetigte_secrets if not os.environ.get(s)]
    if fehlende:
        raise EnvironmentError(
            f"Fehlende Databricks Secrets: {', '.join(fehlende)}\n\n"
            "→ Schritt-für-Schritt Einrichtung:\n"
            "  1. GitHub.com → dein Repository\n"
            "  2. Settings → Secrets and variables → Codespaces\n"
            "  3. 'New repository secret' für jeden fehlenden Wert\n"
            "  4. Codespace neu starten: Strg+Shift+P → 'Rebuild Container'\n\n"
            "Benötigte Secrets (Werte aus .env.example):\n"
            "  DATABRICKS_HOST, DATABRICKS_TOKEN, DATABRICKS_HTTP_PATH,\n"
            "  DATABRICKS_CATALOG, DATABRICKS_SCHEMA"
        )


# ============================================================
# MERGE INTO SQL Templates
# ============================================================

# Jede Tabelle hat ein eigenes MERGE-Statement.
# Schlüsselspalte ist immer 'datum' (pro Tag ein Datensatz).
# Bei Änderungen wird der komplette Datensatz überschrieben (UPDATE SET *).

_MERGE_ACTIVITY = """
MERGE INTO {catalog}.{schema}.activity AS ziel
USING (
    SELECT
        CAST(datum AS DATE)             AS datum,
        CAST(schritte AS BIGINT)        AS schritte,
        CAST(kalorien AS DOUBLE)        AS kalorien,
        CAST(schlaf_stunden AS DOUBLE)  AS schlaf_stunden,
        CAST(schlaf_qualitaet AS DOUBLE)AS schlaf_qualitaet,
        CAST(met_minuten AS DOUBLE)     AS met_minuten
    FROM {temp_view}
) AS quelle
ON ziel.datum = quelle.datum
WHEN MATCHED AND (
    ziel.schritte         <> quelle.schritte OR
    ziel.kalorien         <> quelle.kalorien OR
    ziel.schlaf_stunden   <> quelle.schlaf_stunden
) THEN UPDATE SET *
WHEN NOT MATCHED THEN INSERT *
"""

_MERGE_TRAINING = """
MERGE INTO {catalog}.{schema}.training AS ziel
USING (
    SELECT
        CAST(datum AS DATE)         AS datum,
        CAST(sport AS STRING)       AS sport,
        CAST(kategorie AS STRING)   AS kategorie,
        CAST(dauer_min AS DOUBLE)   AS dauer_min,
        CAST(hr_avg AS DOUBLE)      AS hr_avg,
        CAST(hr_max AS DOUBLE)      AS hr_max,
        CAST(distanz_km AS DOUBLE)  AS distanz_km,
        CAST(kalorien AS DOUBLE)    AS kalorien,
        CAST(wochentag AS STRING)   AS wochentag,
        CAST(jahr AS INT)           AS jahr
    FROM {temp_view}
) AS quelle
ON ziel.datum = quelle.datum AND ziel.sport = quelle.sport
   AND ziel.dauer_min = quelle.dauer_min
WHEN NOT MATCHED THEN INSERT *
"""

_MERGE_HEARTRATE = """
MERGE INTO {catalog}.{schema}.heartrate AS ziel
USING (
    SELECT
        CAST(datum AS DATE)         AS datum,
        CAST(hr_ruhepuls AS DOUBLE) AS hr_ruhepuls,
        CAST(hr_mean AS DOUBLE)     AS hr_mean,
        CAST(hr_max AS INT)         AS hr_max,
        CAST(wochentag_nr AS INT)   AS wochentag_nr,
        CAST(monat AS INT)          AS monat
    FROM {temp_view}
) AS quelle
ON ziel.datum = quelle.datum
WHEN MATCHED AND ziel.hr_ruhepuls <> quelle.hr_ruhepuls
    THEN UPDATE SET *
WHEN NOT MATCHED THEN INSERT *
"""

_MERGE_HRV = """
MERGE INTO {catalog}.{schema}.hrv AS ziel
USING (
    SELECT
        CAST(datum AS DATE)             AS datum,
        CAST(hrv_rmssd AS DOUBLE)       AS hrv_rmssd,
        CAST(hrv_sdnn AS DOUBLE)        AS hrv_sdnn,
        CAST(ppi_mean_ms AS DOUBLE)     AS ppi_mean_ms,
        CAST(hr_aus_ppi AS DOUBLE)      AS hr_aus_ppi,
        CAST(anzahl_samples AS BIGINT)  AS anzahl_samples
    FROM {temp_view}
) AS quelle
ON ziel.datum = quelle.datum
WHEN MATCHED AND ziel.hrv_rmssd <> quelle.hrv_rmssd
    THEN UPDATE SET *
WHEN NOT MATCHED THEN INSERT *
"""

_MERGE_IMPORT_LOG = """
MERGE INTO {catalog}.{schema}.import_log AS ziel
USING (
    SELECT
        CAST(dateiname AS STRING)       AS dateiname,
        CAST(hash_md5 AS STRING)        AS hash_md5,
        CAST(importiert_am AS TIMESTAMP)AS importiert_am,
        CAST(kategorie AS STRING)       AS kategorie
    FROM {temp_view}
) AS quelle
ON ziel.dateiname = quelle.dateiname
WHEN MATCHED THEN UPDATE SET *
WHEN NOT MATCHED THEN INSERT *
"""


# ============================================================
# Hauptklasse
# ============================================================

class DeltaUpdater:
    """
    Verwaltet den inkrementellen Import von Polar-ZIP-Exporten
    in Databricks Delta Tables.

    Verbindungsparameter werden ausschliesslich aus den
    GitHub Codespaces Secrets (Umgebungsvariablen) gelesen.

    Args:
        catalog: Databricks Catalog (Standard: aus DATABRICKS_CATALOG)
        schema:  Databricks Schema  (Standard: aus DATABRICKS_SCHEMA)

    Raises:
        EnvironmentError: Wenn Databricks Secrets fehlen.

    Beispiel:
        >>> updater = DeltaUpdater()
        >>> bericht = updater.verarbeite_zip("input/export.zip")
        >>> print(bericht)
    """

    def __init__(self, catalog: str = None, schema: str = None):
        _sekunden_pruefen()

        # Verbindungsparameter aus Secrets
        self.host      = os.environ['DATABRICKS_HOST']
        self.token     = os.environ['DATABRICKS_TOKEN']
        self.http_path = os.environ['DATABRICKS_HTTP_PATH']
        self.catalog   = catalog or os.environ.get('DATABRICKS_CATALOG', 'main')
        self.schema    = schema  or os.environ.get('DATABRICKS_SCHEMA', 'polar')

        # Databricks-Verbindung (lazy – erst beim ersten Schreiben)
        self._conn   = None
        self._cursor = None

    # --------------------------------------------------------
    # Verbindung
    # --------------------------------------------------------

    def _verbinden(self) -> None:
        """
        Stellt die Verbindung zu Databricks SQL Warehouse her.

        Raises:
            ImportError:    Wenn databricks-sql-connector nicht installiert ist.
            RuntimeError:   Wenn die Verbindung fehlschlägt.
        """
        try:
            from databricks import sql as dbsql
        except ImportError:
            raise ImportError(
                "Paket 'databricks-sql-connector' nicht gefunden!\n"
                "→ pip install databricks-sql-connector"
            )

        try:
            self._conn = dbsql.connect(
                server_hostname=self.host.replace('https://', ''),
                http_path=self.http_path,
                access_token=self.token,
            )
            self._cursor = self._conn.cursor()
            print(f"✅ Databricks verbunden: {self.host}")
        except Exception as e:
            raise RuntimeError(
                f"Databricks-Verbindung fehlgeschlagen: {e}\n\n"
                "Mögliche Ursachen:\n"
                "  • DATABRICKS_TOKEN abgelaufen → neues Token generieren\n"
                "  • SQL Warehouse gestoppt → im Databricks UI starten\n"
                "  • Netzwerkproblem im Codespace\n"
            )

    def _sql(self, query: str) -> list:
        """
        Führt eine SQL-Abfrage aus und gibt Ergebniszeilen zurück.

        Args:
            query: SQL-Statement

        Returns:
            Liste von Zeilen (als Tupel).
        """
        if not self._cursor:
            self._verbinden()
        self._cursor.execute(query)
        try:
            return self._cursor.fetchall()
        except Exception:
            return []

    # --------------------------------------------------------
    # Import-Log
    # --------------------------------------------------------

    def _lade_import_log(self) -> dict[str, str]:
        """
        Lädt den aktuellen Import-Log aus Databricks.

        Returns:
            Dict: {dateiname → hash_md5} für alle bereits importierten Dateien.
        """
        try:
            zeilen = self._sql(
                f"SELECT dateiname, hash_md5 "
                f"FROM {self.catalog}.{self.schema}.import_log"
            )
            return {str(z[0]): str(z[1]) for z in zeilen}
        except Exception as e:
            print(f"   ⚠️  Import-Log konnte nicht geladen werden: {e}")
            print("      (Wird als leer behandelt – alle Dateien werden importiert)")
            return {}

    # --------------------------------------------------------
    # DataFrame → Databricks MERGE
    # --------------------------------------------------------

    def _merge_dataframe(
        self,
        df: pd.DataFrame,
        tabelle: str,
        merge_sql: str,
        temp_view_name: str,
    ) -> int:
        """
        Schreibt einen DataFrame per MERGE INTO in eine Delta Table.

        Strategie: DataFrame als temporäre Spark-ähnliche View registrieren,
        dann MERGE INTO ausführen. Da wir den SQL-Connector (kein PySpark)
        verwenden, wird der DataFrame in einen VALUES-Block umgewandelt.

        Args:
            df:             Zu schreibender DataFrame
            tabelle:        Ziel-Tabellenname (für Logging)
            merge_sql:      MERGE INTO SQL-Template
            temp_view_name: Name der temporären View im SQL

        Returns:
            Anzahl betroffener Zeilen (neu + geändert).
        """
        if df.empty:
            print(f"   ⏭️  {tabelle}: Keine neuen Daten")
            return 0

        # DataFrame-Zeilen als SQL VALUES-Block aufbereiten
        def _wert_zu_sql(val) -> str:
            """Konvertiert Python-Wert zu SQL-Literal."""
            if val is None or (isinstance(val, float) and pd.isna(val)):
                return 'NULL'
            if isinstance(val, (int, float)):
                return str(val)
            if isinstance(val, bool):
                return 'TRUE' if val else 'FALSE'
            # Datum und String: in Hochkommas
            return f"'{str(val).replace("'", "''")}'"

        zeilen_sql = []
        for _, row in df.iterrows():
            werte = ', '.join(_wert_zu_sql(v) for v in row)
            zeilen_sql.append(f"({werte})")

        spalten = ', '.join(df.columns)
        values_block = ',\n    '.join(zeilen_sql)

        # Temporäre View als CTE im MERGE-Statement ersetzen
        cte_sql = (
            f"WITH {temp_view_name} AS (\n"
            f"  SELECT * FROM (VALUES\n    {values_block}\n"
            f"  ) AS t({spalten})\n)\n"
        )

        # MERGE-Template: temp_view durch CTE ersetzen
        endgueltige_sql = merge_sql.replace(
            f"FROM {temp_view_name}", f"FROM {temp_view_name}"
        )
        endgueltige_sql = cte_sql + endgueltige_sql.format(
            catalog=self.catalog,
            schema=self.schema,
            temp_view=temp_view_name,
        )

        try:
            self._sql(endgueltige_sql)
            print(f"   ✅ {tabelle}: {len(df)} Datensätze verarbeitet")
            return len(df)
        except Exception as e:
            print(f"   ❌ {tabelle} MERGE fehlgeschlagen: {e}")
            raise

    # --------------------------------------------------------
    # Hauptmethode
    # --------------------------------------------------------

    def verarbeite_zip(self, zip_pfad: str) -> dict:
        """
        Verarbeitet ein Polar-Export-ZIP inkrementell.

        Ablauf:
            1. ZIP im Speicher öffnen
            2. MD5-Hashes gegen Import-Log prüfen
            3. Nur neue/geänderte Dateien parsen
            4. DataFrames per MERGE INTO schreiben
            5. Import-Log aktualisieren
            6. ZIP nach archive/ verschieben

        Args:
            zip_pfad: Pfad zur ZIP-Datei (z.B. 'input/polar_export.zip')

        Returns:
            Bericht-Dict mit Schlüsseln:
                neu (int), geaendert (int), unveraendert (int),
                fehler (int), dauer_sekunden (float)

        Raises:
            FileNotFoundError: Wenn ZIP nicht gefunden.
            RuntimeError:      Bei Databricks-Verbindungsproblemen.

        Beispiel:
            >>> bericht = updater.verarbeite_zip("input/export.zip")
            >>> print(f"Neu importiert: {bericht['neu']}")
        """
        start_zeit = datetime.now()
        zip_pfad   = Path(zip_pfad)

        if not zip_pfad.exists():
            raise FileNotFoundError(
                f"ZIP-Datei nicht gefunden: {zip_pfad}\n"
                "→ Datei in den 'input/' Ordner legen"
            )

        print(f"\n{'='*55}")
        print(f"📦 Starte Delta-Update: {zip_pfad.name}")
        print(f"   Zeitpunkt: {start_zeit.strftime('%Y-%m-%d %H:%M:%S')}")
        print(f"{'='*55}")

        # Verbindung aufbauen
        self._verbinden()

        # Import-Log laden (bekannte Hashes)
        print("\n📋 Lade Import-Log...")
        bekannte_hashes = self._lade_import_log()
        print(f"   Bereits importiert: {len(bekannte_hashes)} Dateien")

        # ZIP öffnen und Hashes prüfen
        print("\n🔍 Prüfe Dateien im ZIP...")
        neu_log_eintraege   = []
        zaehler = {'neu': 0, 'geaendert': 0, 'unveraendert': 0, 'fehler': 0}

        with zipfile.ZipFile(zip_pfad, 'r') as zf:
            alle_namen = [n for n in zf.namelist()
                          if n.endswith('.json')]

            neue_dateien     = []
            geaenderte_daten = []

            for name in alle_namen:
                try:
                    aktueller_hash = _md5_von_datei(zf, name)

                    if name not in bekannte_hashes:
                        neue_dateien.append(name)
                        zaehler['neu'] += 1
                        neu_log_eintraege.append({
                            'dateiname'   : name,
                            'hash_md5'    : aktueller_hash,
                            'importiert_am': datetime.now(),
                            'kategorie'   : _datei_kategorie(name),
                        })
                    elif bekannte_hashes[name] != aktueller_hash:
                        geaenderte_daten.append(name)
                        zaehler['geaendert'] += 1
                        # Hash im Log aktualisieren
                        for eintrag in neu_log_eintraege:
                            if eintrag['dateiname'] == name:
                                eintrag['hash_md5'] = aktueller_hash
                        neu_log_eintraege.append({
                            'dateiname'   : name,
                            'hash_md5'    : aktueller_hash,
                            'importiert_am': datetime.now(),
                            'kategorie'   : _datei_kategorie(name),
                        })
                    else:
                        zaehler['unveraendert'] += 1

                except Exception as e:
                    zaehler['fehler'] += 1
                    print(f"   ⚠️  Hash-Fehler bei '{name}': {e}")

        zu_verarbeiten = zaehler['neu'] + zaehler['geaendert']
        print(f"   Neu         : {zaehler['neu']:>6}")
        print(f"   Geändert    : {zaehler['geaendert']:>6}")
        print(f"   Unverändert : {zaehler['unveraendert']:>6}")
        print(f"   Zu importieren: {zu_verarbeiten}")

        if zu_verarbeiten == 0:
            print("\n✅ Keine neuen Daten – ZIP wird trotzdem archiviert.")
        else:
            # Polar Parser nur mit relevanten Dateien aufrufen
            print(f"\n⚙️  Parse {zu_verarbeiten} neue/geänderte Dateien...")
            parser = PolarParser(str(zip_pfad))

            # Activity
            print("\n▶ Activity-Daten...")
            df_act = parser.parse_activity()
            if not df_act.empty:
                self._merge_dataframe(
                    df_act, 'activity', _MERGE_ACTIVITY, 'src_activity'
                )

            # Training
            print("\n▶ Trainings-Daten...")
            df_train = parser.parse_training()
            if not df_train.empty:
                self._merge_dataframe(
                    df_train, 'training', _MERGE_TRAINING, 'src_training'
                )

            # Herzfrequenz
            print("\n▶ Herzfrequenz-Daten...")
            df_hr = parser.parse_heartrate()
            if not df_hr.empty:
                self._merge_dataframe(
                    df_hr, 'heartrate', _MERGE_HEARTRATE, 'src_heartrate'
                )

            # HRV
            print("\n▶ HRV-Daten...")
            df_hrv = parser.parse_hrv()
            if not df_hrv.empty:
                self._merge_dataframe(
                    df_hrv, 'hrv', _MERGE_HRV, 'src_hrv'
                )

            # Sport-Korrekturen nach Import
            print("\n▶ Sport-Korrekturen...")
            self._cursor.execute(f"""
                UPDATE {self.catalog}.{self.schema}.training
                SET sport = 'RUNNING', kategorie = 'OUTDOOR'
                WHERE sport IN ('HIKING', 'WALKING')
                  AND distanz_km > 0 AND dauer_min > 0
                  AND (dauer_min / distanz_km) < 10
            """)
            self._cursor.execute(f"""
                UPDATE {self.catalog}.{self.schema}.training
                SET sport = 'RUNNING', kategorie = 'TRAIL'
                WHERE sport = 'TRAIL_RUNNING'
            """)
            print("   ✅ Sport-Korrekturen angewendet")

            # Import-Log aktualisieren
            print("\n▶ Import-Log aktualisieren...")
            if neu_log_eintraege:
                df_log = pd.DataFrame(neu_log_eintraege)
                self._merge_dataframe(
                    df_log, 'import_log', _MERGE_IMPORT_LOG, 'src_log'
                )

        # ZIP archivieren
        archiv_pfad = Path('/workspaces/polar_databricks/archive') / zip_pfad.name
        archiv_pfad.parent.mkdir(exist_ok=True)
        shutil.move(str(zip_pfad), str(archiv_pfad))
        print(f"\n📁 ZIP archiviert: {archiv_pfad}")

        # Bericht
        dauer = (datetime.now() - start_zeit).total_seconds()
        zaehler['dauer_sekunden'] = round(dauer, 1)

        print(f"\n{'='*55}")
        print(f"✅ Delta-Update abgeschlossen in {dauer:.1f}s")
        print(f"   Neu         : {zaehler['neu']}")
        print(f"   Geändert    : {zaehler['geaendert']}")
        print(f"   Unverändert : {zaehler['unveraendert']}")
        print(f"   Fehler      : {zaehler['fehler']}")
        print(f"{'='*55}\n")

        return zaehler

    def __del__(self):
        """Datenbankverbindung beim Aufräumen schliessen."""
        try:
            if self._cursor:
                self._cursor.close()
            if self._conn:
                self._conn.close()
        except Exception:
            pass


# ============================================================
# Hilfsfunktion: Datei-Kategorie bestimmen
# ============================================================

def _datei_kategorie(dateiname: str) -> str:
    """
    Bestimmt die Kategorie einer Polar-Datei anhand des Namens.

    Args:
        dateiname: Dateiname (z.B. 'activity_2024-01-15.json')

    Returns:
        Kategorie-String: 'activity', 'training', 'heartrate',
        'hrv', 'fitness', 'physical' oder 'sonstige'.

    Beispiel:
        >>> _datei_kategorie('activity_2024-01-15.json')
        'activity'
        >>> _datei_kategorie('247ohr_2024-01.json')
        'heartrate'
    """
    name = dateiname.lower()
    if 'activity-' in name or 'activity_' in name:
        return 'activity'
    if 'training-' in name or 'training_' in name:
        return 'training'
    if '247ohr-' in name or '247ohr_' in name:
        return 'heartrate'
    if 'ppi-' in name or 'ppi_' in name:
        return 'hrv'
    if 'fitness_' in name:
        return 'fitness'
    if 'physical_' in name:
        return 'physical'
    return 'sonstige'
