"""
db_loader.py
============
Databricks-Verbindung und Daten laden für die Analyse-Notebooks.

Stellt eine einfache, einheitliche Schnittstelle bereit um:
    - Verbindung zu Databricks SQL Warehouse herzustellen
    - Delta Tables als pandas DataFrames zu laden
    - Häufig benötigte Abfragen als vorgefertigte Methoden anzubieten
    - Verbindungsstatus und Tabellenübersicht anzuzeigen

Sicherheit:
    - Alle Credentials ausschliesslich via GitHub Codespaces Secrets
    - Kein Hardcoding von Tokens, Passwörtern oder Verbindungsstrings
    - Nur lesende Zugriffe (SELECT) – kein Schreiben via db_loader

Verwendung:
    from db_loader import DatabricksLoader
    db = DatabricksLoader()
    df_activity  = db.lade_activity()
    df_training  = db.lade_training()
    df_heartrate = db.lade_heartrate()
    df_hrv       = db.lade_hrv()
"""

import os
from datetime import date
from typing import Optional

import pandas as pd


# ============================================================
# Secrets-Prüfung
# ============================================================

def secrets_pruefen() -> None:
    """
    Prüft ob alle benötigten Databricks-Secrets als Umgebungsvariablen
    vorhanden sind. Gibt bei Fehler eine klare Schritt-für-Schritt-
    Anleitung aus.

    Raises:
        EnvironmentError: Mit Anleitung wenn ein Secret fehlt.

    Beispiel:
        >>> from db_loader import secrets_pruefen
        >>> secrets_pruefen()  # Wirft Fehler wenn Secrets fehlen
    """
    benoetigte = {
        'DATABRICKS_HOST'     : 'https://dbc-XXXXXXXX.cloud.databricks.com',
        'DATABRICKS_TOKEN'    : 'dapi_XXXXXXXXXXXXXXXX',
        'DATABRICKS_HTTP_PATH': '/sql/1.0/warehouses/XXXXXXXXXXXXXXXX',
        'DATABRICKS_CATALOG'  : 'main',
        'DATABRICKS_SCHEMA'   : 'polar',
    }
    fehlende = {k: v for k, v in benoetigte.items() if not os.environ.get(k)}

    if fehlende:
        anleitung = '\n'.join(f'  {k}={v}' for k, v in fehlende.items())
        raise EnvironmentError(
            f"\n{'='*55}\n"
            f"❌ Fehlende Databricks Secrets:\n{anleitung}\n\n"
            f"→ Schritt-für-Schritt Einrichtung:\n"
            f"  1. https://github.com → dein Repository öffnen\n"
            f"  2. Settings → Secrets and variables → Codespaces\n"
            f"  3. Für jeden fehlenden Wert: 'New repository secret'\n"
            f"  4. Codespace neu starten:\n"
            f"     Strg+Shift+P → 'Codespaces: Rebuild Container'\n"
            f"{'='*55}"
        )
    print("✅ Alle Databricks Secrets gefunden.")


# ============================================================
# Hauptklasse
# ============================================================

class DatabricksLoader:
    """
    Lädt Daten aus den Polar Delta Tables in Databricks.

    Verbindungsparameter werden ausschliesslich aus den
    GitHub Codespaces Secrets (Umgebungsvariablen) gelesen.
    Die Verbindung wird beim ersten Datenzugriff aufgebaut (lazy).

    Args:
        catalog: Databricks Catalog (Standard: DATABRICKS_CATALOG Secret)
        schema:  Databricks Schema  (Standard: DATABRICKS_SCHEMA Secret)

    Raises:
        EnvironmentError: Wenn Databricks Secrets fehlen.

    Beispiel:
        >>> db = DatabricksLoader()
        >>> df = db.lade_activity()
        >>> df.shape
        (4000, 6)
    """

    def __init__(self, catalog: str = None, schema: str = None):
        secrets_pruefen()

        self._host      = os.environ['DATABRICKS_HOST']
        self._token     = os.environ['DATABRICKS_TOKEN']
        self._http_path = os.environ['DATABRICKS_HTTP_PATH']
        self.catalog    = catalog or os.environ.get('DATABRICKS_CATALOG', 'main')
        self.schema     = schema  or os.environ.get('DATABRICKS_SCHEMA', 'polar')

        self._conn   = None
        self._cursor = None

        print(f"🔧 DatabricksLoader initialisiert")
        print(f"   Catalog : {self.catalog}")
        print(f"   Schema  : {self.schema}")
        print(f"   Host    : {self._host}")

    # --------------------------------------------------------
    # Verbindung
    # --------------------------------------------------------

    def verbinden(self) -> None:
        """
        Stellt die Verbindung zum Databricks SQL Warehouse her.
        Wird automatisch beim ersten Datenzugriff aufgerufen.

        Raises:
            ImportError:  Wenn databricks-sql-connector fehlt.
            RuntimeError: Bei Verbindungsfehler.
        """
        if self._cursor:
            return  # Bereits verbunden

        try:
            from databricks import sql as dbsql
        except ImportError:
            raise ImportError(
                "Paket 'databricks-sql-connector' nicht gefunden!\n"
                "→ pip install databricks-sql-connector"
            )

        try:
            self._conn = dbsql.connect(
                server_hostname=self._host.replace('https://', ''),
                http_path=self._http_path,
                access_token=self._token,
            )
            self._cursor = self._conn.cursor()
            print(f"✅ Databricks verbunden")
        except Exception as e:
            raise RuntimeError(
                f"Verbindung zu Databricks fehlgeschlagen:\n{e}\n\n"
                "Mögliche Ursachen:\n"
                "  • DATABRICKS_TOKEN abgelaufen → neues Token generieren\n"
                "    (Databricks UI → User Settings → Developer → Access Tokens)\n"
                "  • SQL Warehouse gestoppt → im Databricks UI starten\n"
                "  • Falscher DATABRICKS_HTTP_PATH\n"
            )

    def verbindung_testen(self) -> bool:
        """
        Testet die Datenbankverbindung mit einer einfachen Abfrage.

        Returns:
            True wenn Verbindung funktioniert, False bei Fehler.

        Beispiel:
            >>> db = DatabricksLoader()
            >>> db.verbindung_testen()
            True
        """
        try:
            self.verbinden()
            self._cursor.execute("SELECT 1 AS test")
            self._cursor.fetchone()
            print("✅ Datenbankverbindung funktioniert")
            return True
        except Exception as e:
            print(f"❌ Verbindungstest fehlgeschlagen: {e}")
            return False

    # --------------------------------------------------------
    # Generische Abfrage
    # --------------------------------------------------------

    def abfrage(self, sql: str) -> pd.DataFrame:
        """
        Führt eine beliebige SQL-Abfrage aus und gibt einen DataFrame zurück.

        Args:
            sql: SELECT-Statement (nur lesende Abfragen empfohlen)

        Returns:
            DataFrame mit Abfrageergebnis. Leerer DataFrame bei Fehler.

        Beispiel:
            >>> df = db.abfrage("SELECT datum, schritte FROM polar.activity LIMIT 10")
        """
        self.verbinden()
        try:
            self._cursor.execute(sql)
            spalten = [desc[0] for desc in self._cursor.description]
            zeilen  = self._cursor.fetchall()
            return pd.DataFrame(zeilen, columns=spalten)
        except Exception as e:
            print(f"❌ Abfragefehler: {e}")
            print(f"   SQL: {sql[:200]}...")
            return pd.DataFrame()

    def _tabelle(self, name: str) -> str:
        """Gibt den vollqualifizierten Tabellennamen zurück."""
        return f"{self.catalog}.{self.schema}.{name}"

    # --------------------------------------------------------
    # Tabellen laden
    # --------------------------------------------------------

    def lade_activity(
        self,
        von: Optional[date] = None,
        bis: Optional[date] = None,
    ) -> pd.DataFrame:
        """
        Lädt die täglichen Aktivitätsdaten aus polar.activity.

        Args:
            von: Startdatum (Optional, z.B. date(2020, 1, 1))
            bis: Enddatum   (Optional, z.B. date(2024, 12, 31))

        Returns:
            DataFrame mit Spalten:
                datum (date), schritte (int), kalorien (float),
                schlaf_stunden (float), schlaf_qualitaet (float),
                met_minuten (float)

        Beispiel:
            >>> from datetime import date
            >>> df = db.lade_activity(von=date(2023, 1, 1))
            >>> df['schritte'].mean()
        """
        where = _datum_filter(von, bis)
        sql = f"""
            SELECT
                datum,
                schritte,
                kalorien,
                schlaf_stunden,
                schlaf_qualitaet,
                met_minuten
            FROM {self._tabelle('activity')}
            {where}
            ORDER BY datum
        """
        df = self.abfrage(sql)
        if not df.empty:
            df['datum'] = pd.to_datetime(df['datum']).dt.date
            df = df.sort_values('datum').reset_index(drop=True)
        print(f"✅ Activity geladen: {len(df)} Zeilen")
        return df

    def lade_training(
        self,
        von: Optional[date] = None,
        bis: Optional[date] = None,
        sport: Optional[str] = None,
    ) -> pd.DataFrame:
        """
        Lädt die Trainingseinheiten aus polar.training.

        Args:
            von:   Startdatum (Optional)
            bis:   Enddatum   (Optional)
            sport: Sportart filtern (Optional, z.B. 'RUNNING')

        Returns:
            DataFrame mit Spalten:
                datum (date), sport (str), dauer_min (float),
                hr_avg (float), hr_max (float),
                distanz_km (float), kalorien (float),
                wochentag (str), jahr (int)

        Beispiel:
            >>> df_laufen = db.lade_training(sport='RUNNING')
            >>> df_laufen['distanz_km'].sum()
        """
        where = _datum_filter(von, bis)
        sport_filter = ""
        if sport:
            sport_filter = (
                f"{'AND' if where else 'WHERE'} "
                f"UPPER(sport) = '{sport.upper()}'"
            )

        sql = f"""
            SELECT
                datum, sport, dauer_min,
                hr_avg, hr_max, distanz_km,
                kalorien, wochentag, jahr
            FROM {self._tabelle('training')}
            {where} {sport_filter}
            ORDER BY datum
        """
        df = self.abfrage(sql)
        if not df.empty:
            df['datum'] = pd.to_datetime(df['datum']).dt.date
            df = df.sort_values('datum').reset_index(drop=True)
        print(f"✅ Training geladen: {len(df)} Einheiten")
        return df

    def lade_heartrate(
        self,
        von: Optional[date] = None,
        bis: Optional[date] = None,
    ) -> pd.DataFrame:
        """
        Lädt die täglichen Herzfrequenzdaten aus polar.heartrate.

        Args:
            von: Startdatum (Optional)
            bis: Enddatum   (Optional)

        Returns:
            DataFrame mit Spalten:
                datum (date), hr_ruhepuls (float), hr_mean (float),
                hr_max (int), wochentag_nr (int), monat (int)

        Beispiel:
            >>> df = db.lade_heartrate()
            >>> df['hr_ruhepuls'].describe()
        """
        where = _datum_filter(von, bis)
        sql = f"""
            SELECT
                datum, hr_ruhepuls, hr_mean,
                hr_max, wochentag_nr, monat
            FROM {self._tabelle('heartrate')}
            {where}
            ORDER BY datum
        """
        df = self.abfrage(sql)
        if not df.empty:
            df['datum'] = pd.to_datetime(df['datum']).dt.date
            df = df.sort_values('datum').reset_index(drop=True)
        print(f"✅ Herzfrequenz geladen: {len(df)} Tage")
        return df

    def lade_hrv(
        self,
        von: Optional[date] = None,
        bis: Optional[date] = None,
    ) -> pd.DataFrame:
        """
        Lädt die HRV-Daten aus polar.hrv.

        Args:
            von: Startdatum (Optional)
            bis: Enddatum   (Optional)

        Returns:
            DataFrame mit Spalten:
                datum (date), hrv_rmssd (float), hrv_sdnn (float),
                ppi_mean_ms (float), hr_aus_ppi (float),
                anzahl_samples (int)

        Beispiel:
            >>> df = db.lade_hrv()
            >>> df['hrv_rmssd'].resample('M').mean()
        """
        where = _datum_filter(von, bis)
        sql = f"""
            SELECT
                datum, hrv_rmssd, hrv_sdnn,
                ppi_mean_ms, hr_aus_ppi, anzahl_samples
            FROM {self._tabelle('hrv')}
            {where}
            ORDER BY datum
        """
        df = self.abfrage(sql)
        if not df.empty:
            df['datum'] = pd.to_datetime(df['datum']).dt.date
            df = df.sort_values('datum').reset_index(drop=True)
        print(f"✅ HRV geladen: {len(df)} Tage")
        return df

    # --------------------------------------------------------
    # Vorgefertigte Analyse-Abfragen
    # --------------------------------------------------------

    def monatsaggregat_activity(self) -> pd.DataFrame:
        """
        Berechnet Monatsdurchschnitte der Aktivitätsdaten.

        Nützlich für Trendanalysen und Dashboard-Charts.

        Returns:
            DataFrame mit Spalten:
                jahr (int), monat (int), schritte_avg (float),
                kalorien_avg (float), schlaf_avg (float),
                schlaf_q_avg (float)

        Beispiel:
            >>> df = db.monatsaggregat_activity()
            >>> df[df['jahr'] == 2024]['schritte_avg'].plot()
        """
        sql = f"""
            SELECT
                YEAR(datum)                     AS jahr,
                MONTH(datum)                    AS monat,
                ROUND(AVG(schritte), 0)         AS schritte_avg,
                ROUND(AVG(kalorien), 1)         AS kalorien_avg,
                ROUND(AVG(schlaf_stunden), 2)   AS schlaf_avg,
                ROUND(AVG(schlaf_qualitaet), 3) AS schlaf_q_avg,
                COUNT(*)                        AS anzahl_tage
            FROM {self._tabelle('activity')}
            WHERE schritte IS NOT NULL
            GROUP BY YEAR(datum), MONTH(datum)
            ORDER BY jahr, monat
        """
        df = self.abfrage(sql)
        print(f"✅ Monatsaggregat Activity: {len(df)} Monate")
        return df

    def ruhepuls_trend(self, glaettung_tage: int = 30) -> pd.DataFrame:
        """
        Berechnet den geglätteten Ruhepuls-Trend (rollender Durchschnitt).

        Args:
            glaettung_tage: Fenstertage für den rollenden Durchschnitt
                            (Standard: 30 Tage)

        Returns:
            DataFrame mit Spalten:
                datum (date), hr_ruhepuls (float),
                hr_ruhepuls_trend (float)

        Beispiel:
            >>> df = db.ruhepuls_trend(glaettung_tage=180)
            >>> df.plot(x='datum', y='hr_ruhepuls_trend')
        """
        df = self.lade_heartrate()
        if df.empty:
            return df
        df['hr_ruhepuls_trend'] = (
            df['hr_ruhepuls']
            .rolling(window=glaettung_tage, min_periods=7, center=True)
            .mean()
            .round(2)
        )
        return df[['datum', 'hr_ruhepuls', 'hr_ruhepuls_trend']]

    def trainings_pro_jahr(self) -> pd.DataFrame:
        """
        Zählt Trainingseinheiten und Stunden pro Jahr und Sportart.

        Returns:
            DataFrame mit Spalten:
                jahr (int), sport (str),
                anzahl (int), stunden_gesamt (float)

        Beispiel:
            >>> df = db.trainings_pro_jahr()
            >>> df[df['sport'] == 'RUNNING']
        """
        sql = f"""
            SELECT
                jahr,
                sport,
                COUNT(*)                        AS anzahl,
                ROUND(SUM(dauer_min) / 60, 1)   AS stunden_gesamt
            FROM {self._tabelle('training')}
            WHERE dauer_min > 0
            GROUP BY jahr, sport
            ORDER BY jahr, anzahl DESC
        """
        df = self.abfrage(sql)
        print(f"✅ Trainings pro Jahr: {len(df)} Zeilen")
        return df

    def ruhepuls_heatmap(self) -> pd.DataFrame:
        """
        Berechnet den durchschnittlichen Ruhepuls nach Monat und Wochentag.
        Dient als Grundlage für die Heatmap im Dashboard.

        Returns:
            DataFrame mit Spalten:
                monat (int 1-12), wochentag_nr (int 0-6),
                hr_avg (float), anzahl (int)

        Beispiel:
            >>> df = db.ruhepuls_heatmap()
            >>> df.pivot(index='monat', columns='wochentag_nr', values='hr_avg')
        """
        sql = f"""
            SELECT
                monat,
                wochentag_nr,
                ROUND(AVG(hr_ruhepuls), 1) AS hr_avg,
                COUNT(*)                   AS anzahl
            FROM {self._tabelle('heartrate')}
            WHERE hr_ruhepuls IS NOT NULL
            GROUP BY monat, wochentag_nr
            ORDER BY monat, wochentag_nr
        """
        df = self.abfrage(sql)
        print(f"✅ Ruhepuls-Heatmap: {len(df)} Kombinationen")
        return df

    def import_log_uebersicht(self) -> pd.DataFrame:
        """
        Zeigt eine Übersicht der importierten Dateien nach Kategorie.

        Returns:
            DataFrame mit Spalten:
                kategorie (str), anzahl (int),
                letzter_import (datetime)

        Beispiel:
            >>> db.import_log_uebersicht()
        """
        sql = f"""
            SELECT
                kategorie,
                COUNT(*)            AS anzahl,
                MAX(importiert_am)  AS letzter_import
            FROM {self._tabelle('import_log')}
            GROUP BY kategorie
            ORDER BY anzahl DESC
        """
        df = self.abfrage(sql)
        print(f"✅ Import-Log: {len(df)} Kategorien")
        return df

    def tabellen_uebersicht(self) -> pd.DataFrame:
        """
        Zeigt alle Tabellen im Schema mit Zeilenanzahl und letztem Datum.

        Returns:
            DataFrame mit Spalten:
                tabelle (str), zeilen (int), min_datum, max_datum

        Beispiel:
            >>> db.tabellen_uebersicht()
        """
        tabellen = ['activity', 'training', 'heartrate', 'hrv']
        zeilen_liste = []

        for tabelle in tabellen:
            try:
                df = self.abfrage(
                    f"SELECT COUNT(*) AS n, "
                    f"MIN(datum) AS min_d, MAX(datum) AS max_d "
                    f"FROM {self._tabelle(tabelle)}"
                )
                if not df.empty:
                    zeilen_liste.append({
                        'tabelle'   : tabelle,
                        'zeilen'    : int(df['n'].iloc[0]),
                        'min_datum' : df['min_d'].iloc[0],
                        'max_datum' : df['max_d'].iloc[0],
                    })
            except Exception as e:
                zeilen_liste.append({
                    'tabelle': tabelle, 'zeilen': -1,
                    'min_datum': None, 'max_datum': str(e)[:50],
                })

        df_result = pd.DataFrame(zeilen_liste)
        print("\n📊 Tabellen-Übersicht:")
        print(df_result.to_string(index=False))
        return df_result

    # --------------------------------------------------------
    # Verbindung schliessen
    # --------------------------------------------------------

    def schliessen(self) -> None:
        """Schliesst die Datenbankverbindung explizit."""
        try:
            if self._cursor:
                self._cursor.close()
                self._cursor = None
            if self._conn:
                self._conn.close()
                self._conn = None
            print("✅ Datenbankverbindung geschlossen")
        except Exception as e:
            print(f"⚠️  Fehler beim Schliessen: {e}")

    def __del__(self):
        """Verbindung beim Aufräumen schliessen."""
        try:
            self.schliessen()
        except Exception:
            pass

    def __enter__(self):
        """Kontext-Manager: with DatabricksLoader() as db:"""
        self.verbinden()
        return self

    def __exit__(self, *args):
        """Kontext-Manager: Verbindung automatisch schliessen."""
        self.schliessen()


# ============================================================
# Hilfsfunktion
# ============================================================

def _datum_filter(
    von: Optional[date],
    bis: Optional[date],
) -> str:
    """
    Erstellt eine SQL WHERE-Klausel für Datumsfilter.

    Args:
        von: Startdatum (Optional)
        bis: Enddatum   (Optional)

    Returns:
        SQL WHERE-Klausel (leer wenn kein Filter gesetzt).

    Beispiel:
        >>> _datum_filter(date(2023, 1, 1), None)
        "WHERE datum >= '2023-01-01'"
    """
    bedingungen = []
    if von:
        bedingungen.append(f"datum >= '{von}'")
    if bis:
        bedingungen.append(f"datum <= '{bis}'")
    if bedingungen:
        return "WHERE " + " AND ".join(bedingungen)
    return ""
