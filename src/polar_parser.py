"""
polar_parser.py
===============
Parst alle bekannten Polar JSON-Formate aus dem Daten-Export-ZIP.

Unterstützte Dateitypen:
    - activity_*.json   → Tägliche Aktivität (Schritte, Kalorien, Schlaf)
    - training_*.json   → Trainingseinheiten (Sport, HR, Distanz)
    - 247ohr_*.json     → 24/7 Herzfrequenz-Samples pro Monat
    - ppi_*.json        → HRV / Peak-to-Peak Intervalle

Sicherheit:
    - ZIP wird NUR im Arbeitsspeicher verarbeitet (nie auf Disk entpackt)
    - Rohdaten verlassen den Speicher nach der Verarbeitung
    - Keine Rohdaten werden zurückgegeben, nur aggregierte DataFrames

Verwendung:
    from polar_parser import PolarParser
    parser = PolarParser("input/polar_export.zip")
    df_activity = parser.parse_activity()
    df_training = parser.parse_training()
    df_hr       = parser.parse_heartrate()
    df_hrv      = parser.parse_hrv()
"""

import io
import json
import re
import zipfile
from datetime import datetime, timedelta

import numpy as np
import pandas as pd


# ============================================================
# Hilfsfunktionen
# ============================================================

def _parse_iso_duration(duration_str: str) -> float:
    """
    Wandelt ISO-8601-Dauern (z.B. 'PT3600S', 'PT1H30M') in Minuten um.

    Args:
        duration_str: ISO-8601-Dauer-String (z.B. 'PT21420S', 'PT1H30M45S')

    Returns:
        Dauer in Minuten als float. 0.0 bei ungültigem Format.

    Beispiel:
        >>> _parse_iso_duration('PT3600S')
        60.0
        >>> _parse_iso_duration('PT1H30M')
        90.0
    """
    if not duration_str or not isinstance(duration_str, str):
        return 0.0

    total_sekunden = 0.0
    try:
        # Stunden
        h_match = re.search(r'(\d+(?:\.\d+)?)H', duration_str)
        if h_match:
            total_sekunden += float(h_match.group(1)) * 3600

        # Minuten
        m_match = re.search(r'(\d+(?:\.\d+)?)M', duration_str)
        if m_match:
            total_sekunden += float(m_match.group(1)) * 60

        # Sekunden
        s_match = re.search(r'(\d+(?:\.\d+)?)S', duration_str)
        if s_match:
            total_sekunden += float(s_match.group(1))

    except (ValueError, AttributeError):
        return 0.0

    return round(total_sekunden / 60, 4)


def _safe_float(value, default: float = None) -> float:
    """
    Konvertiert einen Wert sicher zu float.

    Args:
        value: Zu konvertierender Wert
        default: Rückgabewert bei Fehler (Standard: None)

    Returns:
        Float-Wert oder default bei Fehler.
    """
    try:
        return float(value)
    except (TypeError, ValueError):
        return default


def _safe_int(value, default: int = None) -> int:
    """
    Konvertiert einen Wert sicher zu int.

    Args:
        value: Zu konvertierender Wert
        default: Rückgabewert bei Fehler (Standard: None)

    Returns:
        Int-Wert oder default bei Fehler.
    """
    try:
        return int(value)
    except (TypeError, ValueError):
        return default


# ============================================================
# Hauptklasse
# ============================================================




# Polar Sport-ID → Sportartname
POLAR_SPORT_IDS = {
    # ── Laufen ──────────────────────────────────────────────
    '1'  : 'RUNNING',
    '27' : 'TRAIL_RUNNING',
    # ── Velo / Cycling ──────────────────────────────────────
    '2'  : 'CYCLING',
    '3'  : 'MOUNTAIN_BIKING',
    '38' : 'INDOOR_CYCLING',
    '113': 'E_BIKE',
    # ── Wandern / Walking ───────────────────────────────────
    '4'  : 'HIKING',
    '5'  : 'HIKING',
    '17' : 'WALKING',
    '94' : 'WALKING',
    # ── Wintersport ─────────────────────────────────────────
    '6'  : 'DOWNHILL_SKIING',
    '7'  : 'CROSS_COUNTRY_SKIING',
    '8'  : 'INDOOR_ROWING',       # war Snowboarding → Rudern
    '58' : 'BOOTCAMP',            # war Ski_Touring → Bootcamp
    # ── Schwimmen ───────────────────────────────────────────
    '11' : 'SWIMMING',
    '23' : 'SWIMMING',            # war Badminton → Schwimmen
    '83' : 'OPEN_WATER_SWIMMING',
    '103': 'SWIMMING',            # war Padel → Bahnschwimmen
    '105': 'OPEN_WATER_SWIMMING', # war SUP → Freiwasser
    # ── Kraft / Gym ─────────────────────────────────────────
    '15' : 'FITNESS_TRAINING',
    '117': 'INDOOR_ROWING',       # war Gym → Indoor-Rudern
    '24' : 'CROSS_COUNTRY_SKIING',# war Rowing Machine → Langlauf
    # ── Mobilität / Yoga ────────────────────────────────────
    '16' : 'OUTDOOR_OTHER',       # war Yoga → Sonstige Outdoor
    '111': 'MOBILITY_DYNAMIC',    # war Pilates → Mobilität dyn.
    '126': 'CORE',                # war Stretching → Core
    '127': 'MOBILITY_STATIC',     # war Meditation → Mobilität stat.
    # ── Teamsport ───────────────────────────────────────────
    '18' : 'INDOOR_CYCLING',      # war Football → Indoor Cycling
    '51' : 'UNIHOCKEY',           # war Frisbee → Unihockey
    '55' : 'VOLLEYBALL',
    '177': 'E_BIKE',              # war Beach Volleyball → E-Bike
    # ── Diverse ─────────────────────────────────────────────
    '36' : 'RUNNING',             # war Tennis → Leichtathletik → Running
    '61' : 'AEROBIC',             # war unbekannt → Aerobic
    '92' : 'TRAIL_RUNNING',       # war Mountaineering → Trail Running
    '36' : 'RUNNING',
}

# Nachkorrektur: Sportarten die anhand von Pace umkategorisiert werden
# HIKING/WALKING mit Pace < 10 min/km → RUNNING (Joggen auf falschem Modus aufgezeichnet)
SPORT_PACE_KORREKTUREN = {
    'HIKING' : ('RUNNING', 10.0),   # pace < 10 min/km → RUNNING
    'WALKING': ('RUNNING', 10.0),   # pace < 10 min/km → RUNNING
}

def _sport_lesen(sport_wert) -> str:
    """Liest den Sportnamen – unterstützt String und Dict-Format mit ID-Mapping."""
    if isinstance(sport_wert, dict):
        sid = str(sport_wert.get('id', sport_wert.get('ID', '')))
        return POLAR_SPORT_IDS.get(sid, f'ID_{sid}')
    val = str(sport_wert).strip().replace("'", "")
    return val if val else 'UNKNOWN'


class PolarParser:
    """
    Liest und parst einen Polar-Datenexport (ZIP-Datei) im Arbeitsspeicher.

    Das ZIP-File wird beim Initialisieren geöffnet aber nicht entpackt.
    Alle Parse-Methoden arbeiten direkt auf dem ZIP-Stream.

    Args:
        zip_pfad: Pfad zur ZIP-Datei (z.B. 'input/polar_export.zip')

    Raises:
        FileNotFoundError: Wenn die ZIP-Datei nicht gefunden wird.
        zipfile.BadZipFile: Wenn die Datei kein gültiges ZIP ist.

    Beispiel:
        >>> parser = PolarParser("input/polar_export.zip")
        >>> print(f"Dateien im ZIP: {parser.anzahl_dateien}")
        >>> df = parser.parse_activity()
    """

    def __init__(self, zip_pfad: str):
        self.zip_pfad = zip_pfad
        self._zip = zipfile.ZipFile(zip_pfad, 'r')
        self._alle_namen = self._zip.namelist()

        # Dateien nach Typ vorsortieren (nur Dateinamen, kein Inhalt)
        self._activity_dateien = [n for n in self._alle_namen
                                   if re.match(r'.*activity-.*.json$', n)]
        self._training_dateien = [n for n in self._alle_namen
                                   if re.match(r'.*training-.*.json$', n)]
        self._hr_dateien       = [n for n in self._alle_namen
                                   if re.match(r'.*247ohr_.*.json$', n)]
        self._ppi_dateien      = [n for n in self._alle_namen
                                   if re.match(r'.*ppi_.*.json$', n)]

        print(f"📦 ZIP geladen: {zip_pfad}")
        print(f"   Dateien gesamt : {len(self._alle_namen):>6}")
        print(f"   activity_*     : {len(self._activity_dateien):>6}")
        print(f"   training_*     : {len(self._training_dateien):>6}")
        print(f"   247ohr_*       : {len(self._hr_dateien):>6}")
        print(f"   ppi_*          : {len(self._ppi_dateien):>6}")

    @property
    def anzahl_dateien(self) -> int:
        """Gesamtzahl der Dateien im ZIP."""
        return len(self._alle_namen)

    def _lese_json(self, dateiname: str) -> dict | list | None:
        """
        Liest eine einzelne JSON-Datei aus dem ZIP in den Speicher.

        Args:
            dateiname: Name der Datei innerhalb des ZIP

        Returns:
            Geparstes JSON-Objekt oder None bei Fehler.
        """
        try:
            with self._zip.open(dateiname) as f:
                inhalt = f.read()
            return json.loads(inhalt)
        except (json.JSONDecodeError, KeyError, UnicodeDecodeError) as e:
            print(f"   ⚠️  Fehler beim Lesen von '{dateiname}': {e}")
            return None

    # --------------------------------------------------------
    # activity_*.json → polar.activity
    # --------------------------------------------------------

    def parse_activity(self, fortschritt: bool = True) -> pd.DataFrame:
        """
        Parst alle activity_*.json Dateien.

        Jede Datei enthält die Aktivitätsdaten eines Tages:
        Schritte, Kalorien, Schlafdauer, Schlafqualität, MET-Minuten.

        Args:
            fortschritt: Fortschrittsanzeige alle 500 Dateien (Standard: True)

        Returns:
            DataFrame mit Spalten:
                datum (date), schritte (int), kalorien (float),
                schlaf_stunden (float), schlaf_qualitaet (float),
                met_minuten (float)

        Beispiel:
            >>> df = parser.parse_activity()
            >>> df.head()
        """
        zeilen = []
        fehler = 0

        for i, dateiname in enumerate(self._activity_dateien):
            if fortschritt and i % 500 == 0:
                print(f"   Activity: {i}/{len(self._activity_dateien)}...")

            daten = self._lese_json(dateiname)
            if daten is None:
                fehler += 1
                continue

            try:
                datum_str = daten.get('date', '')
                if not datum_str:
                    continue

                summary = daten.get('summary', {})

                # Schlafdauer: ISO-8601 → Stunden
                schlaf_min = _parse_iso_duration(
                    summary.get('sleepDuration', '')
                )
                schlaf_h = round(schlaf_min / 60, 2) if schlaf_min else None

                zeilen.append({
                    'datum'           : pd.to_datetime(datum_str).date(),
                    'schritte'        : _safe_int(summary.get('stepCount')),
                    'kalorien'        : _safe_float(summary.get('calories')),
                    'schlaf_stunden'  : schlaf_h,
                    'schlaf_qualitaet': _safe_float(summary.get('sleepQuality')),
                    'met_minuten'     : _safe_float(summary.get('dailyMetMinutes')),
                })

            except Exception as e:
                fehler += 1
                print(f"   ⚠️  Parse-Fehler in '{dateiname}': {e}")

        df = pd.DataFrame(zeilen)
        if not df.empty:
            df = df.sort_values('datum').drop_duplicates('datum').reset_index(drop=True)

        print(f"✅ Activity: {len(df)} Tage geladen ({fehler} Fehler)")
        return df

    # --------------------------------------------------------
    # training_*.json → polar.training
    # --------------------------------------------------------

    def parse_training(self, fortschritt: bool = True) -> pd.DataFrame:
        """
        Parst alle training_*.json Dateien.

        Jede Datei enthält eine Trainingseinheit mit einer oder mehreren
        Exercises (Sportarten). Jede Exercise wird als eigene Zeile erfasst.

        Args:
            fortschritt: Fortschrittsanzeige alle 200 Dateien (Standard: True)

        Returns:
            DataFrame mit Spalten:
                datum (date), sport (str), dauer_min (float),
                hr_avg (float), hr_max (float),
                distanz_km (float), kalorien (float),
                wochentag (str), jahr (int)

        Beispiel:
            >>> df = parser.parse_training()
            >>> df[df['sport'] == 'RUNNING'].describe()
        """
        zeilen = []
        fehler = 0

        for i, dateiname in enumerate(self._training_dateien):
            if fortschritt and i % 200 == 0:
                print(f"   Training: {i}/{len(self._training_dateien)}...")

            daten = self._lese_json(dateiname)
            if daten is None:
                fehler += 1
                continue

            try:
                start_str = daten.get('startTime', '')
                if not start_str:
                    continue

                # startTime kann ISO-8601 mit Zeitzone sein
                try:
                    start_dt = pd.to_datetime(start_str, utc=True).tz_convert(None)
                except Exception:
                    start_dt = pd.to_datetime(start_str)

                datum   = start_dt.date()
                wochentag = start_dt.strftime('%A')  # Englisch: Monday..Sunday
                jahr    = start_dt.year

                # Sport auf Training-Ebene lesen
                sport_training = daten.get('sport', 'UNKNOWN')

                # Dauer/Distanz/HR: Training-Ebene als Fallback
                dauer_ms_top   = _safe_float(daten.get('durationMillis', 0)) or 0
                distanz_m_top  = _safe_float(daten.get('distanceMeters', 0)) or 0
                kalorien_top   = _safe_float(daten.get('calories'))
                hr_avg_top     = _safe_float(daten.get('hrAvg'))
                hr_max_top     = _safe_float(daten.get('hrMax'))

                exercises = daten.get('exercises', [])

                if exercises:
                    for ex in exercises:
                        hr_info   = ex.get('heartRate', {}) or {}
                        distanz_m = _safe_float(ex.get('distanceMeters',
                                        ex.get('distance', 0))) or distanz_m_top
                        dauer_ms  = _safe_float(ex.get('durationMillis',
                                        0)) or dauer_ms_top
                        sport_ex  = ex.get('sport', sport_training)

                        zeilen.append({
                            'datum'     : datum,
                            'sport'     : _sport_lesen(sport_ex),
                            'dauer_min' : round(dauer_ms / 60000, 2) if dauer_ms else
                                          _parse_iso_duration(ex.get('duration', '')),
                            'hr_avg'    : _safe_float(hr_info.get('average')) or hr_avg_top,
                            'hr_max'    : _safe_float(hr_info.get('maximum')) or hr_max_top,
                            'distanz_km': round(distanz_m / 1000, 3) if distanz_m else None,
                            'kalorien'  : _safe_float(ex.get('calories')) or kalorien_top,
                            'wochentag' : wochentag,
                            'jahr'      : jahr,
                        })
                else:
                    # Kein Exercise → direkt Training-Ebene verwenden
                    zeilen.append({
                        'datum'     : datum,
                        'sport'     : _sport_lesen(sport_training),
                        'dauer_min' : round(dauer_ms_top / 60000, 2) if dauer_ms_top else None,
                        'hr_avg'    : hr_avg_top,
                        'hr_max'    : hr_max_top,
                        'distanz_km': round(distanz_m_top / 1000, 3) if distanz_m_top else None,
                        'kalorien'  : kalorien_top,
                        'wochentag' : wochentag,
                        'jahr'      : jahr,
                    })

            except Exception as e:
                fehler += 1
                print(f"   ⚠️  Parse-Fehler in '{dateiname}': {e}")

        df = pd.DataFrame(zeilen)
        if not df.empty:
            df = df.sort_values('datum').reset_index(drop=True)

        print(f"✅ Training: {len(df)} Einheiten geladen ({fehler} Fehler)")
        return df

    # --------------------------------------------------------
    # 247ohr_*.json → polar.heartrate
    # --------------------------------------------------------

    def parse_heartrate(self, fortschritt: bool = True) -> pd.DataFrame:
        """
        Parst alle 247ohr_*.json Dateien (24/7 Herzfrequenz-Monitoring).

        Jede Datei enthält Herzfrequenz-Samples eines Monats.
        Pro Tag wird aggregiert: 5. Perzentil (≈ Ruhepuls), Mittelwert, Maximum.

        Args:
            fortschritt: Fortschrittsanzeige alle 50 Dateien (Standard: True)

        Returns:
            DataFrame mit Spalten:
                datum (date), hr_ruhepuls (float), hr_mean (float),
                hr_max (float), wochentag_nr (int 0=Mo), monat (int)

        Beispiel:
            >>> df = parser.parse_heartrate()
            >>> df['hr_ruhepuls'].describe()
        """
        tages_daten: dict[str, list] = {}  # datum_str → [hr_werte]
        fehler = 0

        for i, dateiname in enumerate(self._hr_dateien):
            if fortschritt and i % 50 == 0:
                print(f"   Herzfrequenz: {i}/{len(self._hr_dateien)}...")

            daten = self._lese_json(dateiname)
            if daten is None:
                fehler += 1
                continue

            try:
                device_days = daten.get('deviceDays', [])

                for tag in device_days:
                    datum_str = tag.get('date', '')
                    if not datum_str:
                        continue

                    samples = tag.get('samples', [])
                    hr_werte = []
                    for s in samples:
                        hr = _safe_int(s.get('heartRate'))
                        if hr and 20 <= hr <= 250:  # physiologisch sinnvoll
                            hr_werte.append(hr)

                    if hr_werte:
                        if datum_str not in tages_daten:
                            tages_daten[datum_str] = []
                        tages_daten[datum_str].extend(hr_werte)

            except Exception as e:
                fehler += 1
                print(f"   ⚠️  Parse-Fehler in '{dateiname}': {e}")

        # Aggregation pro Tag
        zeilen = []
        for datum_str, hr_werte in tages_daten.items():
            try:
                dt = pd.to_datetime(datum_str)
                arr = np.array(hr_werte)
                zeilen.append({
                    'datum'       : dt.date(),
                    'hr_ruhepuls' : round(float(np.percentile(arr, 5)), 1),
                    'hr_mean'     : round(float(np.mean(arr)), 1),
                    'hr_max'      : int(np.max(arr)),
                    'wochentag_nr': dt.weekday(),   # 0=Montag, 6=Sonntag
                    'monat'       : dt.month,
                })
            except Exception as e:
                print(f"   ⚠️  Aggregations-Fehler für '{datum_str}': {e}")

        df = pd.DataFrame(zeilen)
        if not df.empty:
            df = df.sort_values('datum').drop_duplicates('datum').reset_index(drop=True)

        print(f"✅ Herzfrequenz: {len(df)} Tage geladen ({fehler} Fehler)")
        return df

    # --------------------------------------------------------
    # ppi_*.json → polar.hrv
    # --------------------------------------------------------

    def parse_hrv(self, fortschritt: bool = True) -> pd.DataFrame:
        """
        Parst alle ppi_*.json Dateien (HRV / Peak-to-Peak Intervalle).

        Berechnet pro Tag: RMSSD, SDNN, mittleres PPI-Intervall,
        daraus abgeleiteten Ruhepuls und Anzahl Samples.

        HRV-Metriken:
            RMSSD = Root Mean Square of Successive Differences (Kurzzeit-HRV)
            SDNN  = Standard Deviation of NN intervals (Gesamt-HRV)

        Args:
            fortschritt: Fortschrittsanzeige alle 100 Dateien (Standard: True)

        Returns:
            DataFrame mit Spalten:
                datum (date), hrv_rmssd (float), hrv_sdnn (float),
                ppi_mean_ms (float), hr_aus_ppi (float), anzahl_samples (int)

        Beispiel:
            >>> df = parser.parse_hrv()
            >>> df[['datum', 'hrv_rmssd']].plot(x='datum', y='hrv_rmssd')
        """
        tages_ppi: dict[str, list] = {}  # datum_str → [ppi_ms_werte]
        fehler = 0

        for i, dateiname in enumerate(self._ppi_dateien):
            if fortschritt and i % 100 == 0:
                print(f"   HRV: {i}/{len(self._ppi_dateien)}...")

            daten = self._lese_json(dateiname)
            if daten is None:
                fehler += 1
                continue

            try:
                # ppi_*.json ist eine Liste von Objekten
                eintraege = daten if isinstance(daten, list) else [daten]

                for eintrag in eintraege:
                    datum_str = eintrag.get('date', '')
                    if not datum_str:
                        continue

                    ppi_liste = []
                    for geraet in eintrag.get('devicePpiSamplesList', []):
                        for probe in geraet.get('ppiSamples', []):
                            ppi_ms = _safe_int(probe.get('pulseLength'))
                            # physiologisch: 300ms (200 bpm) bis 2000ms (30 bpm)
                            if ppi_ms and 300 <= ppi_ms <= 2000:
                                ppi_liste.append(ppi_ms)

                    if ppi_liste:
                        if datum_str not in tages_ppi:
                            tages_ppi[datum_str] = []
                        tages_ppi[datum_str].extend(ppi_liste)

            except Exception as e:
                fehler += 1
                print(f"   ⚠️  Parse-Fehler in '{dateiname}': {e}")

        # HRV-Metriken pro Tag berechnen
        zeilen = []
        for datum_str, ppi_werte in tages_ppi.items():
            try:
                arr = np.array(ppi_werte, dtype=float)
                if len(arr) < 5:  # Mindestanzahl für sinnvolle HRV-Berechnung
                    continue

                # Successive Differences für RMSSD
                diffs = np.diff(arr)
                rmssd = float(np.sqrt(np.mean(diffs ** 2)))
                sdnn  = float(np.std(arr, ddof=1))
                mean  = float(np.mean(arr))

                # HR aus mittlerem PPI: 60'000 ms / PPI_ms
                hr_ppi = round(60000 / mean, 1) if mean > 0 else None

                zeilen.append({
                    'datum'         : pd.to_datetime(datum_str).date(),
                    'hrv_rmssd'     : round(rmssd, 2),
                    'hrv_sdnn'      : round(sdnn, 2),
                    'ppi_mean_ms'   : round(mean, 1),
                    'hr_aus_ppi'    : hr_ppi,
                    'anzahl_samples': len(arr),
                })

            except Exception as e:
                print(f"   ⚠️  HRV-Berechnungsfehler für '{datum_str}': {e}")

        df = pd.DataFrame(zeilen)
        if not df.empty:
            df = df.sort_values('datum').drop_duplicates('datum').reset_index(drop=True)

        print(f"✅ HRV: {len(df)} Tage geladen ({fehler} Fehler)")
        return df

    # --------------------------------------------------------
    # Hilfsmethode: Übersicht
    # --------------------------------------------------------

    def zusammenfassung(self) -> None:
        """
        Gibt eine Übersicht aller Dateien im ZIP nach Typ aus.
        Nützlich zur schnellen Diagnose des ZIP-Inhalts.
        """
        print("\n📊 ZIP-Inhalt Zusammenfassung")
        print("=" * 40)
        print(f"  activity_*.json  : {len(self._activity_dateien):>5} Dateien")
        print(f"  training_*.json  : {len(self._training_dateien):>5} Dateien")
        print(f"  247ohr_*.json    : {len(self._hr_dateien):>5} Dateien")
        print(f"  ppi_*.json       : {len(self._ppi_dateien):>5} Dateien")

        # Weitere Typen
        bekannte = set(
            self._activity_dateien +
            self._training_dateien +
            self._hr_dateien +
            self._ppi_dateien
        )
        andere = [n for n in self._alle_namen if n not in bekannte]
        print(f"  Sonstige         : {len(andere):>5} Dateien")
        print(f"  {'─' * 22}")
        print(f"  Gesamt           : {len(self._alle_namen):>5} Dateien")
        print()

    def __del__(self):
        """ZIP-Datei beim Aufräumen schliessen."""
        try:
            self._zip.close()
        except Exception:
            pass
