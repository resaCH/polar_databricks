"""
polar_accesslink.py
===================
Alternativer Daten-Import via Polar Accesslink REST API.
Ersetzt (oder ergänzt) den ZIP-basierten Import aus delta_updater.py.

Unterstützte Datentypen:
    - Aktivität          → polar.activity  (Schritte, Kalorien)
    - Schlaf             → polar.activity  (Schlafdauer, -qualität via Sleep-Endpoint)
    - Training/Exercises → polar.training
    - Physical Info      → polar.physical_info
    - Nightly Recharge   → polar.sonstige_daten (inkl. HRV-avg)

Nicht verfügbar via API (nur via ZIP-Export):
    - Sekundenweise 24/7-HR-Daten (247ohr_*.json) → polar.heartrate
    - PPI-Samples für RMSSD/SDNN-Berechnung       → polar.hrv

OAuth2-Credentials (GitHub Codespaces Secrets):
    POLAR_CLIENT_ID       – App-Client-ID (Polar Accesslink Developer Portal)
    POLAR_CLIENT_SECRET   – App-Client-Secret

Benutzerspezifische Tokens (lokal in .polar_tokens.json gespeichert, gitignored):
    access_token          – OAuth2 Access Token
    refresh_token         – OAuth2 Refresh Token (lange gültig)
    user_id               – Polar User ID (nach Registrierung)

Einrichtung (einmalig, Notebook 05_accesslink_setup.ipynb):
    1. App auf https://admin.polaraccesslink.com registrieren
       Redirect URI: http://localhost:5678/callback
    2. POLAR_CLIENT_ID und POLAR_CLIENT_SECRET als Codespaces Secrets setzen
    3. client.oauth2_setup() aufrufen → Browser-URL folgen
    4. client.register_user() aufrufen
    5. Tokens werden automatisch in .polar_tokens.json gespeichert

Verwendung (täglich):
    from polar_accesslink import AccesslinkUpdater
    updater = AccesslinkUpdater()
    bericht = updater.import_alle()
"""

import base64
import json
import os
import re
import socket
import threading
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Optional
from urllib.parse import parse_qs, urlparse

import pandas as pd
import requests

# ──────────────────────────────────────────────────────────────────────────────
# Konstanten
# ──────────────────────────────────────────────────────────────────────────────

_AUTH_URL    = "https://flow.polar.com/oauth2/authorization"
_TOKEN_URL   = "https://polarremote.com/v2/oauth2/token"
_API_BASE    = "https://www.polaraccesslink.com"
_TOKEN_FILE  = Path(".polar_tokens.json")

# ──────────────────────────────────────────────────────────────────────────────
# MERGE SQL Templates (identisch zum Schema in delta_updater.py)
# ──────────────────────────────────────────────────────────────────────────────

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
    ziel.schritte       <> quelle.schritte OR
    ziel.kalorien       <> quelle.kalorien OR
    ziel.schlaf_stunden <> quelle.schlaf_stunden
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

_MERGE_PHYSICAL_INFO = """
MERGE INTO {catalog}.{schema}.physical_info AS ziel
USING (
    SELECT
        CAST(datum AS DATE)       AS datum,
        CAST(json_data AS STRING) AS json_data
    FROM {temp_view}
) AS quelle
ON ziel.datum = quelle.datum
WHEN MATCHED THEN UPDATE SET *
WHEN NOT MATCHED THEN INSERT *
"""

_MERGE_SONSTIGE = """
MERGE INTO {catalog}.{schema}.sonstige_daten AS ziel
USING (
    SELECT
        CAST(dateiname AS STRING) AS dateiname,
        CAST(kategorie AS STRING) AS kategorie,
        CAST(datum AS DATE)       AS datum,
        CAST(json_data AS STRING) AS json_data
    FROM {temp_view}
) AS quelle
ON ziel.dateiname = quelle.dateiname
WHEN MATCHED THEN UPDATE SET *
WHEN NOT MATCHED THEN INSERT *
"""


# ──────────────────────────────────────────────────────────────────────────────
# Sport-Mapping (Accesslink-Strings → interne Normalisierung)
# ──────────────────────────────────────────────────────────────────────────────

_SPORT_MAP = {
    "RUNNING":              "RUNNING",
    "TRAIL_RUNNING":        "RUNNING",
    "TREADMILL_RUNNING":    "RUNNING",
    "CYCLING":              "CYCLING",
    "INDOOR_CYCLING":       "CYCLING",
    "SPINNING":             "CYCLING",
    "SWIMMING":             "SWIMMING",
    "WALKING":              "WALKING",
    "HIKING":               "HIKING",
    "STRENGTH_TRAINING":    "STRENGTH",
    "FUNCTIONAL_TRAINING":  "STRENGTH",
    "FLOOR_GYMNASTICS":     "STRENGTH",
    "CORE":                 "STRENGTH",
    "YOGA":                 "YOGA",
    "PILATES":              "PILATES",
    "MOBILITY_DYNAMIC":     "MOBILITY_DYNAMIC",
    "MOBILITY_STATIC":      "MOBILITY_STATIC",
    "STRETCHING":           "MOBILITY_STATIC",
    "FITNESS_CLASS":        "FITNESS",
    "AEROBICS":             "FITNESS",
    "STEP_AEROBICS":        "FITNESS",
    "GROUP_EXERCISE":       "FITNESS",
    "CROSS_TRAINING":       "FITNESS",
    "ELLIPTICAL_TRAINING":  "FITNESS",
    "SKI_CLASSIC":          "SKI",
    "SKI_FREESTYLE":        "SKI",
    "CROSS_COUNTRY_SKIING": "SKI",
    "DOWNHILL_SKIING":      "SKI",
    "SNOWBOARDING":         "SKI",
    "ROWING":               "ROWING",
    "TRIATHLON":            "TRIATHLON",
    "BOXING":               "BOXING",
    "CLIMBING":             "CLIMBING",
    "DANCING":              "DANCING",
    "GOLF":                 "GOLF",
    "TENNIS":               "TENNIS",
    "BADMINTON":            "BADMINTON",
    "FOOTBALL":             "SOCCER",
    "BASKETBALL":           "BASKETBALL",
    "VOLLEYBALL":           "VOLLEYBALL",
    "ICE_SKATING":          "ICE_SKATING",
    "PADDLING":             "PADDLING",
    "MARTIAL_ARTS":         "MARTIAL_ARTS",
    "OTHER":                "OTHER",
}

_KATEGORIE_MAP = {
    "RUNNING":              "OUTDOOR",
    "CYCLING":              "OUTDOOR",
    "SWIMMING":             "OUTDOOR",
    "HIKING":               "OUTDOOR",
    "WALKING":              "OUTDOOR",
    "TRAIL_RUNNING":        "TRAIL",
    "TREADMILL_RUNNING":    "TREADMILL",
    "INDOOR_CYCLING":       "INDOOR",
    "SPINNING":             "INDOOR",
    "STRENGTH_TRAINING":    "INDOOR",
    "FUNCTIONAL_TRAINING":  "INDOOR",
    "FLOOR_GYMNASTICS":     "INDOOR",
    "YOGA":                 "INDOOR",
    "PILATES":              "INDOOR",
    "FITNESS_CLASS":        "INDOOR",
    "AEROBICS":             "INDOOR",
    "STEP_AEROBICS":        "INDOOR",
    "GROUP_EXERCISE":       "INDOOR",
    "CROSS_TRAINING":       "INDOOR",
    "ELLIPTICAL_TRAINING":  "INDOOR",
    "CORE":                 "INDOOR",
    "MOBILITY_DYNAMIC":     "INDOOR",
    "MOBILITY_STATIC":      "INDOOR",
    "STRETCHING":           "INDOOR",
}


# ──────────────────────────────────────────────────────────────────────────────
# Hilfsfunktionen
# ──────────────────────────────────────────────────────────────────────────────

def _parse_iso_duration(duration_str: str) -> float:
    """Konvertiert ISO-8601-Dauer (PT1H30M45S) in Minuten."""
    if not duration_str:
        return 0.0
    m = re.match(r'PT(?:(\d+)H)?(?:(\d+)M)?(?:(\d+(?:\.\d+)?)S)?', duration_str)
    if not m:
        return 0.0
    h = float(m.group(1) or 0)
    min_ = float(m.group(2) or 0)
    s = float(m.group(3) or 0)
    return h * 60 + min_ + s / 60


def _wert_zu_sql(val) -> str:
    """Konvertiert Python-Wert in SQL-Literal."""
    if val is None or (isinstance(val, float) and pd.isna(val)):
        return 'NULL'
    if isinstance(val, bool):
        return 'TRUE' if val else 'FALSE'
    if isinstance(val, (int, float)):
        return str(val)
    return f"'{str(val).replace(chr(39), chr(39)+chr(39))}'"


# ──────────────────────────────────────────────────────────────────────────────
# OAuth2-Callback-Server (für Einrichtung)
# ──────────────────────────────────────────────────────────────────────────────

def _starte_callback_server(port: int = 5678, timeout: int = 120) -> Optional[str]:
    """
    Startet einen einmaligen HTTP-Server auf localhost:{port},
    wartet auf den OAuth2-Callback und gibt den 'code'-Parameter zurück.

    Args:
        port:    Lokaler Port (muss mit Redirect URI übereinstimmen)
        timeout: Maximale Wartezeit in Sekunden

    Returns:
        Authorization Code oder None bei Timeout
    """
    code_holder = [None]

    class _CallbackHandler:
        def handle(self, conn: socket.socket) -> None:
            data = conn.recv(4096).decode("utf-8", errors="ignore")
            # Erste Zeile: GET /callback?code=xxx HTTP/1.1
            first_line = data.split("\r\n")[0] if data else ""
            path = first_line.split(" ")[1] if len(first_line.split(" ")) > 1 else ""
            params = parse_qs(urlparse(path).query)
            code_holder[0] = params.get("code", [None])[0]

            response = (
                "HTTP/1.1 200 OK\r\n"
                "Content-Type: text/html; charset=utf-8\r\n\r\n"
                "<html><body><h2>✅ Polar Autorisierung erfolgreich!</h2>"
                "<p>Du kannst dieses Fenster schliessen.</p></body></html>"
            )
            conn.sendall(response.encode())
            conn.close()

    event = threading.Event()

    def _serve() -> None:
        handler = _CallbackHandler()
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as srv:
            srv.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            srv.bind(("localhost", port))
            srv.listen(1)
            srv.settimeout(timeout)
            try:
                conn, _ = srv.accept()
                handler.handle(conn)
            except socket.timeout:
                pass
            finally:
                event.set()

    t = threading.Thread(target=_serve, daemon=True)
    t.start()
    event.wait(timeout=timeout + 2)
    return code_holder[0]


# ──────────────────────────────────────────────────────────────────────────────
# Hauptklasse: Polar Accesslink Client
# ──────────────────────────────────────────────────────────────────────────────

class PolarAccesslinkClient:
    """
    REST-Client für die Polar Accesslink API v3.

    Liest Client-ID und Secret aus Umgebungsvariablen (GitHub Codespaces Secrets).
    Tokens werden lokal in .polar_tokens.json gespeichert (gitignored).

    Beispiel (nach Einrichtung):
        client = PolarAccesslinkClient()
        df_aktivitaet = client.fetch_activity()
        df_training   = client.fetch_exercises()
    """

    def __init__(self):
        self.client_id     = os.environ.get("POLAR_CLIENT_ID")
        self.client_secret = os.environ.get("POLAR_CLIENT_SECRET")
        self.access_token  = os.environ.get("POLAR_ACCESS_TOKEN")
        self.refresh_token = os.environ.get("POLAR_REFRESH_TOKEN")
        self.user_id       = os.environ.get("POLAR_USER_ID")

        self._load_tokens()

        missing = [k for k, v in {
            "POLAR_CLIENT_ID": self.client_id,
            "POLAR_CLIENT_SECRET": self.client_secret,
        }.items() if not v]
        if missing:
            raise EnvironmentError(
                f"Fehlende Secrets: {', '.join(missing)}\n\n"
                "→ GitHub.com → Repo → Settings → Secrets and variables\n"
                "  → Codespaces → New repository secret"
            )

    # ── Token-Verwaltung ─────────────────────────────────────────────────────

    def _load_tokens(self) -> None:
        """Lädt gespeicherte Tokens aus .polar_tokens.json (überschreibt env vars)."""
        if _TOKEN_FILE.exists():
            try:
                data = json.loads(_TOKEN_FILE.read_text())
                self.access_token  = data.get("access_token",  self.access_token)
                self.refresh_token = data.get("refresh_token", self.refresh_token)
                self.user_id       = data.get("user_id",       self.user_id)
            except Exception:
                pass

    def _save_tokens(self) -> None:
        """Speichert Tokens in .polar_tokens.json."""
        _TOKEN_FILE.write_text(json.dumps({
            "access_token":  self.access_token,
            "refresh_token": self.refresh_token,
            "user_id":       self.user_id,
            "aktualisiert":  datetime.now().isoformat(),
        }, indent=2))

    def refresh_access_token(self) -> bool:
        """
        Erneuert den Access Token via Refresh Token.

        Returns:
            True bei Erfolg, False wenn kein Refresh Token vorhanden.
        """
        if not self.refresh_token:
            return False
        creds = base64.b64encode(
            f"{self.client_id}:{self.client_secret}".encode()
        ).decode()
        try:
            r = requests.post(
                _TOKEN_URL,
                headers={
                    "Authorization": f"Basic {creds}",
                    "Content-Type":  "application/x-www-form-urlencoded",
                    "Accept":        "application/json",
                },
                data={
                    "grant_type":    "refresh_token",
                    "refresh_token": self.refresh_token,
                },
                timeout=15,
            )
            r.raise_for_status()
            tokens = r.json()
            self.access_token  = tokens["access_token"]
            self.refresh_token = tokens.get("x_refresh_token", self.refresh_token)
            self._save_tokens()
            return True
        except Exception as e:
            print(f"⚠️  Token-Refresh fehlgeschlagen: {e}")
            return False

    # ── OAuth2-Einrichtung ───────────────────────────────────────────────────

    def get_authorization_url(self, redirect_uri: str = "http://localhost:5678/callback") -> str:
        """Gibt die Autorisierungs-URL zurück, die der Nutzer im Browser öffnen muss."""
        return (
            f"{_AUTH_URL}"
            f"?response_type=code"
            f"&client_id={self.client_id}"
            f"&redirect_uri={redirect_uri}"
        )

    def oauth2_setup(
        self,
        redirect_uri: str = "http://localhost:5678/callback",
        callback_port: int = 5678,
        timeout: int = 120,
    ) -> None:
        """
        Interaktiver OAuth2-Flow: öffnet Autorisierungs-URL und wartet
        automatisch auf den Callback (Codespaces leitet Port weiter).

        Args:
            redirect_uri:  Muss mit der im Polar Developer Portal eingetragenen URI übereinstimmen
            callback_port: Lokaler Port für den Callback-Server
            timeout:       Maximale Wartezeit in Sekunden

        Raises:
            RuntimeError: Wenn kein Code empfangen wurde.
        """
        url = self.get_authorization_url(redirect_uri)
        print("=" * 60)
        print("🔐 Polar OAuth2-Autorisierung")
        print("=" * 60)
        print("\n1. Öffne diese URL im Browser:")
        print(f"\n   {url}\n")
        print(f"2. Melde dich mit deinem Polar-Konto an")
        print(f"3. Erteile der App Zugriff")
        print(f"4. Warte – der Callback wird automatisch abgefangen...\n")

        code = _starte_callback_server(port=callback_port, timeout=timeout)

        if not code:
            print("\n⚠️ Kein Authorization Code empfangen (Timeout).")
            print("Tipp: Stelle sicher, dass Port 5678 im Codespace weitergeleitet wird.")
            print("      Bei lokalen Setups: http://localhost:5678/callback weiterleiten.")
            print("      Falls der Callback nicht ankommt, kannst du den Code manuell eingeben.")

            try:
                callback_url = input(
                    "Füge die vollständige Callback-URL (aus Browser-Adresszeile nach Login) ein oder drücke Enter zum Abbrechen:\n"
                ).strip()
            except Exception:
                callback_url = ""

            if callback_url:
                parsed = urlparse(callback_url)
                code = parse_qs(parsed.query).get("code", [None])[0]

        if not code:
            raise RuntimeError(
                "Kein Authorization Code empfangen (Timeout oder manuelle Eingabe fehlgeschlagen).\n"
                "Tipp: Klicke auf die gedruckte URL, melde dich an und kopiere den `code`-Parameter aus der Redirect-URL."
            )

        self._exchange_code(code, redirect_uri)
        print(f"\n✅ Tokens gespeichert in {_TOKEN_FILE}")
        print(f"   User ID: {self.user_id}")

    def _exchange_code(self, code: str, redirect_uri: str) -> None:
        """Tauscht Authorization Code gegen Access/Refresh Token."""
        creds = base64.b64encode(
            f"{self.client_id}:{self.client_secret}".encode()
        ).decode()
        r = requests.post(
            _TOKEN_URL,
            headers={
                "Authorization": f"Basic {creds}",
                "Content-Type":  "application/x-www-form-urlencoded",
                "Accept":        "application/json",
            },
            data={
                "grant_type":   "authorization_code",
                "code":         code,
                "redirect_uri": redirect_uri,
            },
            timeout=15,
        )
        r.raise_for_status()
        tokens = r.json()
        self.access_token  = tokens["access_token"]
        self.refresh_token = tokens.get("x_refresh_token")
        self.user_id       = str(tokens.get("x_user_id", ""))
        self._save_tokens()

    def register_user(self) -> dict:
        """
        Registriert den Nutzer bei Accesslink (einmalig erforderlich).
        Ignoriert Fehler wenn Nutzer bereits registriert ist.
        """
        r = self._request("POST", "/v3/users", json={"member-id": self.user_id or "user"})
        if r.status_code == 409:
            print("ℹ️  Benutzer bereits registriert.")
            return {"status": "already_registered"}
        r.raise_for_status()
        data = r.json()
        if "polar-user-id" in data:
            self.user_id = str(data["polar-user-id"])
            self._save_tokens()
        print(f"✅ Benutzer registriert, ID: {self.user_id}")
        return data

    # ── HTTP-Hilfsmethoden ───────────────────────────────────────────────────

    def _request(
        self,
        method: str,
        path: str,
        _retry: bool = True,
        **kwargs,
    ) -> requests.Response:
        """Authentifizierter API-Request mit automatischem Token-Refresh bei 401."""
        headers = kwargs.pop("headers", {})
        headers["Authorization"] = f"Bearer {self.access_token}"
        headers.setdefault("Accept", "application/json")

        r = requests.request(
            method,
            f"{_API_BASE}{path}",
            headers=headers,
            timeout=30,
            **kwargs,
        )
        if r.status_code == 401 and _retry:
            if self.refresh_access_token():
                return self._request(method, path, _retry=False, **kwargs)
        return r

    def _get(self, path_or_url: str, **kwargs) -> dict:
        """GET-Request, gibt geparsten JSON-Body zurück."""
        # Vollständige URL oder relativer Pfad
        if path_or_url.startswith("http"):
            path = path_or_url.replace(_API_BASE, "")
        else:
            path = path_or_url
        r = self._request("GET", path, **kwargs)
        try:
            r.raise_for_status()
        except requests.HTTPError as e:
            if r.status_code == 404:
                # Manche Endpunkte liefern 404 wenn keine Daten verfügbar sind.
                return {}
            raise
        return r.json() if r.text.strip() else {}

    def _check_user(self) -> None:
        if not self.user_id:
            raise ValueError(
                "Keine user_id vorhanden.\n"
                "→ Zuerst oauth2_setup() und register_user() ausführen."
            )

    # ── Aktivitätsdaten ──────────────────────────────────────────────────────

    def fetch_activity(self) -> pd.DataFrame:
        """
        Lädt neue tägliche Aktivitätsdaten via Transaction API.
        Liefert nur Datensätze seit dem letzten Commit (inkrementell).

        Returns:
            DataFrame mit Spalten: datum, schritte, kalorien,
            schlaf_stunden (None), schlaf_qualitaet (None), met_minuten
        """
        self._check_user()

        r = self._request("POST", f"/v3/users/{self.user_id}/activity-transactions")
        if r.status_code == 204:
            print("   ℹ️  Aktivität: Keine neuen Daten seit letztem Import.")
            return pd.DataFrame()
        r.raise_for_status()

        txn_id = r.json()["transaction-id"]
        rows = []

        try:
            txn_data = self._get(
                f"/v3/users/{self.user_id}/activity-transactions/{txn_id}"
            )
            for url in txn_data.get("activity-log", []):
                day = self._get(url)
                rows.append({
                    "datum":           date.fromisoformat(day["date"]),
                    "schritte":        int(day.get("steps") or 0),
                    "kalorien":        float(day.get("calories") or 0),
                    "schlaf_stunden":  None,
                    "schlaf_qualitaet":None,
                    "met_minuten":     float(day.get("active-calories") or 0),
                })
        finally:
            self._request("PUT", f"/v3/users/{self.user_id}/activity-transactions/{txn_id}")

        if not rows:
            return pd.DataFrame()
        return (
            pd.DataFrame(rows)
            .drop_duplicates(subset=["datum"])
            .sort_values("datum")
            .reset_index(drop=True)
        )

    # ── Schlafdaten ──────────────────────────────────────────────────────────

    def fetch_sleep(
        self,
        von: Optional[date] = None,
        bis: Optional[date] = None,
    ) -> pd.DataFrame:
        """
        Lädt Schlafdaten für einen Zeitraum (max. 28 Tage pro Aufruf).
        Ergebnis kann mit fetch_activity() per datum zusammengeführt werden.

        Args:
            von: Startdatum (Standard: 28 Tage zurück)
            bis: Enddatum   (Standard: heute)

        Returns:
            DataFrame mit Spalten: datum, schlaf_stunden, schlaf_qualitaet
        """
        self._check_user()
        params = {}
        if von:
            params["from"] = von.isoformat()
        if bis:
            params["to"] = bis.isoformat()

        try:
            data = self._get(f"/v3/users/{self.user_id}/sleep", params=params)
        except requests.HTTPError as e:
            if e.response is not None and e.response.status_code == 404:
                print("   ℹ️  Schlafdaten nicht verfügbar (404)")
                return pd.DataFrame()
            raise

        nights = data.get("nights", [])

        rows = []
        for night in nights:
            datum_str = night.get("date")
            if not datum_str:
                continue
            total_min = night.get("total_sleep_minutes")
            rating    = night.get("continuous_rating")  # 1–5 Skala

            rows.append({
                "datum":            date.fromisoformat(datum_str),
                "schlaf_stunden":   round(total_min / 60, 2) if total_min else None,
                "schlaf_qualitaet": float(rating) if rating is not None else None,
            })

        if not rows:
            return pd.DataFrame()
        return (
            pd.DataFrame(rows)
            .drop_duplicates(subset=["datum"])
            .sort_values("datum")
            .reset_index(drop=True)
        )

    def fetch_activity_with_sleep(
        self,
        sleep_von: Optional[date] = None,
        sleep_bis: Optional[date] = None,
    ) -> pd.DataFrame:
        """
        Kombiniert Aktivitäts- und Schlafdaten in einem DataFrame.
        Aktivitätsdaten kommen aus der Transaction API (inkrementell),
        Schlafdaten aus dem Sleep-Endpoint (datumsbasiert).

        Returns:
            DataFrame mit vollständigen polar.activity-Spalten
        """
        df_akt  = self.fetch_activity()

        if df_akt.empty:
            return df_akt

        df_slp  = self.fetch_sleep(von=sleep_von, bis=sleep_bis)

        if not df_slp.empty:
            df_slp = df_slp[["datum", "schlaf_stunden", "schlaf_qualitaet"]]
            df_akt = df_akt.drop(columns=["schlaf_stunden", "schlaf_qualitaet"])
            df_akt = df_akt.merge(df_slp, on="datum", how="left")

        return df_akt

    # ── Trainingsdaten ───────────────────────────────────────────────────────

    def fetch_exercises(self) -> pd.DataFrame:
        """
        Lädt neue Trainingseinheiten via Transaction API.
        Liefert nur Datensätze seit dem letzten Commit (inkrementell).

        Returns:
            DataFrame mit Spalten: datum, sport, kategorie, dauer_min,
            hr_avg, hr_max, distanz_km, kalorien, wochentag, jahr
        """
        self._check_user()

        r = self._request("POST", f"/v3/users/{self.user_id}/exercise-transactions")
        if r.status_code == 204:
            print("   ℹ️  Training: Keine neuen Daten seit letztem Import.")
            return pd.DataFrame()
        r.raise_for_status()

        txn_id = r.json()["transaction-id"]
        rows = []

        try:
            txn_data = self._get(
                f"/v3/users/{self.user_id}/exercise-transactions/{txn_id}"
            )
            for url in txn_data.get("exercises", []):
                ex = self._get(url)

                start = ex.get("start-time", "")
                if not start:
                    continue
                d = date.fromisoformat(start[:10])

                sport_raw = ex.get("sport", "OTHER")
                # detailed-sport-info liefert präzisere Angabe (z.B. MOBILITY_DYNAMIC statt OTHER)
                detail    = ex.get("detailed-sport-info", "").upper()
                if sport_raw == "OTHER" and detail and detail in _SPORT_MAP:
                    sport_raw = detail
                sport     = _SPORT_MAP.get(sport_raw, sport_raw)
                kategorie = _KATEGORIE_MAP.get(sport_raw, "OUTDOOR")

                dauer_min = _parse_iso_duration(ex.get("duration", ""))
                hr_info   = ex.get("heart-rate") or {}
                distanz   = ex.get("distance")

                # Pace-basierte Kategorie-Korrektur (analog zu delta_updater.py)
                if sport in ("WALKING", "HIKING") and distanz and dauer_min > 0:
                    pace = dauer_min / (distanz / 1000)
                    if pace < 10:
                        sport     = "RUNNING"
                        kategorie = "OUTDOOR"

                rows.append({
                    "datum":      d,
                    "sport":      sport,
                    "kategorie":  kategorie,
                    "dauer_min":  round(dauer_min, 1),
                    "hr_avg":     float(hr_info["average"]) if hr_info.get("average") else None,
                    "hr_max":     float(hr_info["maximum"]) if hr_info.get("maximum") else None,
                    "distanz_km": round(float(distanz) / 1000, 3) if distanz else None,
                    "kalorien":   float(ex.get("calories") or 0),
                    "wochentag":  d.strftime("%A"),
                    "jahr":       d.year,
                })
        finally:
            self._request("PUT", f"/v3/users/{self.user_id}/exercise-transactions/{txn_id}")

        if not rows:
            return pd.DataFrame()
        return pd.DataFrame(rows).reset_index(drop=True)

    # ── Physical Information ─────────────────────────────────────────────────

    def fetch_physical_info(self) -> pd.DataFrame:
        """
        Lädt neue körperliche Informationen (Gewicht, Größe etc.) via Transaction API.

        Returns:
            DataFrame mit Spalten: datum, json_data
        """
        self._check_user()

        r = self._request("POST", f"/v3/users/{self.user_id}/physical-information-transactions")
        if r.status_code == 204:
            print("   ℹ️  Physical Info: Keine neuen Daten.")
            return pd.DataFrame()
        r.raise_for_status()

        txn_id = r.json()["transaction-id"]
        rows = []

        try:
            txn_data = self._get(
                f"/v3/users/{self.user_id}/physical-information-transactions/{txn_id}"
            )
            for url in txn_data.get("physical-informations", []):
                info = self._get(url)
                created = info.get("created", "")
                d = date.fromisoformat(created[:10]) if created else date.today()
                rows.append({"datum": d, "json_data": json.dumps(info)})
        finally:
            self._request(
                "PUT",
                f"/v3/users/{self.user_id}/physical-information-transactions/{txn_id}",
            )

        if not rows:
            return pd.DataFrame()
        return (
            pd.DataFrame(rows)
            .drop_duplicates(subset=["datum"])
            .reset_index(drop=True)
        )

    # ── Nightly Recharge ─────────────────────────────────────────────────────

    def fetch_nightly_recharge(
        self,
        von: Optional[date] = None,
        bis: Optional[date] = None,
    ) -> pd.DataFrame:
        """
        Lädt Nightly-Recharge-Daten (Erholungswert inkl. nächtlichem HRV-avg).
        Wird in polar.sonstige_daten unter Kategorie 'nightly_recharge' gespeichert.

        Args:
            von: Startdatum (Standard: 28 Tage zurück)
            bis: Enddatum   (Standard: heute)

        Returns:
            DataFrame mit Spalten: dateiname, kategorie, datum, json_data
        """
        self._check_user()
        params = {}
        if von:
            params["from"] = von.isoformat()
        if bis:
            params["to"] = bis.isoformat()

        try:
            data = self._get(f"/v3/users/{self.user_id}/nightly-recharge", params=params)
        except requests.HTTPError as e:
            if e.response is not None and e.response.status_code == 404:
                print("   ℹ️  Nightly Recharge nicht verfügbar (404)")
                return pd.DataFrame()
            raise

        recharges = data.get("recharges", [])

        rows = []
        for r in recharges:
            datum_str = r.get("date")
            if not datum_str:
                continue
            d = date.fromisoformat(datum_str)
            rows.append({
                "dateiname": f"nightly_recharge_{datum_str}",
                "kategorie": "nightly_recharge",
                "datum":     d,
                "json_data": json.dumps(r),
            })

        if not rows:
            return pd.DataFrame()
        return (
            pd.DataFrame(rows)
            .drop_duplicates(subset=["dateiname"])
            .reset_index(drop=True)
        )

    # ── Benutzerinfo ─────────────────────────────────────────────────────────

    def get_user_info(self) -> dict:
        """Gibt das Polar-Nutzerprofil zurück."""
        self._check_user()
        return self._get(f"/v3/users/{self.user_id}")


# ──────────────────────────────────────────────────────────────────────────────
# AccesslinkUpdater: API-Daten → Databricks Delta Tables
# ──────────────────────────────────────────────────────────────────────────────

class AccesslinkUpdater:
    """
    Kombiniert PolarAccesslinkClient mit Databricks MERGE INTO.
    Schreibt API-Daten direkt in die bestehenden Delta Tables.

    Umgebungsvariablen (GitHub Codespaces Secrets):
        POLAR_CLIENT_ID, POLAR_CLIENT_SECRET  – Polar API
        DATABRICKS_HOST, DATABRICKS_TOKEN,
        DATABRICKS_HTTP_PATH,
        DATABRICKS_CATALOG, DATABRICKS_SCHEMA  – Databricks

    Beispiel:
        updater = AccesslinkUpdater()
        bericht = updater.import_alle()
        print(bericht)
    """

    def __init__(
        self,
        catalog: str = None,
        schema:  str = None,
    ):
        self.client  = PolarAccesslinkClient()
        self.catalog = catalog or os.environ.get("DATABRICKS_CATALOG", "main")
        self.schema  = schema  or os.environ.get("DATABRICKS_SCHEMA",  "polar")

        # Databricks-Verbindung (lazy)
        self._conn   = None
        self._cursor = None

        # Databricks-Secrets prüfen
        missing = [
            k for k in ["DATABRICKS_HOST", "DATABRICKS_TOKEN", "DATABRICKS_HTTP_PATH"]
            if not os.environ.get(k)
        ]
        if missing:
            raise EnvironmentError(
                f"Fehlende Databricks Secrets: {', '.join(missing)}\n"
                "→ GitHub Codespaces Secrets setzen und Codespace neu starten."
            )

    # ── Datenbankverbindung ──────────────────────────────────────────────────

    def _verbinden(self) -> None:
        try:
            from databricks import sql as dbsql
        except ImportError:
            raise ImportError("pip install databricks-sql-connector")

        self._conn = dbsql.connect(
            server_hostname=os.environ["DATABRICKS_HOST"].replace("https://", ""),
            http_path=os.environ["DATABRICKS_HTTP_PATH"],
            access_token=os.environ["DATABRICKS_TOKEN"],
        )
        self._cursor = self._conn.cursor()
        print(f"✅ Databricks verbunden")

    def _sql(self, query: str) -> list:
        if not self._cursor:
            self._verbinden()
        self._cursor.execute(query)
        try:
            return self._cursor.fetchall()
        except Exception:
            return []

    def schliessen(self) -> None:
        """Schließt die Datenbankverbindung."""
        if self._cursor:
            self._cursor.close()
        if self._conn:
            self._conn.close()
        self._cursor = self._conn = None

    def __enter__(self):
        return self

    def __exit__(self, *_):
        self.schliessen()

    # ── MERGE Helper ─────────────────────────────────────────────────────────

    def _merge(self, df: pd.DataFrame, tabelle: str, merge_sql: str, view: str) -> int:
        """Schreibt DataFrame per MERGE INTO in eine Delta Table."""
        if df is None or df.empty:
            print(f"   ⏭️  {tabelle}: Keine Daten")
            return 0

        zeilen_sql = []
        for _, row in df.iterrows():
            werte = ", ".join(_wert_zu_sql(v) for v in row)
            zeilen_sql.append(f"({werte})")

        spalten = ", ".join(df.columns)
        values  = ",\n    ".join(zeilen_sql)

        cte = (
            f"WITH {view} AS (\n"
            f"  SELECT * FROM (VALUES\n    {values}\n"
            f"  ) AS t({spalten})\n)\n"
        )
        sql = cte + merge_sql.format(
            catalog=self.catalog,
            schema=self.schema,
            temp_view=view,
        )

        try:
            self._sql(sql)
            print(f"   ✅ {tabelle}: {len(df)} Datensätze verarbeitet")
            return len(df)
        except Exception as e:
            print(f"   ❌ {tabelle} MERGE fehlgeschlagen: {e}")
            raise

    # ── Öffentliche Import-Methoden ──────────────────────────────────────────

    def import_aktivitaet(
        self,
        sleep_von: Optional[date] = None,
        sleep_bis: Optional[date] = None,
    ) -> int:
        """Importiert Aktivitäts- und Schlafdaten → polar.activity."""
        print("\n📊 Aktivität & Schlaf...")
        df = self.client.fetch_activity_with_sleep(
            sleep_von=sleep_von,
            sleep_bis=sleep_bis,
        )
        return self._merge(df, "activity", _MERGE_ACTIVITY, "v_activity")

    def import_training(self) -> int:
        """Importiert Trainingseinheiten → polar.training."""
        print("\n🏃 Training...")
        df = self.client.fetch_exercises()
        return self._merge(df, "training", _MERGE_TRAINING, "v_training")

    def import_physical_info(self) -> int:
        """Importiert körperliche Informationen → polar.physical_info."""
        print("\n⚖️  Physical Info...")
        df = self.client.fetch_physical_info()
        return self._merge(df, "physical_info", _MERGE_PHYSICAL_INFO, "v_physical_info")

    def import_nightly_recharge(
        self,
        von: Optional[date] = None,
        bis: Optional[date] = None,
    ) -> int:
        """Importiert Nightly Recharge → polar.sonstige_daten."""
        print("\n🌙 Nightly Recharge...")
        df = self.client.fetch_nightly_recharge(von=von, bis=bis)
        return self._merge(df, "sonstige_daten (nightly_recharge)", _MERGE_SONSTIGE, "v_recharge")

    def import_alle(
        self,
        sleep_tage: int = 28,
        recharge_tage: int = 28,
    ) -> dict:
        """
        Führt alle verfügbaren Importe durch.

        Args:
            sleep_tage:    Wie viele Tage zurück Schlafdaten abgerufen werden
            recharge_tage: Wie viele Tage zurück Recharge-Daten abgerufen werden

        Returns:
            Bericht-Dict mit Anzahl importierter Datensätze pro Kategorie.
        """
        start = datetime.now()
        print(f"\n{'='*55}")
        print(f"🌐 Polar Accesslink Import")
        print(f"   {start.strftime('%Y-%m-%d %H:%M:%S')}")
        print(f"{'='*55}")

        bis  = date.today()
        von  = bis - timedelta(days=sleep_tage)
        von_r = bis - timedelta(days=recharge_tage)

        bericht = {}
        fehler  = []

        for name, fn in [
            ("aktivitaet",       lambda: self.import_aktivitaet(sleep_von=von, sleep_bis=bis)),
            ("training",         lambda: self.import_training()),
            ("physical_info",    lambda: self.import_physical_info()),
            ("nightly_recharge", lambda: self.import_nightly_recharge(von=von_r, bis=bis)),
        ]:
            try:
                bericht[name] = fn()
            except Exception as e:
                print(f"   ❌ {name}: {e}")
                fehler.append(name)
                bericht[name] = 0

        dauer = (datetime.now() - start).total_seconds()
        bericht["dauer_sekunden"] = round(dauer, 1)
        bericht["fehler"]         = fehler

        print(f"\n{'='*55}")
        print(f"✅ Import abgeschlossen in {dauer:.1f}s")
        gesamt = sum(v for k, v in bericht.items() if k not in ("dauer_sekunden", "fehler"))
        print(f"   Gesamt: {gesamt} Datensätze")
        if fehler:
            print(f"   ⚠️  Fehler bei: {', '.join(fehler)}")
        print(f"{'='*55}\n")

        return bericht
