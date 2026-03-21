# ⌚ Polar Health Analytics

Persönliche Gesundheitsdaten einer Polar-Sportuhr analysieren und visualisieren –
von rohen JSON-Exporten bis zum interaktiven Dashboard.

> Begleitprojekt zum HSLU-Blogbeitrag *«Was 12 Jahre Pulsdaten über mich verraten»*

---

## Demo

![Dashboard Preview](output/dashboard_preview.png)

**Live-Dashboard:** `output/polar_dashboard.html` (nach lokalem Setup generieren)

---

## Projektübersicht

| Merkmal | Details |
|---|---|
| **Datenquelle** | Polar Flow Export (ZIP, ~8'000 JSON-Dateien) |
| **Zeitraum** | 2014–2026 (12 Jahre) |
| **Datenspeicher** | Databricks Free Edition · Delta Lake |
| **Umgebung** | GitHub Codespaces · Python 3.12 |
| **Visualisierung** | Plotly (interaktiv) · matplotlib · seaborn |

---

## Key Insights

| Metrik | Befund |
|---|---|
| **Ruhepuls-Trend** | Langzeitige Veränderung über 12 Jahre sichtbar |
| **Saisonalität** | Ruhepuls im Sommer tiefer als im Winter |
| **Hypothese 1** | Training → Ruhepuls: r ≈ −0.04 (kein linearer Zusammenhang) |
| **Hypothese 2** | Schlaf → Ruhepuls: r ≈ +0.02 (kein linearer Zusammenhang) |
| **HRV** | RMSSD im Sommer höher als im Winter |
| **Wochenmuster** | Ruhepuls-Heatmap zeigt klare Monat-×-Wochentag-Muster |

> **Fazit:** Ruhepuls und HRV werden von vielen Faktoren gleichzeitig beeinflusst
> (Stress, Hydration, Krankheit, Temperatur). Einfache Korrelationen mit einzelnen
> Variablen erklären nur einen Bruchteil der Varianz.

---

## Projektstruktur

```
polar-health-analytics/
├── .devcontainer/
│   └── devcontainer.json     ← Codespace-Konfiguration
├── input/                    ← Neue ZIP-Files hier ablegen
├── archive/                  ← Verarbeitete ZIP-Files
├── data/                     ← Lokale Temp-Daten
├── output/                   ← Exportierte Dashboards & Plots
├── notebooks/
│   ├── 00_setup.ipynb        ← Databricks Setup & Erstmigration
│   ├── 01_delta_update.ipynb ← Inkrementelles Update
│   ├── 02_exploration.ipynb  ← Data Discovery
│   ├── 03_analysis.ipynb     ← Korrelationsanalysen & Hypothesen
│   └── 04_dashboard.ipynb    ← Interaktives Plotly-Dashboard
├── src/
│   ├── polar_parser.py       ← Polar JSON-Formate parsen
│   ├── delta_updater.py      ← ZIP → Delta MERGE INTO
│   └── db_loader.py          ← Databricks-Verbindung & Laden
├── .env.example              ← Secret-Vorlage (kein echter Wert)
├── .gitignore                ← Schützt alle Gesundheitsdaten
├── requirements.txt
└── README.md
```

---

## Setup

### Voraussetzungen

- GitHub-Account mit Codespaces-Zugang
- Databricks Free Edition Account ([databricks.com](https://databricks.com))
- Polar-Datenexport (ZIP von [flow.polar.com](https://flow.polar.com))

### 1 – Repository forken / clonen

```bash
git clone https://github.com/DEIN-USERNAME/polar-health-analytics.git
```

### 2 – GitHub Codespaces Secrets einrichten

> ⚠️ **Kein `.env`-File** – Credentials ausschliesslich als verschlüsselte
> Codespaces Secrets speichern.

Repository → **Settings** → **Secrets and variables** → **Codespaces** →
**New repository secret**

| Secret | Beispielwert |
|---|---|
| `DATABRICKS_HOST` | `https://dbc-440507f2-1163.cloud.databricks.com` |
| `DATABRICKS_TOKEN` | `dapi_XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX` |
| `DATABRICKS_HTTP_PATH` | `/sql/1.0/warehouses/f9476e8729ec9452` |
| `DATABRICKS_CATALOG` | `main` |
| `DATABRICKS_SCHEMA` | `polar` |

**Token generieren:** Databricks UI → *User Settings* → *Developer* →
*Access Tokens* → *Generate new token*

### 3 – Codespace starten

```
Code → Codespaces → Create codespace on main
```

Der Container installiert automatisch alle Pakete aus `requirements.txt`
und aktiviert die `.venv`.

### 4 – Databricks Setup (einmalig)

`notebooks/00_setup.ipynb` öffnen und **Run All** ausführen:

- Schema `main.polar` anlegen
- 5 Delta Tables erstellen
- Ersten Polar-Export importieren

### 5 – Polar-Daten exportieren

1. [flow.polar.com](https://flow.polar.com) → Profil → Einstellungen
2. *Eigene Daten exportieren* → ZIP herunterladen
3. ZIP in den `input/` Ordner kopieren

### 6 – Regelmässige Updates

```
notebooks/01_delta_update.ipynb → Run All
```

Verarbeitet nur neue/geänderte Dateien (MD5-Hash-Vergleich).

---

## Databricks Delta Tables

| Tabelle | Inhalt | Schlüssel |
|---|---|---|
| `polar.activity` | Schritte, Kalorien, Schlaf | `datum` |
| `polar.training` | Sport, Dauer, HR, Distanz | `datum + sport + dauer` |
| `polar.heartrate` | Ruhepuls, HR-Mean, HR-Max | `datum` |
| `polar.hrv` | RMSSD, SDNN, PPI | `datum` |
| `polar.import_log` | MD5-Hashes aller importierten Dateien | `dateiname` |

---

## Notebooks

| Notebook | Zweck | Wann ausführen |
|---|---|---|
| `00_setup` | Schema & Tables anlegen, Erstimport | **Einmalig** |
| `01_delta_update` | Neue ZIP-Exporte verarbeiten | Nach jedem Export |
| `02_exploration` | Datenqualität, Verteilungen, erste Plots | Jederzeit |
| `03_analysis` | Korrelationen, Hypothesen, Heatmaps | Nach Updates |
| `04_dashboard` | Interaktives HTML-Dashboard generieren | Nach Analyse |

---

## Technischer Stack

```
Polar Uhr  →  flow.polar.com  →  ZIP-Export
                                      ↓
                              polar_parser.py
                            (ZIP nur im RAM, nie entpacken)
                                      ↓
                             delta_updater.py
                           (MD5-Check → MERGE INTO)
                                      ↓
                          Databricks Delta Lake
                         main.polar.{activity, training,
                                     heartrate, hrv}
                                      ↓
                               db_loader.py
                           (pandas DataFrames laden)
                                      ↓
                    Notebooks 02–04 (Analyse & Dashboard)
                                      ↓
                       output/polar_dashboard.html
```

---

## Sicherheit & Datenschutz

| Massnahme | Details |
|---|---|
| **Keine Rohdaten im Repo** | `input/`, `archive/`, `data/`, `*.json`, `*.zip` in `.gitignore` |
| **Kein Token im Code** | Ausschliesslich via GitHub Codespaces Secrets |
| **ZIP nur im RAM** | `zipfile.ZipFile` – nie auf Disk entpacken |
| **Nur Aggregate in Databricks** | Kein Upload von Rohdaten oder persönlichen Infos |
| **MD5-Check** | Verhindert doppelten Import und erkennt Änderungen |

> Gesundheitsdaten sind besonders schützenswert. Dieses Projekt
> wurde so konzipiert, dass Rohdaten niemals das Codespace-Dateisystem
> verlassen.

---

## Polar-Datenformate

Das ZIP enthält ~8'000 flache JSON-Dateien:

| Muster | Inhalt | Schlüsselfelder |
|---|---|---|
| `activity_*.json` | Tägliche Aktivität | `stepCount`, `calories`, `sleepDuration` (ISO-8601) |
| `training_*.json` | Trainingseinheiten | `sport`, `duration`, `heartRate.average` |
| `247ohr_*.json` | 24/7 Herzfrequenz | `deviceDays[].samples[].heartRate` |
| `ppi_*.json` | HRV Peak-to-Peak | `devicePpiSamplesList[].ppiSamples[].pulseLength` |

---

## Lokale Entwicklung (ohne Codespaces)

```bash
# Repository clonen
git clone https://github.com/DEIN-USERNAME/polar-health-analytics.git
cd polar-health-analytics

# Virtuelle Umgebung
python3.12 -m venv .venv
source .venv/bin/activate      # Windows: .venv\Scripts\activate

# Pakete installieren
pip install -r requirements.txt

# Secrets als Umgebungsvariablen setzen (nur lokal, nie committen!)
export DATABRICKS_HOST=https://dbc-...
export DATABRICKS_TOKEN=dapi_...
export DATABRICKS_HTTP_PATH=/sql/1.0/warehouses/...
export DATABRICKS_CATALOG=main
export DATABRICKS_SCHEMA=polar

# JupyterLab starten
jupyter lab
```

---

## Lizenz

MIT License – siehe [LICENSE](LICENSE)

Gesundheitsdaten sind **nicht** Teil dieses Repositories.

---

*Erstellt im Rahmen des HSLU Data Science Studiums · 2026*
