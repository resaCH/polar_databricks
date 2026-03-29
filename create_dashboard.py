#!/usr/bin/env python3
import sys
from pathlib import Path
sys.path.insert(0, '/workspaces/polar_databricks/src')

import pandas as pd
import numpy as np
from datetime import datetime
import plotly.graph_objects as go
from db_loader import DatabricksLoader, secrets_pruefen

secrets_pruefen()

# Colors
C = {
    'bg': '#0f1117', 'card': '#1a1d27', 'border': '#2d3142',
    'text': '#e8eaf0', 'subtext': '#8892a4', 'blau': '#4f8ef7',
    'blau_hell': '#7eb3ff', 'orange': '#ff8c42', 'gruen': '#43c59e',
    'rot': '#f25f5c', 'lila': '#9d7fe3', 'gelb': '#ffd166', 'grid': '#1e2236'
}

LAYOUT_BASE = dict(
    paper_bgcolor=C['bg'], plot_bgcolor=C['card'],
    font=dict(family='Inter, Arial, sans-serif', color=C['text'], size=12),
    title_font=dict(size=15, color=C['text']),
    margin=dict(l=50, r=30, t=50, b=40),
    xaxis=dict(gridcolor=C['grid'], linecolor=C['border'], tickcolor=C['border'], tickfont=dict(color=C['subtext'])),
    yaxis=dict(gridcolor=C['grid'], linecolor=C['border'], tickcolor=C['border'], tickfont=dict(color=C['subtext'])),
    legend=dict(bgcolor='rgba(0,0,0,0)', bordercolor=C['border'], font=dict(color=C['subtext'])),
    hoverlabel=dict(bgcolor=C['card'], font_color=C['text'], bordercolor=C['border'])
)

# Load data
db = DatabricksLoader()
df_act = db.lade_activity()
df_train = db.lade_training()
df_hr = db.lade_heartrate()
df_hrv = db.lade_hrv()
df_monat = db.monatsaggregat_activity()

for df in [df_act, df_train, df_hr, df_hrv]:
    if not df.empty:
        df['datum'] = pd.to_datetime(df['datum'])

print('Data loaded successfully')

# KPI calculation
kpi = {}
if not df_act.empty:
    kpi['jahre'] = round((df_act['datum'].max() - df_act['datum'].min()).days / 365.25, 1)
    schritte_mean = df_act['schritte'].dropna().mean()
    kpi['schritte_avg'] = int(round(schritte_mean, 0)) if pd.notna(schritte_mean) else 0
    schlaf_mean = df_act['schlaf_stunden'].dropna().mean()
    kpi['schlaf_avg'] = round(float(schlaf_mean), 1) if pd.notna(schlaf_mean) else 0
else:
    kpi['jahre'] = kpi['schritte_avg'] = kpi['schlaf_avg'] = 0

if not df_hr.empty and 'hr_ruhepuls' in df_hr.columns:
    ruhepuls_mean = df_hr['hr_ruhepuls'].dropna().mean()
    kpi['ruhepuls_avg'] = round(float(ruhepuls_mean), 1) if pd.notna(ruhepuls_mean) else 0
else:
    kpi['ruhepuls_avg'] = 0

if not df_train.empty:
    kpi['trainings_n'] = int(len(df_train))
    kpi['laeufe_n'] = int(len(df_train[df_train['sport'] == 'RUNNING']))
else:
    kpi['trainings_n'] = kpi['laeufe_n'] = 0

print('KPIs calculated')

# Create all charts (simplified versions for speed)
fig_schritte = go.Figure()
if not df_monat.empty:
    datum_liste = [f"{int(r.jahr):04d}-{int(r.monat):02d}-01" for r in df_monat.itertuples()]
    schritte_liste = [float(v) if not pd.isna(v) else None for v in df_monat['schritte_avg']]
    fig_schritte.add_trace(go.Bar(x=datum_liste, y=schritte_liste, name='Ø Schritte', marker_color=C['blau']))

fig_schritte.update_layout(**LAYOUT_BASE, title='📶 Schritte – Monatsdurchschnitt', height=380)

fig_hr = go.Figure()
if not df_hr.empty:
    df_hr_s = df_hr.sort_values('datum').copy()
    datum_hr = [str(d.date()) for d in df_hr_s['datum']]
    ruhepuls = [float(v) if not pd.isna(v) else None for v in df_hr_s['hr_ruhepuls']]
    fig_hr.add_trace(go.Scatter(x=datum_hr, y=ruhepuls, mode='markers', name='Tageswert', marker=dict(color=C['blau'], size=2.5, opacity=0.25)))

fig_hr.update_layout(**LAYOUT_BASE, title='❤️ Ruhepuls-Entwicklung', height=380)

fig_training = go.Figure()
if not df_train.empty:
    df_train['jahr'] = df_train['datum'].dt.year
    top5 = df_train['sport'].value_counts().head(5).index.tolist()
    farben_sports = [C['blau'], C['orange'], C['gruen'], C['lila'], C['gelb']]
    for sport, farbe in zip(top5, farben_sports):
        df_s = df_train[df_train['sport'] == sport].groupby('jahr').size()
        jahre = [int(j) for j in df_s.index.tolist()]
        anzahl = [int(n) for n in df_s.values.tolist()]
        fig_training.add_trace(go.Bar(x=jahre, y=anzahl, name=sport.title(), marker_color=farbe))

fig_training.update_layout(**LAYOUT_BASE, title='🏃 Trainingseinheiten pro Jahr', height=380)

fig_donut = go.Figure()
if not df_train.empty:
    sport_counts = df_train['sport'].value_counts()
    top7 = sport_counts.head(7)
    rest_n = int(sport_counts.iloc[7:].sum()) if len(sport_counts) > 7 else 0
    labels = [str(s).title() for s in top7.index.tolist()]
    values = [int(v) for v in top7.values.tolist()]
    if rest_n > 0:
        labels.append('Andere')
        values.append(rest_n)
    donut_farben = [C['blau'], C['orange'], C['gruen'], C['lila'], C['gelb'], C['rot'], C['blau_hell'], C['subtext']]
    fig_donut.add_trace(go.Pie(labels=labels, values=values, hole=0.55,
                               marker=dict(colors=donut_farben[:len(labels)], line=dict(color=C['bg'], width=2)),
                               textinfo='label+percent'))

fig_donut.update_layout(**LAYOUT_BASE, title='🥇 Sportarten-Verteilung', height=400)

fig_heatmap = go.Figure()
if not df_hr.empty and {'monat', 'wochentag_nr', 'hr_ruhepuls'}.issubset(df_hr.columns):
    source = df_hr.copy()
    value_col = 'hr_ruhepuls'
else:
    source = db.ruhepuls_heatmap()
    value_col = 'hr_avg'

if not source.empty and {'monat', 'wochentag_nr'}.issubset(source.columns):
    wochentage_de = ['Mo', 'Di', 'Mi', 'Do', 'Fr', 'Sa', 'So']
    monate_de = ['Jan','Feb','Mär','Apr','Mai','Jun','Jul','Aug','Sep','Okt','Nov','Dez']
    pivot = source.groupby(['monat', 'wochentag_nr'])[value_col].mean().unstack()
    pivot = pivot.reindex(index=range(1, 13), columns=range(0, 7))
    z_werte = [[round(float(v), 1) if not pd.isna(v) else None for v in row] for row in pivot.values.tolist()]
    fig_heatmap.add_trace(go.Heatmap(z=z_werte, x=wochentage_de, y=monate_de,
                                     colorscale=[[0.0, '#43c59e'], [0.5, '#ffd166'], [1.0, '#f25f5c']]))
else:
    fig_heatmap.add_annotation(text='Keine Daten für Ruhepuls-Heatmap verfügbar', x=0.5, y=0.5,
                               font=dict(color=C['subtext'], size=16), showarrow=False)

fig_heatmap.update_layout(**LAYOUT_BASE, title='🔥 Heatmap: Ruhepuls nach Monat & Wochentag', height=420)

fig_schlaf = go.Figure()
if not df_monat.empty:
    datum_liste = [f"{int(r.jahr):04d}-{int(r.monat):02d}-01" for r in df_monat.itertuples()]
    schlaf_liste = [float(v) if not pd.isna(v) else None for v in df_monat['schlaf_avg']]
    fig_schlaf.add_trace(go.Scatter(x=datum_liste, y=schlaf_liste, mode='lines', name='Ø Schlafdauer',
                                    line=dict(color=C['lila'], width=2), yaxis='y1'))
else:
    fig_schlaf.add_annotation(text='Keine Schlafdaten vorhanden', x=0.5, y=0.5, font=dict(color=C['subtext'], size=16), showarrow=False)

fig_schlaf.update_layout(**LAYOUT_BASE, title='😴 Schlafdauer & -qualität', height=380)

print('All charts created')

# Convert to HTML
def fig_zu_html(fig, include_js=False):
    return fig.to_html(full_html=False, include_plotlyjs='cdn' if include_js else False,
                       config={'displayModeBar': True, 'modeBarButtonsToRemove': ['sendDataToCloud', 'lasso2d', 'select2d'],
                               'displaylogo': False, 'responsive': True})

html_schritte = fig_zu_html(fig_schritte, include_js=True)
html_hr = fig_zu_html(fig_hr)
html_training = fig_zu_html(fig_training)
html_donut = fig_zu_html(fig_donut)
html_heatmap = fig_zu_html(fig_heatmap)
html_schlaf = fig_zu_html(fig_schlaf)

print('Charts converted to HTML')

# KPI HTML
def kpi_karte(titel, wert, einheit, farbe):
    return f"""
    <div style="background:{C['card']};border:1px solid {C['border']};border-top:3px solid {farbe};border-radius:8px;padding:20px 24px;text-align:center;min-width:130px;flex:1;">
        <div style="color:{C['subtext']};font-size:11px;letter-spacing:1px;text-transform:uppercase;margin-bottom:8px;">{titel}</div>
        <div style="color:{farbe};font-size:32px;font-weight:700;line-height:1.1;">{wert}</div>
        <div style="color:{C['subtext']};font-size:12px;margin-top:4px;">{einheit}</div>
    </div>
    """

kpi_html = f"""
<div style="display:flex;gap:16px;flex-wrap:wrap;margin-bottom:8px;">
    {kpi_karte('Datenspanne', str(kpi.get('jahre', '–')), 'Jahre', C['blau'])}
    {kpi_karte('Ø Schritte', f"{kpi.get('schritte_avg',0):,}", '/ Tag', C['gruen'])}
    {kpi_karte('Ø Ruhepuls', str(kpi.get('ruhepuls_avg', '–')), 'bpm', C['rot'])}
    {kpi_karte('Ø Schlaf', str(kpi.get('schlaf_avg', '–')), 'Stunden', C['lila'])}
    {kpi_karte('Trainings', f"{kpi.get('trainings_n',0):,}", 'gesamt', C['orange'])}
    {kpi_karte('Läufe', f"{kpi.get('laeufe_n',0):,}", 'gesamt', C['gelb'])}
</div>
"""

print('KPI cards created')

# Full dashboard HTML
erstellt_am = datetime.now().strftime('%d.%m.%Y %H:%M')

dashboard_html = f"""<!DOCTYPE html>
<html lang="de">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Polar Health Analytics – Dashboard</title>
    <style>
        *, *::before, *::after {{ box-sizing: border-box; }}
        body {{
            margin: 0;
            padding: 24px;
            background: {C['bg']};
            color: {C['text']};
            font-family: Inter, -apple-system, Arial, sans-serif;
            font-size: 14px;
            line-height: 1.5;
        }}
        .header {{
            margin-bottom: 28px;
            padding-bottom: 20px;
            border-bottom: 1px solid {C['border']};
        }}
        .header h1 {{
            margin: 0 0 6px 0;
            font-size: 26px;
            font-weight: 700;
            background: linear-gradient(135deg, {C['blau']}, {C['lila']});
            -webkit-background-clip: text;
            -webkit-text-fill-color: transparent;
            background-clip: text;
        }}
        .header .meta {{
            color: {C['subtext']};
            font-size: 13px;
        }}
        .section {{
            margin-bottom: 28px;
        }}
        .section-title {{
            font-size: 11px;
            font-weight: 600;
            letter-spacing: 1.5px;
            text-transform: uppercase;
            color: {C['subtext']};
            margin-bottom: 12px;
            padding-left: 4px;
            border-left: 3px solid {C['blau']};
            padding-left: 10px;
        }}
        .chart-card {{
            background: {C['card']};
            border: 1px solid {C['border']};
            border-radius: 10px;
            padding: 8px;
            margin-bottom: 20px;
        }}
        .grid-2 {{
            display: grid;
            grid-template-columns: 1fr 1fr;
            gap: 20px;
        }}
        .footer {{
            margin-top: 40px;
            padding-top: 16px;
            border-top: 1px solid {C['border']};
            color: {C['subtext']};
            font-size: 12px;
            text-align: center;
        }}
        @media (max-width: 900px) {{
            .grid-2 {{ grid-template-columns: 1fr; }}
            body {{ padding: 12px; }}
        }}
    </style>
</head>
<body>
    <div class="header">
        <h1>⌚ Polar Health Analytics</h1>
        <div class="meta">
            Persönliche Gesundheitsdaten 2014–2026 &nbsp;·&nbsp;
            Erstellt am {erstellt_am} &nbsp;·&nbsp;
            HSLU Data Science
        </div>
    </div>
    <div class="section">
        <div class="section-title">Kennzahlen</div>
        {kpi_html}
    </div>
    <div class="section">
        <div class="section-title">Aktivität</div>
        <div class="chart-card">{html_schritte}</div>
    </div>
    <div class="section">
        <div class="section-title">Herzfrequenz</div>
        <div class="chart-card">{html_hr}</div>
    </div>
    <div class="section">
        <div class="section-title">Training</div>
        <div class="grid-2">
            <div class="chart-card">{html_training}</div>
            <div class="chart-card">{html_donut}</div>
        </div>
    </div>
    <div class="section">
        <div class="section-title">Muster</div>
        <div class="chart-card">{html_heatmap}</div>
    </div>
    <div class="section">
        <div class="section-title">Schlaf</div>
        <div class="chart-card">{html_schlaf}</div>
    </div>
    <div class="footer">
        Polar Health Analytics &nbsp;·&nbsp;
        Daten: Polar Flow Export &nbsp;·&nbsp;
        Stack: Python · Databricks Delta Lake · Plotly &nbsp;·&nbsp;
        <a href="https://github.com" style="color:{C['blau']};text-decoration:none;">GitHub</a>
    </div>
</body>
</html>"""

# Save file
output_pfad = Path('/workspaces/polar_databricks/output/polar_dashboard.html')
output_pfad.parent.mkdir(parents=True, exist_ok=True)
with open(output_pfad, 'w', encoding='utf-8') as f:
    f.write(dashboard_html)

print('Full dashboard saved!')
print('File size:', output_pfad.stat().st_size, 'bytes')
db.schliessen()