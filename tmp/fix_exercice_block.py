path = '/workspaces/polar_databricks/src/polar_parser.py'
content = open(path).read()

old = """                if exercises:
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
                        })"""

new = """                if exercises:
                    for ex in exercises:
                        hr_info    = ex.get('heartRate', {}) or {}
                        distanz_m  = _safe_float(ex.get('distanceMeters',
                                         ex.get('distance', 0))) or distanz_m_top
                        dauer_ms   = _safe_float(ex.get('durationMillis',
                                         0)) or dauer_ms_top
                        sport_ex   = ex.get('sport', sport_training)
                        sport_name = _sport_lesen(sport_ex)
                        distanz_km = round(distanz_m / 1000, 3) if distanz_m else None
                        dauer_min  = round(dauer_ms / 60000, 2) if dauer_ms else \
                                     _parse_iso_duration(ex.get('duration', ''))

                        # Pace-Korrektur: HIKING/WALKING < 10 min/km → RUNNING
                        if sport_name in ('HIKING', 'WALKING') and distanz_km and dauer_min:
                            if dauer_min / distanz_km < 10:
                                sport_name = 'RUNNING'

                        # Kategorie bestimmen
                        if 'laufband' in training_name or 'laufb.' in training_name or 'treadmill' in training_name:
                            kategorie = 'TREADMILL'
                        elif sport_name == 'TRAIL_RUNNING':
                            sport_name = 'RUNNING'
                            kategorie  = 'TRAIL'
                        elif sport_name == 'RUNNING':
                            kategorie  = 'OUTDOOR'
                        else:
                            kategorie  = None

                        zeilen.append({
                            'datum'     : datum,
                            'sport'     : sport_name,
                            'kategorie' : kategorie,
                            'dauer_min' : dauer_min,
                            'hr_avg'    : _safe_float(hr_info.get('average')) or hr_avg_top,
                            'hr_max'    : _safe_float(hr_info.get('maximum')) or hr_max_top,
                            'distanz_km': distanz_km,
                            'kalorien'  : _safe_float(ex.get('calories')) or kalorien_top,
                            'wochentag' : wochentag,
                            'jahr'      : jahr,
                        })"""

if old in content:
    content = content.replace(old, new)
    open(path, 'w').write(content)
    print("✅ Exercise-Block gepatcht")
else:
    print("❌ Stelle nicht gefunden")