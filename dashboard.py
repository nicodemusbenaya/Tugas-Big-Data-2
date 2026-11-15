def gold_query(namespace):
    return f"""SELECT pickup_date, pickup_zone, borough,
       hour(pickup_hour) AS jam,
       element_at(array('7 Min','1 Sen','2 Sel','3 Rab','4 Kam','5 Jum','6 Sab'),
                  dayofweek(pickup_date)) AS hari,
       jumlah_penjemputan, jumlah_durasi_valid, total_menit_durasi_valid
FROM {namespace}.yellow_taxi_gold_hourly_zone"""


def dashboard_spec(namespace):
    dataset = "gold_taxi"
    pickups = "SUM(`jumlah_penjemputan`)"
    duration = "TRY_DIVIDE(SUM(`total_menit_durasi_valid`), SUM(`jumlah_durasi_valid`))"

    def place(widget, bounds):
        return {"widget": widget, "position": dict(zip(("x", "y", "width", "height"), bounds))}

    def query(fields):
        return [{"name": "main_query", "query": {
            "datasetName": dataset,
            "fields": [{"name": name, "expression": expression} for name, expression in fields.items()],
            "disaggregated": False,
        }}]

    def widget(name, title, kind, version, fields, encodings, bounds):
        return place({
            "name": name, "queries": query(fields),
            "spec": {"version": version, "widgetType": kind, "encodings": encodings,
                     "frame": {"showTitle": True, "title": title}},
        }, bounds)

    def axis(field, label, scale):
        return {"fieldName": field, "displayName": label, "scale": {"type": scale}}

    def filter_by(field, title, kind, bounds):
        return widget(f"filter_{field}", title, kind, 2, {field: f"`{field}`"},
                      {"fields": [{"fieldName": field, "queryName": "main_query"}]}, bounds)

    def counter(name, title, expression, decimals, bounds):
        return widget(name, title, "counter", 2, {"nilai": expression}, {"value": {
            "fieldName": "nilai", "displayName": title, "format": {
                "type": "number-plain", "decimalPlaces": {"type": "max", "places": decimals},
            },
        }}, bounds)

    def note(name, text, bounds):
        return place({"name": name, "multilineTextboxSpec": {"lines": text.splitlines(keepends=True)}}, bounds)

    busiest_zones = widget("zona", "Penjemputan menurut zona", "bar", 3,
                          {"zona": "`pickup_zone`", "pickup": pickups},
                          {"x": axis("pickup", "Penjemputan", "quantitative"),
                           "y": {**axis("zona", "Zona pickup", "categorical"),
                                 "scale": {"type": "categorical", "sort": {"by": "value"}}}}, (7, 7, 5, 6))
    busiest_zones["widget"]["queries"][0]["query"]["orders"] = [{"direction": "DESC", "expression": pickups}]
    layout = [
        note("judul", "# Operasi NYC Yellow Taxi\nJanuari–Februari 2025 · Waktu lokal New York", (0, 0, 12, 2)),
        filter_by("pickup_date", "Periode penjemputan", "filter-date-range-picker", (0, 2, 6, 2)),
        filter_by("borough", "Borough", "filter-multi-select", (6, 2, 6, 2)),
        counter("total_pickup", "Total penjemputan", pickups, 0, (0, 4, 6, 3)),
        counter("durasi_rata_rata", "Rata-rata durasi valid (menit)", duration, 1, (6, 4, 6, 3)),
        widget("tren", "Penjemputan per tanggal", "line", 3,
               {"tanggal": "`pickup_date`", "pickup": pickups},
               {"x": axis("tanggal", "Tanggal", "temporal"), "y": axis("pickup", "Penjemputan", "quantitative")},
               (0, 7, 7, 6)),
        busiest_zones,
        widget("jam_ramai", "Jam ramai menurut hari", "heatmap", 3,
               {"jam": "`jam`", "hari": "`hari`", "pickup": pickups},
               {"x": axis("jam", "Jam pickup", "categorical"), "y": axis("hari", "Hari", "categorical"),
                "color": axis("pickup", "Penjemputan", "quantitative")}, (0, 13, 6, 6)),
        widget("jam_durasi", "Rata-rata durasi menurut jam", "line", 3,
               {"jam": "`jam`", "menit": duration, "borough": "`borough`"},
               {"x": axis("jam", "Jam pickup", "quantitative"), "y": axis("menit", "Menit", "quantitative"),
                "color": axis("borough", "Borough", "categorical")}, (6, 13, 6, 6)),
        note("catatan", "Volume mencakup pickup valid. Perhitungan durasi memakai 0 < menit ≤ 180. Sumber NYC TLC.", (0, 19, 12, 2)),
    ]
    return {"datasets": [{"name": dataset, "displayName": "Gold · Pickup per jam dan zona",
                          "queryLines": gold_query(namespace).splitlines(keepends=True)}],
            "pages": [{"name": "operasi", "displayName": "Operasi Yellow Taxi",
                       "pageType": "PAGE_TYPE_CANVAS", "layout": layout}]}
