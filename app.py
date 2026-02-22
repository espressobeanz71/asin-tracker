# =======================================
# ASIN TRACKER - BACKEND API
# =======================================

from flask import Flask, jsonify, request, send_from_directory
from flask_cors import CORS
from collections import defaultdict
import psycopg2
import psycopg2.extras
import os
import requests
import logging
from datetime import datetime, timedelta, date

app = Flask(__name__)
CORS(app)
logging.basicConfig(level=logging.DEBUG)

# -------------------------------
# AMAZON REFERRAL FEE MAP
# -------------------------------
REFERRAL_FEES = {
    "baby products": 0.08,
    "beauty": 0.08,
    "beauty & personal care": 0.08,
    "clothing & accessories": 0.17,
    "grocery & gourmet food": 0.08,
    "grocery": 0.08,
    "health & household": 0.08,
    "health, household & baby care": 0.08,
    "home & kitchen": 0.15,
    "kitchen & dining": 0.15,
    "office products": 0.15,
    "pet supplies": 0.15,
    "sports & outdoors": 0.15,
    "sports outdoors": 0.15,
    "tools & home improvement": 0.15,
    "toys & games": 0.15,
    "arts & crafts": 0.15,
    "automotive": 0.12,
    "industrial & scientific": 0.12,
    "musical instruments": 0.15,
    "patio, lawn & garden": 0.15,
    "garden & outdoor": 0.15,
}

def get_referral_fee(category):
    if not category:
        return 0.15  # default
    key = category.lower().strip()
    for k, v in REFERRAL_FEES.items():
        if k in key or key in k:
            return v
    return 0.15  # default fallback


# -------------------------------
# DATABASE CONNECTION
# -------------------------------
def get_db():
    conn = psycopg2.connect(os.environ.get("DATABASE_URL"))
    return conn


# -------------------------------
# HELPER - GET SETTING
# -------------------------------
def get_setting(key):
    conn = get_db()
    cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    cur.execute("SELECT value FROM settings WHERE key = %s", (key,))
    row = cur.fetchone()
    conn.close()
    return row["value"] if row else None


# -------------------------------
# ROUTES - ASINS
# -------------------------------

# Get all ASINs
@app.route("/asins", methods=["GET"])
def get_asins():
    conn = get_db()
    cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    cur.execute("""
        SELECT a.*,
          (SELECT buybox_price FROM history h WHERE h.asin = a.asin ORDER BY captured_at DESC LIMIT 1) as current_price,
          (SELECT new_price FROM history h WHERE h.asin = a.asin ORDER BY captured_at DESC LIMIT 1) as current_new_price,
          (SELECT rank FROM history h WHERE h.asin = a.asin ORDER BY captured_at DESC LIMIT 1) as current_rank,
          (SELECT seller_count FROM history h WHERE h.asin = a.asin ORDER BY captured_at DESC LIMIT 1) as current_sellers,
          (SELECT stock FROM history h WHERE h.asin = a.asin ORDER BY captured_at DESC LIMIT 1) as current_stock,
          (SELECT is_amazon_selling FROM history h WHERE h.asin = a.asin ORDER BY captured_at DESC LIMIT 1) as amazon_selling
        FROM asins a
        WHERE a.is_active = TRUE
        ORDER BY a.created_at DESC
    """)
    rows = cur.fetchall()
    conn.close()
    return jsonify(rows)


# Add a new ASIN
@app.route("/asins", methods=["POST"])
def add_asin():
    try:
        data = request.json
        logging.debug(f"Received data: {data}")

        asin = data.get("asin", "").strip().upper()
        if not asin:
            return jsonify({"error": "ASIN is required"}), 400

        conn = get_db()
        cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)

        try:
            cur.execute("""
                INSERT INTO asins (asin, title, brand, category, weight, cost, notes)
                VALUES (%s, %s, %s, %s, %s, %s, %s)
                RETURNING *
            """, (
                asin,
                data.get("title", ""),
                data.get("brand", ""),
                data.get("category", ""),
                data.get("weight"),
                data.get("cost"),
                data.get("notes", "")
            ))
            conn.commit()
            row = cur.fetchone()
            conn.close()
            return jsonify(dict(row)), 201
        except Exception as db_error:
            conn.rollback()
            conn.close()
            logging.error(f"Database error: {str(db_error)}")
            return jsonify({"error": str(db_error)}), 500

    except Exception as e:
        logging.error(f"General error: {str(e)}")
        return jsonify({"error": str(e)}), 500


# Update an ASIN (cost, notes, fba_fee override etc)
@app.route("/asins/<asin>", methods=["PATCH"])
def update_asin(asin):
    try:
        data = request.json
        conn = get_db()
        cur = conn.cursor()

        fields = []
        values = []

        for field in ["title", "cost", "weight", "notes", "fba_fee", "referral_fee_override"]:
            if field in data:
                fields.append(f"{field} = %s")
                values.append(data[field])

        if not fields:
            return jsonify({"error": "No fields to update"}), 400

        values.append(asin.upper())
        cur.execute(
            f"UPDATE asins SET {', '.join(fields)} WHERE asin = %s",
            values
        )
        conn.commit()
        conn.close()
        return jsonify({"success": True})
    except Exception as e:
        logging.error(f"Update error: {str(e)}")
        return jsonify({"error": str(e)}), 500


# Delete an ASIN (soft delete)
@app.route("/asins/<asin>", methods=["DELETE"])
def delete_asin(asin):
    conn = get_db()
    cur = conn.cursor()
    cur.execute("UPDATE asins SET is_active = FALSE WHERE asin = %s", (asin.upper(),))
    conn.commit()
    conn.close()
    return jsonify({"success": True})


# -------------------------------
# ROUTES - SETTINGS
# -------------------------------

@app.route("/settings", methods=["GET"])
def get_settings():
    conn = get_db()
    cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    cur.execute("SELECT key, value FROM settings WHERE key != 'keepa_api_key'")
    rows = cur.fetchall()
    conn.close()
    return jsonify({r["key"]: r["value"] for r in rows})


@app.route("/settings", methods=["POST"])
def save_settings():
    try:
        data = request.json
        conn = get_db()
        cur = conn.cursor()
        for key, value in data.items():
            if key == "keepa_api_key":
                continue  # never overwrite API key via this route
            cur.execute("""
                INSERT INTO settings (key, value)
                VALUES (%s, %s)
                ON CONFLICT (key) DO UPDATE SET value = EXCLUDED.value, updated_at = NOW()
            """, (key, str(value)))
        conn.commit()
        conn.close()
        return jsonify({"success": True})
    except Exception as e:
        logging.error(f"Settings error: {str(e)}")
        return jsonify({"error": str(e)}), 500


# -------------------------------
# ROUTES - KEEPA SYNC
# -------------------------------

@app.route("/sync", methods=["POST"])
def sync_keepa():
    api_key = get_setting("keepa_api_key")
    if not api_key:
        return jsonify({"error": "Keepa API key not configured"}), 500

    conn = get_db()
    cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    cur.execute("SELECT asin FROM asins WHERE is_active = TRUE")
    asins = [row["asin"] for row in cur.fetchall()]
    conn.close()

    if not asins:
        return jsonify({"message": "No ASINs to sync"})

    updated = 0
    errors = []
    batch_size = 10

    for i in range(0, len(asins), batch_size):
        batch = asins[i:i+batch_size]
        url = (
            f"https://api.keepa.com/product"
            f"?key={api_key}"
            f"&domain=1"
            f"&asin={','.join(batch)}"
            f"&stats=1"
            f"&offers=20"
            f"&history=1"
        )

        try:
            response = requests.get(url, timeout=30)
            data = response.json()

            logging.debug(f"Keepa response keys: {list(data.keys())}")

            if "products" not in data:
                errors.append(f"No products in response for batch {batch}")
                continue

            conn = get_db()
            cur = conn.cursor()

            for product in data["products"]:
                asin = product.get("asin")
                if not asin:
                    continue

                csv = product.get("csv") or []

              # --- BUY BOX PRICE (csv index 18) ---
                buybox_price = None
                if len(csv) > 18 and csv[18] and len(csv[18]) >= 2:
                    prices = [csv[18][i] for i in range(1, len(csv[18]), 2)
                              if csv[18][i] not in (-1, 0) and csv[18][i] < 1000000]
                    if prices:
                        buybox_price = prices[-1] / 100

                # --- NEW PRICE / LOWEST NEW (csv index 1) ---
                new_price = None
                if len(csv) > 1 and csv[1] and len(csv[1]) >= 2:
                    prices = [csv[1][i] for i in range(1, len(csv[1]), 2)
                              if csv[1][i] not in (-1, 0) and csv[1][i] < 1000000]
                    if prices:
                        new_price = prices[-1] / 100

                # --- SALES RANK (csv index 3) ---
                rank = None
                if len(csv) > 3 and csv[3]:
                    vals = [v for v in csv[3] if v != -1]
                    if vals:
                        rank = vals[-1]

                # --- SELLER COUNT (csv index 11) ---
                seller_count = None
                if len(csv) > 11 and csv[11]:
                    vals = [v for v in csv[11] if v != -1]
                    if vals:
                        seller_count = vals[-1]

                # --- AMAZON STOCK (csv index 11 is marketplace, 
                #     Amazon stock from stats) ---
                stats = product.get("stats") or {}
                stock = None
                stock_val = stats.get("stockAmazon")
                if stock_val is not None and stock_val != -1:
                    stock = stock_val

                # --- IS AMAZON SELLING ---
                is_amazon = bool(stats.get("isAmazon", False))

                # --- WEIGHT (grams → lbs) ---
                weight_grams = product.get("packageWeight")
                weight_lbs = None
                if weight_grams and weight_grams > 0:
                    weight_lbs = round(weight_grams / 453.592, 2)

                # --- CATEGORY ---
                category = None
                root_cat = product.get("rootCategory")
                if root_cat:
                    category = str(root_cat)

                # --- TITLE ---
                title = product.get("title", "")

                # --- IMAGE ---
                image_url = None
                images = product.get("imagesCSV", "")
                if images:
                    first_image = images.split(",")[0].strip()
                    if first_image:
                        image_url = f"https://images-na.ssl-images-amazon.com/images/I/{first_image}"

                # --- UPDATE ASINS TABLE ---
                update_fields = []
                update_vals = []

                if title:
                    update_fields.append("title = %s")
                    update_vals.append(title)
                if weight_lbs is not None:
                    update_fields.append("weight = %s")
                    update_vals.append(weight_lbs)
                if category:
                    update_fields.append("category = %s")
                    update_vals.append(category)
                if image_url:
                    update_fields.append("image_url = %s")
                    update_vals.append(image_url)

                if update_fields:
                    update_vals.append(asin)
                    cur.execute(
                        f"UPDATE asins SET {', '.join(update_fields)} WHERE asin = %s",
                        update_vals
                    )
                    
               # --- CHECK IF ASIN HAS EXISTING HISTORY ---
                cur.execute("SELECT COUNT(*) FROM history WHERE asin = %s", (asin,))
                history_count = cur.fetchone()[0]

                if history_count < 30:
                    # Not enough history — back-fill 180 days
                    logging.debug(f"{asin}: Less than 30 days history found, back-filling 180 days")

                    # Build a lookup of date -> values from Keepa arrays
                    # Keepa timestamps are minutes since 2011-01-01
                    KEEPA_EPOCH = datetime(2011, 1, 1)
                    cutoff = datetime.utcnow() - timedelta(days=180)

                    def extract_keepa_series(arr):
                        """Extract [(datetime, value)] from Keepa price array - values in cents"""
                        series = []
                        if not arr or len(arr) < 2:
                            return series
                        for i in range(0, len(arr) - 1, 2):
                            try:
                                ts  = arr[i]
                                val = arr[i + 1]
                                if ts is None or val is None:
                                    continue
                                dt = KEEPA_EPOCH + timedelta(minutes=int(ts))
                                if dt < cutoff:
                                    continue
                                if val in (-1, 0) or val > 1000000:
                                    series.append((dt, None))
                                else:
                                    series.append((dt, round(val / 100, 2)))
                            except Exception:
                                continue
                        return series

                    def extract_keepa_int_series(arr):
                        """Extract [(datetime, value)] from Keepa int array - rank/sellers"""
                        series = []
                        if not arr or len(arr) < 2:
                            return series
                        for i in range(0, len(arr) - 1, 2):
                            try:
                                ts  = arr[i]
                                val = arr[i + 1]
                                if ts is None or val is None:
                                    continue
                                dt = KEEPA_EPOCH + timedelta(minutes=int(ts))
                                if dt < cutoff:
                                    continue
                                if val == -1:
                                    series.append((dt, None))
                                else:
                                    series.append((dt, int(val)))
                            except Exception:
                                continue
                        return series

                    def extract_keepa_int_series(arr):
                        """Same but for rank/seller counts (no /100)"""
                        series = []
                        if not arr or len(arr) < 2:
                            return series
                        for i in range(0, len(arr) - 1, 2):
                            ts  = arr[i]
                            val = arr[i + 1]
                            if ts is None or val is None:
                                continue
                            dt = KEEPA_EPOCH + timedelta(minutes=ts)
                            if dt < cutoff:
                                continue
                            if val == -1:
                                val = None
                            series.append((dt, val))
                        return series

                    bb_series     = extract_keepa_series(csv[18] if len(csv) > 18 and csv[18] else [])
                    new_series    = extract_keepa_series(csv[1]  if len(csv) > 1  and csv[1]  else [])
                    rank_series   = extract_keepa_int_series(csv[3]  if len(csv) > 3  and csv[3]  else [])
                    seller_series = extract_keepa_int_series(csv[11] if len(csv) > 11 and csv[11] else [])

                    # Build daily snapshots by date
                    from collections import defaultdict
                    daily = defaultdict(lambda: {
                        "buybox_price": None,
                        "new_price": None,
                        "rank": None,
                        "seller_count": None
                    })

                    for dt, val in bb_series:
                        day = dt.date()
                        if daily[day]["buybox_price"] is None:
                            daily[day]["buybox_price"] = val

                    for dt, val in new_series:
                        day = dt.date()
                        if daily[day]["new_price"] is None:
                            daily[day]["new_price"] = val

                    for dt, val in rank_series:
                        day = dt.date()
                        if daily[day]["rank"] is None:
                            daily[day]["rank"] = val

                    for dt, val in seller_series:
                        day = dt.date()
                        if daily[day]["seller_count"] is None:
                            daily[day]["seller_count"] = val

                    # Insert all days in one bulk operation
                    if daily:
                        bulk_rows = []
                        for day, vals in sorted(daily.items()):
                            snap_dt = datetime.combine(day, datetime.min.time())
                            bulk_rows.append((
                                asin,
                                snap_dt,
                                vals["buybox_price"],
                                vals["new_price"],
                                vals["rank"],
                                vals["seller_count"],
                                None,
                                is_amazon
                            ))

                        psycopg2.extras.execute_values(cur, """
                            INSERT INTO history
                                (asin, captured_at, buybox_price, new_price, rank, seller_count, stock, is_amazon_selling)
                            VALUES %s
                            ON CONFLICT DO NOTHING
                        """, bulk_rows)

                        logging.debug(f"{asin}: Back-filled {len(bulk_rows)} days of history")

                    logging.debug(f"{asin}: Back-filled {len(daily)} days of history")

                else:
                    # Existing ASIN — just insert today's snapshot
                    cur.execute("""
                        INSERT INTO history
                            (asin, buybox_price, new_price, rank, seller_count, stock, is_amazon_selling)
                        VALUES (%s, %s, %s, %s, %s, %s, %s)
                    """, (asin, buybox_price, new_price, rank, seller_count, stock, is_amazon))

                updated += 1

            conn.commit()
            conn.close()

        except Exception as e:
            errors.append(f"Batch error: {str(e)}")
            logging.error(f"Sync error: {str(e)}")

    return jsonify({
        "success": True,
        "updated": updated,
        "errors": errors
    })


# -------------------------------
# ROUTES - HISTORY & DELTAS
# -------------------------------
@app.route("/deltas", methods=["GET"])
def get_all_deltas():
    try:
        conn = get_db()
        cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        now = datetime.utcnow()

        # Get all active ASINs
        cur.execute("SELECT asin FROM asins WHERE is_active = TRUE")
        asins = [row["asin"] for row in cur.fetchall()]

        if not asins:
            conn.close()
            return jsonify({})

        def get_snapshots(days_back):
            cutoff = now - timedelta(days=days_back)
            cur.execute("""
                SELECT DISTINCT ON (asin) 
                    asin, buybox_price, new_price, rank, seller_count
                FROM history
                WHERE asin = ANY(%s) 
                AND captured_at <= %s
                ORDER BY asin, captured_at DESC
            """, (asins, cutoff))
            rows = cur.fetchall()
            return {r["asin"]: r for r in rows}

        current  = get_snapshots(0)
        past_30  = get_snapshots(30)
        past_90  = get_snapshots(90)
        past_180 = get_snapshots(180)

        def delta(c, p, field):
            if not c or not p:
                return None
            cv = c.get(field)
            pv = p.get(field)
            if cv is None or pv is None:
                return None
            return float(cv) - float(pv)

        result = {}
        for asin in asins:
            c = current.get(asin)
            p30  = past_30.get(asin)
            p90  = past_90.get(asin)
            p180 = past_180.get(asin)
            result[asin] = {
                "asin": asin,
                "price_delta_30":      delta(c, p30,  "buybox_price"),
                "price_delta_90":      delta(c, p90,  "buybox_price"),
                "price_delta_180":     delta(c, p180, "buybox_price"),
                "new_price_delta_30":  delta(c, p30,  "new_price"),
                "new_price_delta_90":  delta(c, p90,  "new_price"),
                "new_price_delta_180": delta(c, p180, "new_price"),
                "rank_delta_30":       delta(c, p30,  "rank"),
                "rank_delta_90":       delta(c, p90,  "rank"),
                "rank_delta_180":      delta(c, p180, "rank"),
                "seller_delta_30":     delta(c, p30,  "seller_count"),
                "seller_delta_90":     delta(c, p90,  "seller_count"),
                "seller_delta_180":    delta(c, p180, "seller_count"),
            }

        conn.close()
        return jsonify(result)

    except Exception as e:
        logging.error(f"Bulk deltas error: {str(e)}")
        return jsonify({"error": str(e)}), 500
        
@app.route("/history/<asin>", methods=["GET"])
def get_history(asin):
    days = request.args.get("days", 90, type=int)
    conn = get_db()
    cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    cur.execute("""
        SELECT captured_at, buybox_price, new_price, rank, seller_count
        FROM history
        WHERE asin = %s
        AND captured_at >= NOW() - INTERVAL '%s days'
        ORDER BY captured_at ASC
    """, (asin.upper(), days))
    rows = cur.fetchall()
    conn.close()
    return jsonify(rows)

@app.route("/deltas/<asin>", methods=["GET"])
def get_deltas(asin):
    asin = asin.upper()
    conn = get_db()
    cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)

    now = datetime.utcnow()

    def get_closest(days_back):
        cutoff = now - timedelta(days=days_back)
        cur.execute("""
            SELECT buybox_price, new_price, rank, seller_count
            FROM history
            WHERE asin = %s AND captured_at <= %s
            ORDER BY captured_at DESC
            LIMIT 1
        """, (asin, cutoff))
        return cur.fetchone()

    current  = get_closest(0)
    past_30  = get_closest(30)
    past_90  = get_closest(90)
    past_180 = get_closest(180)

    def delta(current_row, past_row, field):
        if not current_row or not past_row:
            return None
        c = current_row.get(field)
        p = past_row.get(field)
        if c is None or p is None:
            return None
        return float(c) - float(p)

    result = {
        "asin": asin,
        "price_delta_30":       delta(current, past_30,  "buybox_price"),
        "price_delta_90":       delta(current, past_90,  "buybox_price"),
        "price_delta_180":      delta(current, past_180, "buybox_price"),
        "new_price_delta_30":   delta(current, past_30,  "new_price"),
        "new_price_delta_90":   delta(current, past_90,  "new_price"),
        "new_price_delta_180":  delta(current, past_180, "new_price"),
        "rank_delta_30":        delta(current, past_30,  "rank"),
        "rank_delta_90":        delta(current, past_90,  "rank"),
        "rank_delta_180":       delta(current, past_180, "rank"),
        "seller_delta_30":      delta(current, past_30,  "seller_count"),
        "seller_delta_90":      delta(current, past_90,  "seller_count"),
        "seller_delta_180":     delta(current, past_180, "seller_count"),
    }

    conn.close()
    return jsonify(result)
# -------------------------------
# ROUTES - SOURCES
# -------------------------------

@app.route("/sources/<asin>", methods=["GET"])
def get_sources(asin):
    try:
        conn = get_db()
        cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        cur.execute("""
            SELECT * FROM sources 
            WHERE asin = %s 
            ORDER BY created_at ASC
        """, (asin.upper(),))
        rows = cur.fetchall()
        conn.close()
        return jsonify(rows)
    except Exception as e:
        logging.error(f"Get sources error: {str(e)}")
        return jsonify({"error": str(e)}), 500


@app.route("/sources", methods=["POST"])
def add_source():
    try:
        data = request.json
        asin = data.get("asin", "").strip().upper()
        url  = data.get("url", "").strip()

        if not asin or not url:
            return jsonify({"error": "ASIN and URL are required"}), 400

        conn = get_db()
        cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        cur.execute("""
            INSERT INTO sources (asin, supplier_name, url, cost, notes)
            VALUES (%s, %s, %s, %s, %s)
            RETURNING *
        """, (
            asin,
            data.get("supplier_name", ""),
            url,
            data.get("cost"),
            data.get("notes", "")
        ))
        conn.commit()
        row = cur.fetchone()
        conn.close()
        return jsonify(dict(row)), 201
    except Exception as e:
        logging.error(f"Add source error: {str(e)}")
        return jsonify({"error": str(e)}), 500


@app.route("/sources/<source_id>", methods=["DELETE"])
def delete_source(source_id):
    try:
        conn = get_db()
        cur = conn.cursor()
        cur.execute("DELETE FROM sources WHERE id = %s", (source_id,))
        conn.commit()
        conn.close()
        return jsonify({"success": True})
    except Exception as e:
        logging.error(f"Delete source error: {str(e)}")
        return jsonify({"error": str(e)}), 500


@app.route("/sources/<source_id>", methods=["PATCH"])
def update_source(source_id):
    try:
        data = request.json
        conn = get_db()
        cur = conn.cursor()

        fields = []
        values = []

        for field in ["supplier_name", "url", "cost", "notes"]:
            if field in data:
                fields.append(f"{field} = %s")
                values.append(data[field])

        if not fields:
            return jsonify({"error": "No fields to update"}), 400

        values.append(source_id)
        cur.execute(
            f"UPDATE sources SET {', '.join(fields)} WHERE id = %s",
            values
        )
        conn.commit()
        conn.close()
        return jsonify({"success": True})
    except Exception as e:
        logging.error(f"Update source error: {str(e)}")
        return jsonify({"error": str(e)}), 500

@app.route("/sources", methods=["GET"])
def get_all_sources():
    try:
        conn = get_db()
        cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        cur.execute("""
            SELECT s.* FROM sources s
            JOIN asins a ON s.asin = a.asin
            WHERE a.is_active = TRUE
            ORDER BY s.supplier_name ASC
        """)
        rows = cur.fetchall()
        conn.close()
        return jsonify(rows)
    except Exception as e:
        logging.error(f"Get all sources error: {str(e)}")
        return jsonify({"error": str(e)}), 500

# -------------------------------
# SERVE FRONTEND
# -------------------------------
@app.route("/", methods=["GET"])
def frontend():
    return send_from_directory(".", "index.html")


# -------------------------------
# HEALTH CHECK
# -------------------------------
@app.route("/api/health", methods=["GET"])
def health():
    return jsonify({"status": "ok", "message": "ASIN Tracker API is running"})


# -------------------------------
# RUN
# -------------------------------
if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5000))
    app.run(host="0.0.0.0", port=port, debug=False)
