# =======================================
# ASIN TRACKER - BACKEND API
# =======================================

from flask import Flask, jsonify, request, send_from_directory
from flask_cors import CORS
import psycopg2
import psycopg2.extras
import os
import requests
import logging
from datetime import datetime, timedelta

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
                if len(csv) > 18 and csv[18]:
                    vals = [v for v in csv[18] if v != -1]
                    if vals:
                        buybox_price = vals[-1] / 100

                # --- NEW PRICE / LOWEST NEW (csv index 1) ---
                new_price = None
                if len(csv) > 1 and csv[1]:
                    vals = [v for v in csv[1] if v != -1]
                    if vals:
                        new_price = vals[-1] / 100

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

                if update_fields:
                    update_vals.append(asin)
                    cur.execute(
                        f"UPDATE asins SET {', '.join(update_fields)} WHERE asin = %s",
                        update_vals
                    )

                # --- INSERT HISTORY ---
                cur.execute("""
                    INSERT INTO history 
                        (asin, buybox_price, new_price, rank, seller_count, stock, is_amazon_selling)
                    VALUES (%s, %s, %s, %s, %s, %s, %s)
                """, (asin, buybox_price, new_price, rank, seller_count, stock, is_amazon))

                updated += 1
                logging.debug(
                    f"{asin}: buybox={buybox_price}, new={new_price}, "
                    f"rank={rank}, sellers={seller_count}, stock={stock}, "
                    f"weight={weight_lbs}, amazon={is_amazon}"
                )

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

@app.route("/history/<asin>", methods=["GET"])
def get_history(asin):
    days = request.args.get("days", 90, type=int)
    conn = get_db()
    cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    cur.execute("""
        SELECT * FROM history
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
