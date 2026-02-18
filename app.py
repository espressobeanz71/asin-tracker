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
logging.basicConfig(level=logging.DEBUG)
CORS(app)

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
        url = f"https://api.keepa.com/product?key={api_key}&domain=1&asin={','.join(batch)}&stats=1&offers=20"

        try:
            response = requests.get(url, timeout=30)
            data = response.json()

            if "products" not in data:
                errors.append(f"No products returned for batch {batch}")
                continue

            conn = get_db()
            cur = conn.cursor()

            for product in data["products"]:
                asin = product.get("asin")
                if not asin:
                    continue

                # Buy box price
                buybox_price = None
                csv = product.get("csv", [])
                if csv and len(csv) > 10 and csv[10]:
                    last = csv[10][-1] if isinstance(csv[10], list) else None
                    if last and last != -1:
                        buybox_price = last / 100

                # Sales rank
                rank = None
                if csv and len(csv) > 3 and csv[3]:
                    last_rank = csv[3][-1] if isinstance(csv[3], list) else None
                    if last_rank and last_rank != -1:
                        rank = last_rank

                # Seller count from stats
                seller_count = None
                stats = product.get("stats", {})
                if stats:
                    seller_count = stats.get("sellerCount")

                # Stock estimate
                stock = None
                if stats:
                    stock = stats.get("stockAmazon")

                # Is Amazon selling
                is_amazon = False
                if stats:
                    is_amazon = stats.get("isAmazon", False)

                # Update title if we got one
                title = product.get("title", "")
                if title:
                    cur.execute(
                        "UPDATE asins SET title = %s WHERE asin = %s AND (title IS NULL OR title = '')",
                        (title, asin)
                    )

                # Insert history row
                cur.execute("""
                    INSERT INTO history (asin, buybox_price, rank, seller_count, stock, is_amazon_selling)
                    VALUES (%s, %s, %s, %s, %s, %s)
                """, (asin, buybox_price, rank, seller_count, stock, is_amazon))

                updated += 1

            conn.commit()
            conn.close()

        except Exception as e:
            errors.append(f"Batch error: {str(e)}")

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
    conn = get_db()
    cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    cur.execute("""
        SELECT * FROM history 
        WHERE asin = %s 
        ORDER BY captured_at DESC 
        LIMIT 100
    """, (asin.upper(),))
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
            SELECT buybox_price, rank, seller_count
            FROM history
            WHERE asin = %s AND captured_at <= %s
            ORDER BY captured_at DESC
            LIMIT 1
        """, (asin, cutoff))
        return cur.fetchone()

    current = get_closest(0)
    past_30 = get_closest(30)
    past_90 = get_closest(90)

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
        "price_delta_30": delta(current, past_30, "buybox_price"),
        "price_delta_90": delta(current, past_90, "buybox_price"),
        "rank_delta_30": delta(current, past_30, "rank"),
        "rank_delta_90": delta(current, past_90, "rank"),
        "seller_delta_30": delta(current, past_30, "seller_count"),
        "seller_delta_90": delta(current, past_90, "seller_count"),
    }

    conn.close()
    return jsonify(result)


# -------------------------------
# HEALTH CHECK
# -------------------------------
@app.route("/api/health", methods=["GET"])
def health():
    return jsonify({"status": "ok", "message": "ASIN Tracker API is running"})


# -------------------------------
# SERVE FRONTEND
# -------------------------------
@app.route("/", methods=["GET"])
def frontend():
    return send_from_directory(".", "index.html")


# -------------------------------
# RUN
# -------------------------------
if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5000))
    app.run(host="0.0.0.0", port=port, debug=False)
