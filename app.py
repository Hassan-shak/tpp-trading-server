"""
app.py
TPP Trading Server v5.0 — Flask entry point

Starts:
  - Flask app with webhook routes
  - APScheduler for all timed jobs
"""

import logging
import os
from flask import Flask

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(name)s] %(levelname)s — %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)

from webhook_handler import bp as webhook_bp
from scheduler import start_scheduler

app = Flask(__name__)
app.register_blueprint(webhook_bp)


@app.route("/", methods=["GET"])
def health():
    return {"status": "TPP Trading Server v5.0 — live"}, 200


if __name__ == "__main__":
    start_scheduler()
    port = int(os.environ.get("PORT", 5000))
    app.run(host="0.0.0.0", port=port)
