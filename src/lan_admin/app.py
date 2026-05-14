'''Compatibility entrypoint for the LAN management Flask app.'''

import os

from app import create_app


app = create_app()


if __name__ == "__main__":
    flask_debug = os.getenv("FLASK_DEBUG", "").strip().lower() in {"1", "true", "yes", "on"}
    app.run(debug=flask_debug, use_reloader=flask_debug, host="127.0.0.1", port=5051)
