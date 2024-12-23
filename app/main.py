from flask import Flask

from app.routes.statistics_route import statistics_blueprint


app = Flask(__name__)
app.register_blueprint(statistics_blueprint, url_prefix="/api/statistics")


if __name__ == "__main__":
    app.run(port=5001, debug=True)