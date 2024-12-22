from flask import jsonify, Blueprint

from app.services.statistics_services import get_most_lethal_attack_types, get_avg_casualties_by_area, \
    get_top_5_groups_most_casualties, calculate_percent_change, calculate_event_victim_correlation

statistics_blueprint = Blueprint('statistics', __name__)


@statistics_blueprint.route('/most_lethal_attack', methods=['GET'])
def get_most_lethal_attack():
    try:
        return jsonify({"most lethal attack": get_most_lethal_attack_types()}), 200
    except Exception as e:
        return jsonify({"error": f"{str(e)}"}), 500

@statistics_blueprint.route('/mean_casualties_by_area', methods=['GET'])
def get_mean_casualties_by_area():
    try:
        return jsonify({"avg casualties by area": get_avg_casualties_by_area(0)}), 200
    except Exception as e:
        return jsonify({"error": f"{str(e)}"}), 500

@statistics_blueprint.route('/top_five_groups_most_casualties', methods=['GET'])
def get_top_five_groups_most_casualties():
    try:
        return jsonify({"avg casualties by area": get_top_5_groups_most_casualties()}), 200
    except Exception as e:
        return jsonify({"error": f"{str(e)}"}), 500

@statistics_blueprint.route('/calculate_percent_change', methods=['GET'])
def get_calculate_percent_change():
    try:
        return jsonify({"percent change": calculate_percent_change()}), 200
    except Exception as e:
        return jsonify({"error": f"{str(e)}"}), 500
@statistics_blueprint.route('/calculate_event_victim_correlation', methods=['GET'])
def get_calculate_event_victim_correlation():
    try:
        return jsonify({"percent change": calculate_event_victim_correlation()}), 200
    except Exception as e:
        return jsonify({"error": f"{str(e)}"}), 500




