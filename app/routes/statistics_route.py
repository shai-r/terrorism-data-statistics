from flask import jsonify, Blueprint

from app.services.statistics_services import get_most_lethal_attack_types, get_avg_casualties_by_area, \
    get_top_5_groups_most_casualties, calculate_percent_change, calculate_event_victim_correlation, \
    identify_groups_in_same_attack, identify_shared_attack_strategies, identify_target_preferences, \
    identify_high_activity_regions, identify_influential_groups

statistics_blueprint = Blueprint('statistics', __name__)


@statistics_blueprint.route('/most_lethal_attack', methods=['GET'])
def get_most_lethal_attack():
    try:
        return jsonify({"most lethal attack": get_most_lethal_attack_types()}), 200
    except Exception as e:
        return jsonify({"error": f"{str(e)}"}), 500

@statistics_blueprint.route('/mean_casualties_by_area', methods=['GET'])#2
def get_mean_casualties_by_area():
    try:
        return jsonify(get_avg_casualties_by_area(0)), 200
    except Exception as e:
        return jsonify({"error": f"{str(e)}"}), 500

@statistics_blueprint.route('/top_five_groups_most_casualties', methods=['GET'])
def get_top_five_groups_most_casualties():
    try:
        return jsonify(get_top_5_groups_most_casualties()), 200
    except Exception as e:
        return jsonify({"error": f"{str(e)}"}), 500

@statistics_blueprint.route('/calculate_percent_change/', methods=['GET'])#6
@statistics_blueprint.route('/calculate_percent_change/<int:limit>', methods=['GET'])
def get_calculate_percent_change(limit: int = None):
    try:
        return jsonify(calculate_percent_change(limit=limit)), 200
    except Exception as e:
        return jsonify({"error": f"{str(e)}"}), 500

@statistics_blueprint.route('/calculate_event_victim_correlation', methods=['GET'])#10
@statistics_blueprint.route('/calculate_event_victim_correlation/<region>', methods=['GET'])
def get_calculate_event_victim_correlation(region = None):
    try:
        return jsonify(calculate_event_victim_correlation(region)), 200
    except Exception as e:
        return jsonify({"error": f"{str(e)}"}), 500

@statistics_blueprint.route('/identify_groups_in_same_attack', methods=['GET'])
def get_identify_groups_in_same_attack():
    try:
        return jsonify({"identify_groups_in_same_attack": identify_groups_in_same_attack()}), 200
    except Exception as e:
        return jsonify({"error": f"{str(e)}"}), 500

@statistics_blueprint.route('/identify_shared_attack_strategies/<region_type>', methods=['GET'])#14
def get_identify_shared_attack_strategies(region_type: str = "region"):
    try:
        return jsonify(identify_shared_attack_strategies(region_type)), 200
    except Exception as e:
        return jsonify({"error": f"{str(e)}"}), 500

@statistics_blueprint.route('/identify_target_preferences', methods=['GET'])
def get_identify_target_preferences():
    try:
        return jsonify({"identify_target_preferences": identify_target_preferences()}), 200
    except Exception as e:
        return jsonify({"error": f"{str(e)}"}), 500

@statistics_blueprint.route('/identify_high_activity_regions/<region_type>', methods=['GET'])#16
def get_identify_high_activity_regions(region_type: str = "region"):
    try:
        return jsonify(identify_high_activity_regions(region_type)), 200
    except Exception as e:
        return jsonify({"error": f"{str(e)}"}), 500

@statistics_blueprint.route('/identify_influential_groups/<region_type>', methods=['GET'])#16
def get_identify_influential_groups(region_type: str = "region"):
    try:
        return jsonify(identify_influential_groups(region_type)), 200
    except Exception as e:
        return jsonify({"error": f"{str(e)}"}), 500




