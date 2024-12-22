import pandas as pd

from app.mongodb.database_mongo import terrorism_collection

events = terrorism_collection.find()
events_df = pd.DataFrame(events)

def calculate_victims(event):
    killed = event['casualties']['killed']
    wounded = event['casualties']['wounded']
    total_victims = killed * 2 + wounded
    return total_victims

def get_most_lethal_attack_types(limit: int = 5): #1
    event_data = []
    for event in events:
        event_data.append({
            'event_id': event['event_id'],
            'attack_type': event['attack_type']['primary']['text'],
            'victims': calculate_victims(event)
        })
        if len(event['attack_type']['secondary']['text']) > 0:
            event_data.append({
                'event_id': event['event_id'],
                'attack_type': event['attack_type']['secondary']['text'],
                'victims': calculate_victims(event)
            })
        if len(event['attack_type']['tertiary']['text']) > 0:
            event_data.append({
                'event_id': event['event_id'],
                'attack_type': event['attack_type']['tertiary']['text'],
                'victims': calculate_victims(event)
            })

    df = pd.DataFrame(event_data)
    return df.groupby('attack_type')['victims'].sum().nlargest(limit).to_dict()

def calculate_area_victims(event):
    location = event['location']
    victims = calculate_victims(event)
    return location['region']['text'], victims

def get_avg_casualties_by_area(limit: int = 5): #2
    area_data = []
    for event in events:
        region, victims = calculate_area_victims(event)
        area_data.append({'region': region, 'victims': victims})

    area_df = pd.DataFrame(area_data)

    avg_victims_per_region = area_df.groupby('region')['victims'].mean()
    if limit == 5:
        avg_victims_per_region = avg_victims_per_region.nlargest(limit)

    return avg_victims_per_region.to_dict()

def get_top_5_groups_most_casualties(): #3
    group_data = []
    for event in events:
        groups = event.get('groups', [])
        for group in groups:
            group_data.append({
                'group_name': group['name'],
                'victims': calculate_victims(event)
            })

    group_df = pd.DataFrame(group_data)

    return group_df.groupby('group_name')['victims'].sum().nlargest(5).to_dict()

def calculate_percent_change(limit: int = None): #6
    events_df['year'] = events_df['date'].apply(lambda x: x['year'])
    events_df['region'] = events_df['location'].apply(lambda x: x['region']['text'])

    attacks_by_year_region = events_df.groupby(['year', 'region']).size().unstack()

    percent_change = attacks_by_year_region.pct_change(axis=0) * 100

    # percent_change = percent_change.drop(percent_change.index[0])

    return percent_change.head(limit).to_dict() if limit == 5 else percent_change.to_dict()

def calculate_event_victim_correlation(region_filter=None): #10
    eventss = terrorism_collection.find()
    df = pd.DataFrame([
        {
            'region': event['location']['region']['text'],
            'year': event['date']['year'],
            'victims': calculate_victims(event)
        } for event in eventss
    ])

    correlations = {}

    if region_filter:
        regions = [region_filter]
    else:
        regions = df['region'].unique()

    for region in regions:
        region_df = df[df['region'] == region]

        yearly_data = region_df.groupby('year').agg(
            events_count=('year', 'size'),
            victims_count=('victims', 'sum')
        )

        if yearly_data.shape[0] > 1:  # Ensure there is enough data to calculate correlation
            correlation = yearly_data.corr().loc['events_count', 'victims_count']
        else:
            correlation = None

        correlations[region] = correlation

    return correlations
