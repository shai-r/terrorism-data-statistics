import pandas as pd
from typing import List

from app.mongodb.database_mongo import terrorism_collection

def calculate_victims(event):
    killed = event['casualties']['killed']
    wounded = event['casualties']['wounded']
    total_victims = killed * 2 + wounded
    return total_victims

def get_most_lethal_attack_types(limit: int = 5): #1
    events = list(terrorism_collection.find(
        {},
        {
            'attack_type.primary.text': 1,
            'attack_type.secondary.text': 1,
            'attack_type.tertiary.text': 1,
            'victims': 1,
            'casualties.killed': 1,
            'casualties.wounded': 1
        }
    ))
    return pd.DataFrame([{
        'attack_type': event['attack_type']['primary']['text'],
        'victims': calculate_victims(event)
    } for event in events] + [{
        'attack_type': event['attack_type']['secondary']['text'],
        'victims': calculate_victims(event)
    } for event in events
        if len(event.get('attack_type', {}).get('secondary', {}).get('text', '')) > 0
    ] + [{
        'attack_type': event['attack_type']['tertiary']['text'],
        'victims': calculate_victims(event)
    } for event in events
        if len(event.get('attack_type', {}).get('tertiary', {}).get('text', '')) > 0
    ] ).groupby('attack_type')['victims'].sum().nlargest(limit).to_dict()


def calculate_area_victims(event):
    location = event['location']
    victims = calculate_victims(event)
    return location['region']['text'], victims

def get_avg_casualties_by_area(limit: int = None): #2
    events = list(terrorism_collection.find(
        {},
        {
            'location.latitude': 1,
            'location.longitude': 1,
            'location.region.text': 1,
            'victims': 1,
            'casualties.killed': 1,
            'casualties.wounded': 1
        }
    ))

    grouped_data = pd.DataFrame([{
        'region': event.get('location', {}).get('region', {}).get('text', 'Unknown'),
        'victims': calculate_victims(event),
        'latitude': event.get('location', {}).get('latitude'),
        'longitude': event.get('location', {}).get('longitude')} for event in events
    ]).groupby('region').agg(
        victims=('victims', 'mean'),
        latitude=('latitude', 'mean'),
        longitude=('longitude', 'mean')
    ).reset_index()

    if limit:
        grouped_data = grouped_data.nlargest(limit, 'victims')

    return [
        {
            'region': row['region'],
            'latitude': row['latitude'],
            'longitude': row['longitude'],
            'victims': row['victims']
        }
        for _, row in grouped_data.iterrows()
    ]


def get_top_5_groups_most_casualties(): #3
    events = list(terrorism_collection.find(
        {},
        {
            'groups.name': 1,
            'victims': 1,
            'casualties.killed': 1,
            'casualties.wounded': 1
        }
    ))

    return pd.DataFrame([
        {
            'group_name': group['name'],
            'victims': calculate_victims(event)
        }
        for event in events
        for group in event.get('groups', [])
    ]).groupby('group_name')['victims'].sum().nlargest(5).to_dict()


def calculate_percent_change(limit: int = None): #6
    events = list(terrorism_collection.find(
        {},
        {
            'date.year': 1,
            'location.region.text': 1,
            'location.latitude': 1,
            'location.longitude': 1
        }
    ))

    events_df = pd.DataFrame([
        {
            'year': event.get('date', {}).get('year', None),
            'region': event.get('location', {}).get('region', {}).get('text', None),
            'latitude': event.get('location', {}).get('latitude', None),
            'longitude': event.get('location', {}).get('longitude', None)
        }
        for event in events
    ])

    region_avg_coords = events_df.groupby('region')[['latitude', 'longitude']].mean()

    attacks_by_year_region = events_df.groupby(['year', 'region']).size().unstack()

    percent_change = attacks_by_year_region.pct_change(axis=0) * 100

    result = [
        {
            'region': region,
            'latitude': region_avg_coords.loc[region, 'latitude'],
            'longitude': region_avg_coords.loc[region, 'longitude'],
            'percent_change': percent_change[region].dropna().to_dict()
        }
        for region in region_avg_coords.index
    ]

    return result[:limit] if limit else result

correlation_events_victims = lambda df: (
    df.corr().loc)['events_count', 'victims_count'] if df.shape[0] > 1 else None



def calculate_event_victim_correlation(region_filter=None):  # 10
    events = list(terrorism_collection.find(
        {},
        {
            'date.year': 1,
            'location.region.text': 1,
            'casualties.killed': 1,
            'casualties.wounded': 1,
            'location.latitude': 1,
            'location.longitude': 1
        }
    ))

    events_df = pd.DataFrame([
        {
            'region': event['location']['region']['text'],
            'year': event['date']['year'],
            'victims': calculate_victims(event),
            'latitude': event['location']['latitude'],
            'longitude': event['location']['longitude']
        } for event in events
    ])

    region_avg_coords = events_df.groupby('region')[['latitude', 'longitude']].mean()

    regions = [region_filter] if region_filter else events_df['region'].unique()

    result = [
        {
            'region': region,
            'correlation': correlation_events_victims(
                events_df[events_df['region'] == region].groupby('year').agg(
                    events_count=('year', 'size'),
                    victims_count=('victims', 'sum')
                )
            ),
            'coordinates': region_avg_coords.loc[region].to_dict()
        }
        for region in regions
    ]

    return result


def identify_groups_in_same_attack(): #13
    events = list(terrorism_collection.find(
        {},
        {
            'event_id': 1,
            'groups.name': 1
        }
    ))

    df = pd.DataFrame([{
        'event_id': event['event_id'],
        'group': group['name']
    } for event in events for group in event['groups']])
    df = df[df['group'] != 'Unknown']
    grouped = df.groupby('event_id')['group'].apply(lambda x: list(set(x)))

    grouped = grouped[grouped.apply(lambda x: len(x) > 1)]

    unique_groups = [list(g) for g in set(tuple(g) for g in grouped.values.tolist())]

    return unique_groups

def calculate_average_coordinates(events: List[dict], filter: str) -> List[dict]:
    df = pd.DataFrame([{
        'region': event['location']['region']['text'],
        'country': event['location']['country']['text'],
        'latitude': event['location']['latitude'],
        'longitude': event['location']['longitude']
    } for event in events])

    grouped = df.groupby('region').agg({'latitude': 'mean', 'longitude': 'mean'}).reset_index() \
        if filter == 'Region' else \
        df.groupby('country').agg({'latitude': 'mean', 'longitude': 'mean'}).reset_index() \
            if filter == 'Country' else {}
    grouped['type'] = filter

    return grouped.to_dict(orient='records')

def identify_shared_attack_strategies(filter: str):#14
    events = list(terrorism_collection.find(
        {},
        {
            'location.region.text': 1,
            'location.country.text': 1,
            'attack_type.primary.text': 1,
            'groups.name': 1,
            'location.latitude': 1,
            'location.longitude': 1
        }
    ))

    df = pd.DataFrame([{
        'region': event['location']['region']['text'],
        'country': event['location']['country']['text'],
        'attack_type': event['attack_type']['primary']['text'],
        'group': group['name'],
        'latitude': event['location'].get('latitude'),
        'longitude': event['location'].get('longitude')
    } for event in events for group in event['groups'] if 'attack_type' in event])

    df = df[df['group'] != 'Unknown']

    key_column = 'region' if filter == 'region' else 'country'

    grouped = (
        df.groupby([key_column, 'attack_type'])
        .agg({
            'group': lambda x: list(set(x)),
            'latitude': 'mean',
            'longitude': 'mean'
        })
        .reset_index()
    )

    grouped['group_count'] = grouped['group'].apply(len)

    max_attack_types = (
        grouped.loc[grouped.groupby(key_column)['group_count'].idxmax()]
        .reset_index(drop=True)
    )

    result = [
        {
            key_column: row[key_column],
            'attack_type': row['attack_type'],
            'groups': row['group'],
            'latitude': row['latitude'],
            'longitude': row['longitude']
        }
        for _, row in max_attack_types.iterrows()
    ]

    return result



def identify_target_preferences(): #15
    events = list(terrorism_collection.find(
        {},
        {
            'targets.type.text': 1,
            'groups.name': 1
        }
    ))
    df = pd.DataFrame([
        {
            'target_type': event['targets'][0]['type']['text'],
            'group': group['name']
        } for event in events for group in event['groups'] if 'targets' in event and event['targets']
    ])

    df = df[df['group'] != 'Unknown']

    grouped = df.groupby('target_type')['group'].apply(lambda x: list(set(x))).reset_index()

    grouped = grouped[grouped['group'].apply(lambda x: len(x) > 1)]

    grouped['group_count'] = grouped['group'].apply(lambda x: len(x))

    return grouped.to_dict(orient='records')

def identify_high_activity_regions(group_by='region'): #16
    events = list(terrorism_collection.find(
        {},
        {
            'location.region.text': 1,
            'location.country.text': 1,
            'groups.name': 1,
            'location.latitude': 1,
            'location.longitude': 1
        }
    ))

    df = pd.DataFrame([
        {
            'region': event['location']['region']['text'],
            'country': event['location']['country']['text'],
            'group': group['name'],
            'latitude': event['location']['latitude'],
            'longitude': event['location']['longitude']
        } for event in events for group in event['groups']
    ])

    df = df[df['group'] != 'Unknown']

    key_column = 'region' if group_by == 'region' else 'country'

    grouped = (
        df.groupby(key_column)
        .agg({
            'group': lambda x: list(set(x)),
            'latitude': 'mean',
            'longitude': 'mean'
        })
        .reset_index()
    )
    grouped['group_count'] = grouped['group'].apply(len)

    max_attack_types = (
        grouped.loc[grouped.groupby(key_column)['group_count'].idxmax()]
        .reset_index(drop=True)
    )

    result = [
        {
            key_column: row[key_column],
            'groups': row['group'],
            'latitude': row['latitude'],
            'longitude': row['longitude']
        }
        for _, row in max_attack_types.iterrows()
    ]

    return result

def identify_influential_groups(region_type='region'): #18
    events = list(terrorism_collection.find(
        {} ,
        {
            'location.region.text': 1,
            'location.country.text': 1,
            'attack_type.primary.text': 1,
            'groups.name': 1,
            'location.latitude': 1,
            'location.longitude': 1
        }
    ))

    df = pd.DataFrame([{
        'region': event['location']['region']['text'],
        'country': event['location']['country']['text'],
        'attack_type': event['attack_type']['primary']['text'],
        'group': group['name'],
        'latitude': event['location']['latitude'],
        'longitude': event['location']['longitude']
    } for event in events for group in event['groups']])

    df = df[df['group'] != 'Unknown']

    if region_type == 'region':
        grouped = df.groupby(['group', 'region', 'attack_type']).size().reset_index(name='count')
    elif region_type == 'country':
        grouped = df.groupby(['group', 'country', 'attack_type']).size().reset_index(name='count')
    else:
        grouped = {}

    if region_type == 'region':
        region_grouped = grouped.groupby(['region', 'group']).agg({
            'count': 'sum',
            'attack_type': 'nunique'
        }).reset_index()
    elif region_type == 'country':
        region_grouped = grouped.groupby(['country', 'group']).agg({
            'count': 'sum',
            'attack_type': 'nunique'
        }).reset_index()
    else:
        region_grouped = {}

    region_grouped['total_influence'] = region_grouped['count'] + region_grouped['attack_type']

    if region_type == 'region':
        highest_influence_per_region = region_grouped.loc[region_grouped.groupby('region')['total_influence'].idxmax()]
    elif region_type == 'country':
        highest_influence_per_region = region_grouped.loc[region_grouped.groupby('country')['total_influence'].idxmax()]
    else:
        highest_influence_per_region = {}

    if region_type == 'region':
        coords = df.groupby('region').agg({
            'latitude': 'mean',
            'longitude': 'mean'
        }).reset_index()
    elif region_type == 'country':
        coords = df.groupby('country').agg({
            'latitude': 'mean',
            'longitude': 'mean'
        }).reset_index()
    else:
        coords = {}

    result = pd.merge(highest_influence_per_region, coords, on=region_type, how='left')

    return result[[region_type, 'group', 'total_influence', 'latitude', 'longitude']].to_dict(orient='records')

