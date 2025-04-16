import os
import pandas as pd
import folium
import plotly.express as px
from werkzeug.utils import secure_filename
from datetime import datetime, timedelta

pincode_coords = {
    "400078": [19.1011, 72.8376], "410206": [19.0330, 73.0297], "401105": [19.3000, 72.8500],
    "360001": [22.3039, 70.8022], "421301": [19.2333, 73.1333], "400003": [18.9500, 72.8333],
    "401303": [19.7000, 72.7667], "400705": [19.0330, 73.0150], "421204": [19.2167, 73.1500],
    "400072": [19.1667, 72.8333], "400092": [19.1167, 72.9167], "400104": [19.1667, 72.8667],
    "400607": [19.2167, 72.9667], "400089": [19.1333, 72.8167], "400701": [19.0330, 73.0667],
    "400602": [19.2167, 72.9833], "401101": [19.3000, 72.8667], "400065": [19.0667, 72.8833],
    "400601": [19.1950, 72.9770], "400706": [19.0330, 73.0297], "400101": [19.0760, 72.8777],
    "421202": [19.2403, 73.1305], "833214": [22.8000, 85.3333]
}

city_coords = {
    "Mumbai": [19.0760, 72.8777], "Thane": [19.2183, 72.9781], "Navi Mumbai": [19.0330, 73.0297],
    "Rajkot": [22.3039, 70.8022], "Kalyan": [19.2403, 73.1305], "Dombivli": [19.2133, 73.0833],
    "Mira Bhayandar": [19.2952, 72.8544], "Virar": [19.4657, 72.8114], "Adai": [19.0330, 73.0297],
    "Vashi": [19.0771, 72.9986], "Sion": [19.0400, 72.8600], "Ambdiha": [22.8000, 85.3333]
}

def get_next_set_number():
    set_folders = [f for f in os.listdir('/tmp/Uploads') if f.startswith('set_')]
    return 1 if not set_folders else max(int(f.split('_')[1]) for f in set_folders) + 1

def secure_filename(filename):
    return secure_filename(filename)

def process_users_data(file_path):
    df = pd.read_csv(file_path, dtype={'userid': str}, usecols=['userid', 'pincode', 'locality', 'state', 'createEpoch'])
    df['pincode'] = df['pincode'].astype(str).str.extract(r'(\d{6})')
    df['city'] = df['locality'].fillna(df['state'].fillna('Mumbai'))
    df['latitude'] = df['pincode'].map(lambda x: pincode_coords.get(x, [None, None])[0])
    df['longitude'] = df['pincode'].map(lambda x: pincode_coords.get(x, [None, None])[1])
    mask = df['latitude'].isna() | df['longitude'].isna()
    df.loc[mask, 'latitude'] = df.loc[mask, 'city'].map(lambda x: city_coords.get(x, [19.0760, 72.8777])[0])
    df.loc[mask, 'longitude'] = df.loc[mask, 'city'].map(lambda x: city_coords.get(x, [19.0760, 72.8777])[1])
    return df.dropna(subset=['latitude', 'longitude'])

def process_deals_full_data(file_path):
    df = pd.read_csv(file_path, usecols=['user_id', 'user_pincode', 'req_qty', 'created_at'])
    df['pincode'] = df['user_pincode'].fillna('').astype(str).str.extract(r'(\d{6})')
    df['city'] = df['user_pincode'].fillna('').str.split(',').str[0].str.strip().replace('', 'Mumbai')
    df['latitude'] = df['pincode'].map(lambda x: pincode_coords.get(x, [None, None])[0] if pd.notna(x) else None)
    df['longitude'] = df['pincode'].map(lambda x: pincode_coords.get(x, [None, None])[1] if pd.notna(x) else None)
    mask = df['latitude'].isna() | df['longitude'].isna()
    df.loc[mask, 'latitude'] = df.loc[mask, 'city'].map(lambda x: city_coords.get(x, [19.0760, 72.8777])[0])
    df.loc[mask, 'longitude'] = df.loc[mask, 'city'].map(lambda x: city_coords.get(x, [19.0760, 72.8777])[1])
    return df.dropna(subset=['latitude', 'longitude'])

def process_deals_data(file_path):
    df = pd.read_csv(file_path, usecols=['user_id', 'user_pincode', 'req_qty', 'created_at', 'dealerinfo.coname', 'dealerinfo.dealer_id'])
    df['pincode'] = df['user_pincode'].str.extract(r'(\d{6})')
    df['city'] = df['user_pincode'].str.split(',').str[0].str.strip()
    df['latitude'] = df['pincode'].map(lambda x: pincode_coords.get(x, [None, None])[0] if pd.notna(x) else None)
    df['longitude'] = df['pincode'].map(lambda x: pincode_coords.get(x, [None, None])[1] if pd.notna(x) else None)
    mask = df['latitude'].isna() | df['longitude'].isna()
    df.loc[mask, 'latitude'] = df.loc[mask, 'city'].map(lambda x: city_coords.get(x, [19.0760, 72.8777])[0])
    df.loc[mask, 'longitude'] = df.loc[mask, 'city'].map(lambda x: city_coords.get(x, [19.0760, 72.8777])[1])
    return df.dropna(subset=['latitude', 'longitude'])

def process_dealers_data(file_path):
    df = pd.read_csv(file_path, usecols=['_id', 'coname', 'pincode', 'phone_no', 'cat_disp_names', 'subcat_disp_names', 'lat', 'long'])
    df = df.rename(columns={'lat': 'latitude', 'long': 'longitude'})
    df['latitude'] = pd.to_numeric(df['latitude'], errors='coerce')
    df['longitude'] = pd.to_numeric(df['longitude'], errors='coerce')
    df['pincode'] = df['pincode'].astype(str)
    return df.dropna(subset=['latitude', 'longitude'])

def create_users_map(deals_df):
    mumbai_map = folium.Map(location=[19.0760, 72.8777], zoom_start=11, tiles="cartodbpositron")
    for pincode, group in deals_df.groupby('pincode'):
        unique_users = group['user_id'].drop_duplicates().count()
        lat, lon = pincode_coords.get(pincode, [group['latitude'].iloc[0], group['longitude'].iloc[0]])
        folium.CircleMarker(location=[lat, lon], radius=min(5 + unique_users * 2, 20), popup=f"Pincode: {pincode}<br>Users: {unique_users}", color="#1f77b4", fill=True, fill_opacity=0.7).add_to(mumbai_map)
    return mumbai_map._repr_html_()

def create_dealers_map(dealers_df):
    dealers_map = folium.Map(location=[19.0760, 72.8777], zoom_start=11, tiles="cartodbpositron")
    for _, row in dealers_df.iterrows():
        popup_content = f"Dealer: {row['coname']}<br>Pincode: {row['pincode']}"
        folium.CircleMarker(location=[row['latitude'], row['longitude']], radius=5, popup=popup_content, color="#ff7f0e", fill=True, fill_opacity=0.7).add_to(dealers_map)
    return dealers_map._repr_html_()

def create_relational_map(deals_df, dealers_df):
    relation_map = folium.Map(location=[19.0760, 72.8777], zoom_start=11, tiles="cartodbpositron")
    user_locs = {pincode: (lat, lon) for pincode, (lat, lon) in zip(deals_df['pincode'], zip(deals_df['latitude'], deals_df['longitude']))}
    dealer_locs = {pincode: (lat, lon) for pincode, (lat, lon) in zip(dealers_df['pincode'], zip(dealers_df['latitude'], dealers_df['longitude']))}
    for _, deal in deals_df.iterrows():
        user_pin = deal['pincode']
        if user_pin in user_locs:
            dealer_name = deal.get('dealerinfo.coname', '')
            if not dealer_name and 'dealerinfo.dealer_id' in deal:
                dealer_match = dealers_df[dealers_df['_id'] == deal['dealerinfo.dealer_id']]
                dealer_name = dealer_match['coname'].iloc[0] if not dealer_match.empty else ''
            if dealer_name:
                dealer_pin = dealers_df[dealers_df['coname'] == dealer_name]['pincode'].iloc[0] if dealer_name in dealers_df['coname'].values else user_pin
                if dealer_pin in dealer_locs:
                    folium.PolyLine(locations=[user_locs[user_pin], dealer_locs[dealer_pin]], color="grey", weight=1, opacity=0.5).add_to(relation_map)
            folium.CircleMarker(location=user_locs[user_pin], radius=5, color="#1f77b4", fill=True, fill_opacity=0.7).add_to(relation_map)
    for pincode, (lat, lon) in dealer_locs.items():
        folium.CircleMarker(location=[lat, lon], radius=5, popup=f"Pincode: {pincode}", color="#ff7f0e", fill=True, fill_opacity=0.7).add_to(relation_map)
    return relation_map._repr_html_()

def create_new_users_map(users_df):
    if users_df.empty:
        return None
    current_date = datetime(2025, 4, 15)
    thirty_days_ago = current_date - timedelta(days=30)
    new_users_df = users_df[users_df['createEpoch'] >= int(thirty_days_ago.timestamp())].copy()
    if new_users_df.empty:
        return None
    new_users_map = folium.Map(location=[19.0760, 72.8777], zoom_start=10, tiles="cartodbpositron")
    total_pincode_count = new_users_df['pincode'].nunique()
    folium.Marker(location=[19.0760, 72.8777 - 0.2], icon=folium.DivIcon(icon_size=(150, 36), icon_anchor=(0, 0), html=f'<div style="font-size:12pt;color:white;background:rgba(0,0,0,0.7);padding:5px;border-radius:3px;">Total Pincodes: {total_pincode_count}</div>')).add_to(new_users_map)
    for pincode, group in new_users_df.groupby('pincode'):
        lat, lon = pincode_coords.get(pincode, [group['latitude'].iloc[0], group['longitude'].iloc[0]])
        user_count = group['userid'].nunique()
        folium.CircleMarker(location=[lat, lon], radius=5 + user_count * 0.5, popup=f"Pincode: {pincode}<br>Users: {user_count}", color="#17becf", fill=True, fill_opacity=0.7).add_to(new_users_map)
    return new_users_map._repr_html_()

def create_graphs(deals_df, dealers_df, users_df):
    if deals_df.empty or dealers_df.empty or users_df.empty:
        return ({'data': [], 'layout': {}},) * 5
    fig1 = px.bar(deals_df.groupby('pincode').size().reset_index(name='users'), x='pincode', y='users', title="Users per Pincode", color_discrete_sequence=['#1f77b4'], template='plotly_dark')
    fig2 = px.bar(dealers_df.groupby('pincode').size().reset_index(name='dealers'), x='pincode', y='dealers', title="Dealers per Pincode", color_discrete_sequence=['#ff7f0e'], template='plotly_dark')
    fig3 = px.bar(deals_df, x='user_id', y='req_qty', title="Deal Requests per User", color='pincode', template='plotly_dark')
    fig4 = px.bar(dealers_df.assign(cat_disp_names=dealers_df['cat_disp_names'].str.split(r' \| ')).explode('cat_disp_names'), x='coname', y='cat_disp_names', title="Dealer Product Categories", color='pincode', template='plotly_dark')
    current_date = datetime(2025, 4, 15)
    thirty_days_ago = current_date - timedelta(days=30)
    new_users = users_df[users_df['createEpoch'] >= int(thirty_days_ago.timestamp())].copy()
    new_users['onboarding_date'] = pd.to_datetime(new_users['createEpoch'], unit='s').dt.strftime('%Y-%m-%d')
    fig5 = px.scatter(new_users, x='userid', y='onboarding_date', title="New Users Onboarding Timeline", color='pincode', template='plotly_dark')
    for fig in [fig1, fig2, fig3, fig4, fig5]:
        fig.update_layout(autosize=True, height=300, margin=dict(l=40, r=20, t=40, b=60), xaxis=dict(tickangle=45, automargin=True), yaxis=dict(automargin=True))
    return tuple(fig.to_json() for fig in [fig1, fig2, fig3, fig4, fig5])

def filter_deals_by_date(deals_df, deals_full_df, date_str):
    target_date = pd.to_datetime(date_str).date()
    combined_df = pd.concat([deals_df, deals_full_df])
    combined_df['deal_date'] = pd.to_datetime(combined_df['created_at'], errors='coerce').dt.date
    filtered = combined_df[combined_df['deal_date'] == target_date][['user_id', 'user_name', 'pincode', 'req_qty', 'deal_date']].dropna()
    return filtered.to_dict('records') if not filtered.empty else []

def filter_dealers_by_category(dealers_df, category):
    if not category:
        return []
    return dealers_df[dealers_df['cat_disp_names'].str.contains(category, case=False, na=False)][['coname', 'pincode', 'phone_no', 'cat_disp_names', 'subcat_disp_names']].to_dict('records')

def filter_dealers_by_pincode(dealers_df, pincode):
    if not pincode:
        return []
    return dealers_df[dealers_df['pincode'] == pincode][['coname', 'pincode', 'phone_no', 'cat_disp_names', 'subcat_disp_names']].to_dict('records')

def filter_deals_by_pincode(deals_df, deals_full_df, dealers_df, pincode):
    if not pincode:
        return []
    combined_df = pd.concat([deals_df, deals_full_df])
    combined_df['deal_date'] = pd.to_datetime(combined_df['created_at'], errors='coerce').dt.date
    filtered = combined_df[combined_df['pincode'] == pincode]
    results = []
    for _, deal in filtered.iterrows():
        dealer_name = deal.get('dealerinfo.coname', '')
        if not dealer_name and 'dealerinfo.dealer_id' in deal:
            dealer_match = dealers_df[dealers_df['_id'] == deal['dealerinfo.dealer_id']]
            dealer_name = dealer_match['coname'].iloc[0] if not dealer_match.empty else 'Unknown'
        results.append({
            'user_id': deal['user_id'],
            'user_name': deal.get('user_name', 'Unknown'),
            'dealer_name': dealer_name,
            'pincode': deal['pincode'],
            'req_qty': deal['req_qty'],
            'deal_date': deal['deal_date'].strftime('%Y-%m-%d') if pd.notna(deal['deal_date']) else 'N/A'
        })
    return results

def perform_analysis(deals_path, dealers_path, users_path, deals_full_path):
    deals_df = process_deals_data(deals_path)
    dealers_df = process_dealers_data(dealers_path)
    users_df = process_users_data(users_path)
    deals_full_df = process_deals_full_data(deals_full_path)
    if any(df.empty for df in [deals_df, dealers_df, users_df, deals_full_df]):
        return None, "Error: No valid data found in one or more files."
    users_map = create_users_map(deals_df)
    dealers_map = create_dealers_map(dealers_df)
    relation_map = create_relational_map(deals_df, dealers_df)
    new_users_map = create_new_users_map(users_df)
    graph1, graph2, graph3, graph4, graph5 = create_graphs(deals_df, dealers_df, users_df)
    total_users = users_df['userid'].nunique()
    total_visits = len(deals_df) + len(deals_full_df)
    current_date = datetime(2025, 4, 15)
    thirty_days_ago = current_date - timedelta(days=30)
    new_users = (users_df[users_df['createEpoch'] >= int(thirty_days_ago.timestamp())]['userid'].nunique())
    active_users = deals_df['user_id'].drop_duplicates().count()
    total_deals = len(deals_df) + len(deals_full_df)
    unique_deals = pd.concat([deals_df, deals_full_df]).drop_duplicates(subset=['user_id', 'req_qty']).shape[0]
    new_user_deal_ratio = (unique_deals / new_users * 100) if new_users > 0 else 0
    unique_deals_with_response = deals_df[deals_df['req_qty'] > 0].drop_duplicates(subset=['user_id', 'req_qty']).shape[0]
    response_ratio = (unique_deals_with_response / unique_deals * 100) if unique_deals > 0 else 0
    return {
        'users_map': users_map,
        'dealers_map': dealers_map,
        'relation_map': relation_map,
        'new_users_map': new_users_map,
        'graph1': graph1, 'graph2': graph2, 'graph3': graph3, 'graph4': graph4, 'graph5': graph5,
        'total_users': total_users, 'total_visits': total_visits, 'new_users': new_users,
        'active_users': active_users, 'total_deals': total_deals, 'unique_deals': unique_deals,
        'new_user_deal_ratio': new_user_deal_ratio, 'response_ratio': response_ratio,
        'deals_df': deals_df, 'dealers_df': dealers_df, 'users_df': users_df, 'deals_full_df': deals_full_df
    }, None