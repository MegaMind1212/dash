import logging
import os
import pandas as pd
import folium
import plotly.express as px
from werkzeug.utils import secure_filename
from datetime import datetime, timedelta

# Configure logging
logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(levelname)s - %(message)s', handlers=[
    logging.StreamHandler(),
    logging.FileHandler('app.log')
])
logger = logging.getLogger(__name__)

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
    try:
        df = pd.read_csv(file_path, dtype={'userid': str})
        df['pincode'] = df['pincode'].astype(str).str.extract(r'(\d{6})')
        df['city'] = df['locality'].fillna(df.get('state', 'Mumbai'))
        df['latitude'] = df['pincode'].map(lambda x: pincode_coords.get(x, [None, None])[0])
        df['longitude'] = df['pincode'].map(lambda x: pincode_coords.get(x, [None, None])[1])
        mask = df['latitude'].isna() | df['longitude'].isna()
        df.loc[mask, 'latitude'] = df.loc[mask, 'city'].map(lambda x: city_coords.get(x, [19.0760, 72.8777])[0])
        df.loc[mask, 'longitude'] = df.loc[mask, 'city'].map(lambda x: city_coords.get(x, [19.0760, 72.8777])[1])
        logger.debug(f"Users columns: {df.columns.tolist()}")
        return df
    except Exception as e:
        logger.error(f"Error processing users data: {e}")
        return pd.DataFrame()

def process_deals_full_data(file_path):
    try:
        df = pd.read_csv(file_path)
        df['pincode'] = df['user_pincode'].fillna('').astype(str).str.extract(r'(\d{6})')
        df['city'] = df['user_pincode'].fillna('').str.split(',').str[0].str.strip().replace('', 'Mumbai').fillna('Mumbai')
        df['latitude'] = df['pincode'].map(lambda x: pincode_coords.get(x, [None, None])[0] if pd.notna(x) else None)
        df['longitude'] = df['pincode'].map(lambda x: pincode_coords.get(x, [None, None])[1] if pd.notna(x) else None)
        mask = df['latitude'].isna() | df['longitude'].isna()
        df.loc[mask, 'latitude'] = df.loc[mask, 'city'].map(lambda x: city_coords.get(x, [19.0760, 72.8777])[0])
        df.loc[mask, 'longitude'] = df.loc[mask, 'city'].map(lambda x: city_coords.get(x, [19.0760, 72.8777])[1])
        df = df.dropna(subset=['latitude', 'longitude'])
        logger.debug(f"Deals Full columns: {df.columns.tolist()}")
        return df
    except Exception as e:
        logger.error(f"Error processing deals full data: {e}")
        return pd.DataFrame()

def process_deals_data(file_path):
    try:
        df = pd.read_csv(file_path)
        df['pincode'] = df['user_pincode'].str.extract(r'(\d{6})')
        df['city'] = df['user_pincode'].str.split(',').str[0].str.strip()
        df['latitude'] = df['pincode'].map(lambda x: pincode_coords.get(x, [None, None])[0] if pd.notna(x) else None)
        df['longitude'] = df['pincode'].map(lambda x: pincode_coords.get(x, [None, None])[1] if pd.notna(x) else None)
        mask = df['latitude'].isna() | df['longitude'].isna()
        df.loc[mask, 'latitude'] = df.loc[mask, 'city'].map(lambda x: city_coords.get(x, [19.0760, 72.8777])[0])
        df.loc[mask, 'longitude'] = df.loc[mask, 'city'].map(lambda x: city_coords.get(x, [19.0760, 72.8777])[1])
        df = df.dropna(subset=['latitude', 'longitude'])
        logger.debug(f"Deals columns: {df.columns.tolist()}")
        return df
    except Exception as e:
        logger.error(f"Error processing deals data: {e}")
        return pd.DataFrame()

def process_dealers_data(file_path):
    try:
        df = pd.read_csv(file_path)
        df = df.rename(columns={'lat': 'latitude', 'long': 'longitude'})
        df['latitude'] = pd.to_numeric(df['latitude'], errors='coerce')
        df['longitude'] = pd.to_numeric(df['longitude'], errors='coerce')
        df['pincode'] = df['pincode'].astype(str)
        df = df.dropna(subset=['latitude', 'longitude'])
        logger.debug(f"Dealers columns: {df.columns.tolist()}")
        return df
    except Exception as e:
        logger.error(f"Error processing dealers data: {e}")
        return pd.DataFrame()

def create_users_map(deals_df):
    mumbai_map = folium.Map(location=[19.0760, 72.8777], zoom_start=11, tiles="cartodbpositron")
    grouped = deals_df.groupby('pincode')
    for pincode, group in grouped:
        unique_users = group['user_name'].drop_duplicates().tolist()
        user_counts = group['user_name'].value_counts()
        user_display = "<br>".join([f"{user} ({count})" for user, count in user_counts.items()])
        lat, lon = pincode_coords.get(pincode, [group['latitude'].iloc[0], group['longitude'].iloc[0]])
        folium.CircleMarker(
            location=[lat, lon],
            radius=min(5 + len(unique_users) * 2, 20),
            popup=f"Pincode: {pincode}<br>Users ({len(unique_users)}):<br>{user_display}",
            color="#1f77b4",
            fill=True,
            fill_opacity=0.7
        ).add_to(mumbai_map)
    return mumbai_map._repr_html_()

def create_dealers_map(dealers_df):
    dealers_map = folium.Map(location=[19.0760, 72.8777], zoom_start=11, tiles="cartodbpositron")
    for _, row in dealers_df.iterrows():
        img_links = row['Imgurl'] if pd.notna(row['Imgurl']) else "No images available"
        img_html = "<br>".join([f"<a href='{url.strip()}' target='_blank'>{url.strip() if url.strip() else 'Invalid link'}</a>" 
                               for url in img_links.split(' | ')]) if isinstance(img_links, str) else "No images available"
        popup_content = (
            f"Dealer: {row.get('coname', 'Unknown')}<br>"
            f"Phone: {row['phone_no']}<br>"
            f"Address: {row['addr1']}, {row['addr2']}, {row['landmark']}, {row['city']}, {row['pincode']}<br>"
            f"Categories: {row['cat_disp_names']}<br>"
            f"Subcategories: {row['subcat_disp_names']}<br>"
            f"Images: <br>{img_html}"
        )
        folium.CircleMarker(
            location=[row['latitude'], row['longitude']],
            radius=5,
            popup=popup_content,
            color="#ff7f0e",
            fill=True,
            fill_opacity=0.7
        ).add_to(dealers_map)
    return dealers_map._repr_html_()

def create_relational_map(deals_df, dealers_df):
    relation_map = folium.Map(location=[19.0760, 72.8777], zoom_start=11, tiles="cartodbpositron")
    grouped_users = deals_df.groupby('pincode')
    user_locations = {}
    for pincode, group in grouped_users:
        unique_users = group['user_name'].drop_duplicates().tolist()
        user_counts = group['user_name'].value_counts()
        user_display = "<br>".join([f"{user} ({count})" for user, count in user_counts.items()])
        lat, lon = pincode_coords.get(pincode, [group['latitude'].iloc[0], group['longitude'].iloc[0]])
        folium.CircleMarker(
            location=[lat, lon],
            radius=min(5 + len(unique_users) * 2, 20),
            popup=f"Pincode: {pincode}<br>Users ({len(unique_users)}):<br>{user_display}",
            color="#1f77b4",
            fill=True,
            fill_opacity=0.7
        ).add_to(relation_map)
        user_locations[pincode] = (lat, lon)
    
    dealer_locations = {}
    for _, row in dealers_df.iterrows():
        folium.CircleMarker(
            location=[row['latitude'], row['longitude']],
            radius=5,
            popup=f"Dealer: {row.get('coname', 'Unknown')}<br>Pincode: {row['pincode']}",
            color="#ff7f0e",
            fill=True,
            fill_opacity=0.7
        ).add_to(relation_map)
        dealer_locations[row['pincode']] = (row['latitude'], row['longitude'])
    
    for _, deal in deals_df.iterrows():
        user_pincode = deal['pincode']
        if user_pincode in user_locations:
            if 'dealerinfo.coname' in deal and pd.notna(deal['dealerinfo.coname']):
                matching_dealers = dealers_df[dealers_df['coname'] == deal['dealerinfo.coname']]
                if not matching_dealers.empty:
                    dealer_pincode = matching_dealers.iloc[0]['pincode']
                    if dealer_pincode in dealer_locations:
                        folium.PolyLine(
                            locations=[user_locations[user_pincode], dealer_locations[dealer_pincode]],
                            color="grey",
                            weight=1,
                            opacity=0.5
                        ).add_to(relation_map)
            elif 'dealerinfo.dealer_id' in deal and pd.notna(deal['dealerinfo.dealer_id']):
                matching_dealers = dealers_df[dealers_df['_id'] == deal['dealerinfo.dealer_id']]
                if not matching_dealers.empty:
                    dealer_pincode = matching_dealers.iloc[0]['pincode']
                    if dealer_pincode in dealer_locations:
                        folium.PolyLine(
                            locations=[user_locations[user_pincode], dealer_locations[dealer_pincode]],
                            color="grey",
                            weight=1,
                            opacity=0.5
                        ).add_to(relation_map)
            elif user_pincode in dealer_locations:
                folium.PolyLine(
                    locations=[user_locations[user_pincode], dealer_locations[user_pincode]],
                    color="grey",
                    weight=1,
                    opacity=0.5
                ).add_to(relation_map)
    
    return relation_map._repr_html_()

def create_new_users_map(users_df):
    if users_df.empty:
        logger.warning("No users found in the dataset.")
        return None
    current_date = datetime(2025, 4, 15)
    thirty_days_ago = current_date - timedelta(days=30)
    new_users_df = users_df[users_df['createEpoch'] >= int(thirty_days_ago.timestamp())].copy()
    if new_users_df.empty:
        logger.warning("No new users found in the last 30 days.")
        return None
    new_users_df['name'] = new_users_df['name'].astype(str).fillna('Unknown')
    new_users_df['phone'] = new_users_df['phone'].astype(str).fillna('N/A')
    unique_users_per_pincode = new_users_df.groupby('pincode').agg({
        'userid': 'nunique',
        'latitude': 'first',
        'longitude': 'first',
        'name': lambda x: ', '.join(x.unique().astype(str)),
        'phone': lambda x: ', '.join(x.unique().astype(str))
    }).reset_index()
    new_users_map = folium.Map(location=[19.0760, 72.8777], zoom_start=10, tiles="cartodbpositron")
    total_pincode_count = len(unique_users_per_pincode)
    folium.Marker(
        location=[19.0760, 72.8777 - 0.2],
        icon=folium.DivIcon(icon_size=(150, 36), icon_anchor=(0, 0),
                            html=f'<div style="font-size: 12pt; color: white; background: rgba(0, 0, 0, 0.7); padding: 5px; border-radius: 3px;">Total Pincodes: {total_pincode_count}</div>'),
    ).add_to(new_users_map)
    for _, row in unique_users_per_pincode.iterrows():
        lat = row['latitude'] if pd.notna(row['latitude']) else 19.0760
        lon = row['longitude'] if pd.notna(row['longitude']) else 72.8777
        user_count = row['userid']
        popup_content = (
            f"Pincode: {row['pincode']}<br>"
            f"Users: {user_count}<br>"
            f"Names: {row['name']}<br>"
            f"Phones: {row['phone']}"
        )
        folium.CircleMarker(
            location=[lat, lon],
            radius=5 + user_count * 0.5,
            popup=popup_content,
            color="#17becf",
            fill=True,
            fill_opacity=0.7
        ).add_to(new_users_map)
    return new_users_map._repr_html_()

def create_graphs(deals_df, dealers_df, users_df):
    try:
        if deals_df.empty or dealers_df.empty or users_df.empty:
            logger.warning("One or more DataFrames are empty in create_graphs.")
            return {'data': [], 'layout': {}}, {'data': [], 'layout': {}}, {'data': [], 'layout': {}}, {'data': [], 'layout': {}}, {'data': [], 'layout': {}}

        required_cols_deals = ['pincode', 'user_id', 'req_qty']
        required_cols_dealers = ['pincode', 'coname', 'cat_disp_names']
        required_cols_users = ['userid', 'createEpoch', 'pincode']
        if not all(col in deals_df.columns for col in required_cols_deals) or \
           not all(col in dealers_df.columns for col in required_cols_dealers) or \
           not all(col in users_df.columns for col in required_cols_users):
            logger.error(f"Missing required columns. Deals: {deals_df.columns.tolist()}, Dealers: {dealers_df.columns.tolist()}, Users: {users_df.columns.tolist()}")
            return {'data': [], 'layout': {}}, {'data': [], 'layout': {}}, {'data': [], 'layout': {}}, {'data': [], 'layout': {}}, {'data': [], 'layout': {}}

        users_per_pin = deals_df.groupby('pincode').size().reset_index(name='users')
        dealers_per_pin = dealers_df.groupby('pincode').size().reset_index(name='dealers')
        dealers_df_exp = dealers_df.assign(cat_disp_names=dealers_df['cat_disp_names'].str.split(r' \| ')).explode('cat_disp_names')
        current_date = datetime(2025, 4, 15)
        thirty_days_ago = current_date - timedelta(days=30)
        new_users = users_df[users_df['createEpoch'] >= int(thirty_days_ago.timestamp())].copy()
        new_users['onboarding_date'] = new_users['createEpoch'].apply(lambda x: datetime.fromtimestamp(x).strftime('%Y-%m-%d'))

        fig1 = px.bar(users_per_pin, x='pincode', y='users', title="Users per Pincode", 
                      color_discrete_sequence=['#1f77b4'], template='plotly_dark')
        fig1.update_layout(autosize=True, height=300, margin=dict(l=40, r=20, t=40, b=60), 
                           xaxis={'tickangle': 45, 'tickmode': 'auto', 'automargin': True}, yaxis={'automargin': True})

        fig2 = px.bar(dealers_per_pin, x='pincode', y='dealers', title="Dealers per Pincode", 
                      color_discrete_sequence=['#ff7f0e'], template='plotly_dark')
        fig2.update_layout(autosize=True, height=300, margin=dict(l=40, r=20, t=40, b=60), 
                           xaxis={'tickangle': 45, 'tickmode': 'auto', 'automargin': True}, yaxis={'automargin': True})

        fig3 = px.bar(deals_df, x='user_id', y='req_qty', title="Deal Requests per User", 
                      color='pincode', template='plotly_dark')
        fig3.update_layout(autosize=True, height=300, margin=dict(l=40, r=20, t=40, b=60), 
                           xaxis={'tickangle': 45, 'tickmode': 'auto', 'automargin': True}, yaxis={'automargin': True})

        fig4 = px.bar(dealers_df_exp, x='coname' if 'coname' in dealers_df_exp.columns else '_id', y='cat_disp_names', 
                      title="Dealer Product Categories", color='pincode', 
                      hover_data=['phone_no', 'addr1', 'addr2', 'landmark', 'city', 'subcat_disp_names', 'Imgurl'], 
                      template='plotly_dark')
        fig4.update_layout(autosize=True, height=300, margin=dict(l=40, r=20, t=40, b=60), 
                           xaxis={'tickangle': 45, 'tickmode': 'auto', 'automargin': True}, yaxis={'automargin': True})

        fig5 = px.scatter(new_users, x='userid', y='onboarding_date', title="New Users Onboarding Timeline", 
                          color='pincode', hover_data=['name', 'phone', 'pincode'], template='plotly_dark')
        fig5.update_layout(autosize=True, height=300, margin=dict(l=40, r=20, t=40, b=60), 
                           xaxis={'tickangle': 0, 'automargin': True}, 
                           yaxis={'type': 'category', 'title': 'Onboarding Date', 'automargin': True})

        return fig1.to_json(), fig2.to_json(), fig3.to_json(), fig4.to_json(), fig5.to_json()
    except Exception as e:
        logger.error(f"Error creating graphs: {e}")
        return {'data': [], 'layout': {}}, {'data': [], 'layout': {}}, {'data': [], 'layout': {}}, {'data': [], 'layout': {}}, {'data': [], 'layout': {}}

def filter_deals_by_date(deals_df, deals_full_df, date_str):
    try:
        target_date = pd.to_datetime(date_str).date()
        deals_df['deal_date'] = pd.to_datetime(deals_df.get('created_at', deals_df.get('deal_date', pd.Series([pd.NaT] * len(deals_df))))).dt.date
        deals_full_df['deal_date'] = pd.to_datetime(deals_full_df.get('created_at', deals_full_df.get('deal_date', pd.Series([pd.NaT] * len(deals_full_df))))).dt.date
        filtered_deals = pd.concat([
            deals_df[deals_df['deal_date'] == target_date],
            deals_full_df[deals_full_df['deal_date'] == target_date]
        ])
        return filtered_deals[['user_id', 'user_name', 'pincode', 'req_qty', 'deal_date']].to_dict('records')
    except ValueError as e:
        logger.error(f"Error filtering deals by date: {e}")
        return []

def filter_dealers_by_category(dealers_df, category):
    if not category:
        return []
    filtered_dealers = dealers_df[dealers_df['cat_disp_names'].str.contains(category, case=False, na=False)]
    return filtered_dealers[['coname', 'pincode', 'phone_no', 'cat_disp_names', 'subcat_disp_names']].to_dict('records')

def filter_dealers_by_pincode(dealers_df, pincode):
    if not pincode:
        return []
    filtered_dealers = dealers_df[dealers_df['pincode'] == pincode]
    return filtered_dealers[['coname', 'pincode', 'phone_no', 'cat_disp_names', 'subcat_disp_names']].to_dict('records')

def filter_deals_by_pincode(deals_df, deals_full_df, dealers_df, pincode):
    if not pincode:
        return []
    deals_df['deal_date'] = pd.to_datetime(deals_df.get('created_at', deals_df.get('deal_date', pd.Series([pd.NaT] * len(deals_df))))).dt.date
    deals_full_df['deal_date'] = pd.to_datetime(deals_full_df.get('created_at', deals_full_df.get('deal_date', pd.Series([pd.NaT] * len(deals_full_df))))).dt.date
    filtered_deals = pd.concat([
        deals_df[deals_df['pincode'] == pincode],
        deals_full_df[deals_full_df['pincode'] == pincode]
    ])
    results = []
    for _, deal in filtered_deals.iterrows():
        dealer_name = deal.get('dealerinfo.coname', '')
        if not dealer_name:
            dealer_id = deal.get('dealerinfo.dealer_id', '')
            matching_dealer = dealers_df[dealers_df['_id'] == dealer_id]
            dealer_name = matching_dealer['coname'].iloc[0] if not matching_dealer.empty else 'Unknown'
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
    try:
        deals_df = process_deals_data(deals_path)
        dealers_df = process_dealers_data(dealers_path)
        users_df = process_users_data(users_path)
        deals_full_df = process_deals_full_data(deals_full_path)
        if deals_df.empty or dealers_df.empty or users_df.empty or deals_full_df.empty:
            error_msg = f"Error: No valid data found. Dealers: {len(dealers_df)}, Deals: {len(deals_df)}, Users: {len(users_df)}, Deals Full: {len(deals_full_df)}"
            logger.error(error_msg)
            return None, error_msg
        users_map = create_users_map(deals_df)
        dealers_map = create_dealers_map(dealers_df)
        relation_map = create_relational_map(deals_df, dealers_df)
        new_users_map = create_new_users_map(users_df)
        graph1, graph2, graph3, graph4, graph5 = create_graphs(deals_df, dealers_df, users_df)
        if not all([graph1, graph2, graph3, graph4, graph5]):
            error_msg = "Error: Failed to create one or more graphs."
            logger.error(error_msg)
            return None, error_msg
        total_users = len(users_df)
        total_visits = len(deals_df) + len(deals_full_df)
        current_date = datetime(2025, 4, 15)
        thirty_days_ago = current_date - timedelta(days=30)
        new_users = len(users_df[users_df['createEpoch'] >= int(thirty_days_ago.timestamp())])
        active_users = len(deals_df['user_id'].drop_duplicates())
        total_deals = len(deals_df) + len(deals_full_df)
        unique_deals = len(pd.concat([deals_df, deals_full_df]).drop_duplicates(subset=['user_id', 'req_qty']))
        new_user_deal_ratio = (unique_deals / new_users) * 100 if new_users > 0 else 0
        unique_deals_with_response = len(deals_df[deals_df['req_qty'] > 0].drop_duplicates(subset=['user_id', 'req_qty']))
        response_ratio = (unique_deals_with_response / unique_deals) * 100 if unique_deals > 0 else 0
        return {
            'users_map': users_map,
            'dealers_map': dealers_map,
            'relation_map': relation_map,
            'new_users_map': new_users_map,
            'graph1': graph1,
            'graph2': graph2,
            'graph3': graph3,
            'graph4': graph4,
            'graph5': graph5,
            'total_users': total_users,
            'total_visits': total_visits,
            'new_users': new_users,
            'active_users': active_users,
            'total_deals': total_deals,
            'unique_deals': unique_deals,
            'new_user_deal_ratio': new_user_deal_ratio,
            'response_ratio': response_ratio,
            'file_paths': {
                'deals': deals_path,
                'dealers': dealers_path,
                'users': users_path,
                'deals_full': deals_full_path
            },
            'deals_df': deals_df,
            'dealers_df': dealers_df,
            'users_df': users_df,
            'deals_full_df': deals_full_df
        }, None
    except Exception as e:
        error_msg = f"Error performing analysis: {e}"
        logger.error(error_msg)
        return None, error_msg