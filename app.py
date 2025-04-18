from flask import Flask, render_template_string, request, session
from flask_session import Session
import folium
import pandas as pd
import plotly.express as px
import json
import os
from werkzeug.utils import secure_filename
from datetime import datetime, timedelta

app = Flask(__name__)
app.config['SECRET_KEY'] = 'your-secret-key'
app.config['SESSION_TYPE'] = 'filesystem'
app.config['UPLOAD_FOLDER'] = '/tmp/Uploads'
Session(app)

os.makedirs(app.config['UPLOAD_FOLDER'], exist_ok=True)

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
    set_folders = [f for f in os.listdir(app.config['UPLOAD_FOLDER']) if f.startswith('set_')]
    return 1 if not set_folders else max(int(f.split('_')[1]) for f in set_folders) + 1

def process_users_data(file_path):
    df = pd.read_csv(file_path, dtype={'userid': str})
    df['pincode'] = df['pincode'].astype(str).str.extract(r'(\d{6})')
    df['city'] = df['locality'].fillna(df.get('state', 'Mumbai'))
    df['latitude'] = df['pincode'].map(lambda x: pincode_coords.get(x, [None, None])[0])
    df['longitude'] = df['pincode'].map(lambda x: pincode_coords.get(x, [None, None])[1])
    mask = df['latitude'].isna() | df['longitude'].isna()
    df.loc[mask, 'latitude'] = df.loc[mask, 'city'].map(lambda x: city_coords.get(x, [19.0760, 72.8777])[0])
    df.loc[mask, 'longitude'] = df.loc[mask, 'city'].map(lambda x: city_coords.get(x, [19.0760, 72.8777])[1])
    print(f"Users columns: {df.columns.tolist()}")
    return df

def process_deals_full_data(file_path):
    df = pd.read_csv(file_path)
    df['pincode'] = df['user_pincode'].fillna('').astype(str).str.extract(r'(\d{6})')
    df['city'] = df['user_pincode'].fillna('').str.split(',').str[0].str.strip().replace('', 'Mumbai').fillna('Mumbai')
    df['latitude'] = df['pincode'].map(lambda x: pincode_coords.get(x, [None, None])[0] if pd.notna(x) else None)
    df['longitude'] = df['pincode'].map(lambda x: pincode_coords.get(x, [None, None])[1] if pd.notna(x) else None)
    mask = df['latitude'].isna() | df['longitude'].isna()
    df.loc[mask, 'latitude'] = df.loc[mask, 'city'].map(lambda x: city_coords.get(x, [19.0760, 72.8777])[0])
    df.loc[mask, 'longitude'] = df.loc[mask, 'city'].map(lambda x: city_coords.get(x, [19.0760, 72.8777])[1])
    df = df.dropna(subset=['latitude', 'longitude'])
    print(f"Deals Full columns: {df.columns.tolist()}")
    return df

def process_deals_data(file_path):
    df = pd.read_csv(file_path)
    df['pincode'] = df['user_pincode'].str.extract(r'(\d{6})')
    df['city'] = df['user_pincode'].str.split(',').str[0].str.strip()
    df['latitude'] = df['pincode'].map(lambda x: pincode_coords.get(x, [None, None])[0] if pd.notna(x) else None)
    df['longitude'] = df['pincode'].map(lambda x: pincode_coords.get(x, [None, None])[1] if pd.notna(x) else None)
    mask = df['latitude'].isna() | df['longitude'].isna()
    df.loc[mask, 'latitude'] = df.loc[mask, 'city'].map(lambda x: city_coords.get(x, [19.0760, 72.8777])[0])
    df.loc[mask, 'longitude'] = df.loc[mask, 'city'].map(lambda x: city_coords.get(x, [19.0760, 72.8777])[1])
    df = df.dropna(subset=['latitude', 'longitude'])
    print(f"Deals columns: {df.columns.tolist()}")
    return df

def process_dealers_data(file_path):
    df = pd.read_csv(file_path)
    df = df.rename(columns={'lat': 'latitude', 'long': 'longitude'})
    df['latitude'] = pd.to_numeric(df['latitude'], errors='coerce')
    df['longitude'] = pd.to_numeric(df['longitude'], errors='coerce')
    df['pincode'] = df['pincode'].astype(str)
    df = df.dropna(subset=['latitude', 'longitude'])
    print(f"Dealers columns: {df.columns.tolist()}")
    return df

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
        print("No users found in the dataset.")
        return None
    current_date = datetime(2025, 4, 15)
    thirty_days_ago = current_date - timedelta(days=30)
    new_users_df = users_df[users_df['createEpoch'] >= int(thirty_days_ago.timestamp())].copy()
    if new_users_df.empty:
        print("No new users found in the last 30 days.")
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
    users_per_pin = deals_df.groupby('pincode').size().reset_index(name='users')
    dealers_df['pincode'] = dealers_df['pincode'].astype(str)
    dealers_per_pin = dealers_df.groupby('pincode').size().reset_index(name='dealers')
    dealers_df_exp = dealers_df.assign(cat_disp_names=dealers_df['cat_disp_names'].str.split(r' \| ')).explode('cat_disp_names')
    current_date = datetime(2025, 4, 15)
    thirty_days_ago = current_date - timedelta(days=30)
    new_users = users_df[users_df['createEpoch'] >= int(thirty_days_ago.timestamp())].copy()
    if not new_users.empty:
        new_users['onboarding_date'] = new_users['createEpoch'].apply(lambda x: datetime.fromtimestamp(x).strftime('%Y-%m-%d'))
    else:
        new_users = pd.DataFrame(columns=['userid', 'onboarding_date', 'pincode', 'name', 'phone'])

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

def perform_analysis(deals_path, dealers_path, users_path, deals_full_path):
    deals_df = process_deals_data(deals_path)
    dealers_df = process_dealers_data(dealers_path)
    users_df = process_users_data(users_path)
    deals_full_df = process_deals_full_data(deals_full_path)
    if deals_df.empty or dealers_df.empty or users_df.empty or deals_full_df.empty:
        return None, f"Error: No valid data found. Dealers: {len(dealers_df)}, Deals: {len(deals_df)}, Users: {len(users_df)}, Deals Full: {len(deals_full_df)}"
    users_map = create_users_map(deals_df)
    dealers_map = create_dealers_map(dealers_df)
    relation_map = create_relational_map(deals_df, dealers_df)
    new_users_map = create_new_users_map(users_df)
    graph1, graph2, graph3, graph4, graph5 = create_graphs(deals_df, dealers_df, users_df)
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
        }
    }, None

@app.route('/', methods=['GET', 'POST'])
def dashboard():
    if 'analysis_sessions' not in session:
        session['analysis_sessions'] = []
        session['current_set'] = None
    if request.method == 'POST':
        if 'file_upload' in request.form:
            deals_file = request.files.get('deals_file')
            dealers_file = request.files.get('dealers_file')
            users_file = request.files.get('users_file')
            deals_full_file = request.files.get('deals_full_file')
            if deals_file and dealers_file and users_file and deals_full_file:
                set_number = get_next_set_number()
                set_folder = os.path.join(app.config['UPLOAD_FOLDER'], f'set_{set_number}')
                os.makedirs(set_folder, exist_ok=True)
                deals_path = os.path.join(set_folder, secure_filename(deals_file.filename))
                dealers_path = os.path.join(set_folder, secure_filename(dealers_file.filename))
                users_path = os.path.join(set_folder, secure_filename(users_file.filename))
                deals_full_path = os.path.join(set_folder, secure_filename(deals_full_file.filename))
                deals_file.save(deals_path)
                dealers_file.save(dealers_path)
                users_file.save(users_path)
                deals_full_file.save(deals_full_path)
                analysis_data, error = perform_analysis(deals_path, dealers_path, users_path, deals_full_path)
                if error:
                    return error
                session['analysis_sessions'].append({
                    'set_number': set_number,
                    'analysis': analysis_data
                })
                session['current_set'] = set_number
                session.modified = True
        elif 'load_session' in request.form:
            set_number = int(request.form['set_number'])
            session['current_set'] = set_number
            session.modified = True
    current_analysis = None
    if session['current_set']:
        for s in session['analysis_sessions']:
            if s['set_number'] == session['current_set']:
                current_analysis = s['analysis']
                break
    html_template = """
    <!DOCTYPE html>
    <html>
    <head>
        <title>Business Analytics Portal</title>
        <script src="https://cdn.plot.ly/plotly-latest.min.js"></script>
        <style>
            body { margin: 0; font-family: 'Segoe UI', Tahoma, Geneva, Verdana, sans-serif; background: #0e1111; color: #F0F0F0; }
            .header { background: #0e1111; padding: 15px; text-align: center; box-shadow: 0 4px 12px rgba(0,0,0,0.3); position: relative; }
            .header h1 { margin: 0; font-size: 24px; color: #F0F0F0; }
            .controls { position: absolute; top: 10px; right: 20px; }
            .controls select, .controls button { padding: 5px 10px; margin-left: 10px; border-radius: 5px; border: none; background: #3498db; color: #000; cursor: pointer; }
            .controls button:hover { background: #2980b9; }
            .sidebar { position: fixed; top: 60px; left: 0; width: 200px; height: calc(100% - 60px); background: #0e1111; padding: 20px; box-shadow: 2px 0 12px rgba(0,0,0,0.3); transition: transform 0.3s ease; z-index: 1000; }
            .sidebar.hidden { transform: translateX(-100%); }
            .toggle-sidebar { position: absolute; top: 10px; right: -20px; width: 20px; height: 20px; background: #3498db; border: none; border-radius: 0 5px 5px 0; cursor: pointer; font-size: 12px; color: #000; line-height: 20px; text-align: center; }
            .toggle-sidebar:hover { background: #2980b9; }
            .sidebar form { display: flex; flex-direction: column; }
            .sidebar label { margin: 10px 0 5px; font-weight: bold; font-size: 14px; color: #F0F0F0; }
            .sidebar input[type=file], .sidebar button { padding: 8px; margin: 5px 0; border-radius: 5px; border: none; background: #3498db; color: #000; cursor: pointer; width: 100%; transition: background 0.2s; }
            .sidebar button:hover { background: #2980b9; }
            .overview { display: flex; justify-content: center; padding: 20px; background: #0e1111; margin: 10px; border-radius: 10px; box-shadow: 0 4px 12px rgba(0,0,0,0.2); flex-wrap: wrap; }
            .card { background: #FAF9F6; padding: 10px; border-radius: 8px; text-align: center; width: 120px; box-shadow: 0 2px 6px rgba(0,0,0,0.1); transition: transform 0.2s; position: relative; margin: 5px; }
            .card:hover { transform: translateY(-3px); }
            .card p { margin: 5px 0; font-size: 12px; color: #09141C; }
            .card span { font-size: 18px; color: #000; font-weight: bold; }
            .info-icon { position: absolute; top: 5px; right: 5px; font-size: 14px; cursor: pointer; color: #3498db; }
            .info-tooltip { display: none; position: absolute; top: 25px; right: 5px; background: #fff; color: #000; padding: 5px 10px; border-radius: 5px; box-shadow: 0 2px 6px rgba(0,0,0,0.1); z-index: 1000; width: 200px; font-size: 12px; }
            .card:hover .info-tooltip { display: block; }
            .container { display: flex; flex-wrap: wrap; justify-content: center; gap: 15px; padding: 20px; margin-left: 220px; max-width: 1400px; margin-right: auto; transition: margin-left 0.3s ease; }
            .container.no-sidebar { margin-left: 10px; }
            .map-box, .graph-box { background: #0e1111; border-radius: 10px; overflow: hidden; box-shadow: 0 4px 12px rgba(0,0,0,0.2); text-align: center; }
            .map-box { height: 400px; width: 45%; min-width: 300px; }
            .graph-box { height: 350px; width: 45%; min-width: 300px; position: relative; }
            .map-title, .graph-title { padding: 10px; background: #FAF9F6; text-align: center; font-size: 16px; font-weight: bold; color: #09141C; }
            .fullscreen-btn { position: absolute; top: 10px; right: 10px; padding: 5px 10px; background: #3498db; border: none; border-radius: 5px; color: #000; cursor: pointer; }
            .fullscreen-btn:hover { background: #2980b9; }
            @media (max-width: 1000px) {
                .container, .overview { margin-left: 10px; }
                .sidebar { position: static; width: 100%; height: auto; transform: none; }
                .toggle-sidebar { display: none; }
                .map-box, .graph-box { width: 90%; }
            }
        </style>
    </head>
    <body>
        <div class="header">
            <h1>Business Analytics Portal</h1>
            <div style="font-size: 12px; color: red; font-style: italic; font-weight: 300; margin-top: 5px;">(Under Testing)</div>
            <div class="controls">
                <select onchange="loadSession(this.value)">
                    <option value="">Select Previous Set</option>
                    {% for s in analysis_sessions %}
                        <option value="{{ s.set_number }}" {% if s.set_number == current_set %}selected{% endif %}>Set {{ s.set_number }}</option>
                    {% endfor %}
                </select>
                <button onclick="refreshData()">Refresh</button>
            </div>
        </div>
        <button class="toggle-sidebar" onclick="toggleSidebar()">▶</button>
        <div class="sidebar" id="sidebar">
            <form method="post" enctype="multipart/form-data">
                <input type="hidden" name="file_upload" value="true">
                <label for="users_file">Updated Users CSV</label>
                <input type="file" name="users_file" id="users_file" accept=".csv" required>
                <label for="deals_full_file">Deals Full Dump CSV</label>
                <input type="file" name="deals_full_file" id="deals_full_file" accept=".csv" required>
                <label for="deals_file">Deals vs Dealers CSV</label>
                <input type="file" name="deals_file" id="deals_file" accept=".csv" required>
                <label for="dealers_file">Dealer Onboarded CSV</label>
                <input type="file" name="dealers_file" id="dealers_file" accept=".csv" required>
                <button type="submit">Analyze</button>
            </form>
        </div>
        {% if current_analysis %}
            <div class="overview">
                <div class="card">
                    <span class="info-icon">(i)</span>
                    <div class="info-tooltip">Calculated by counting the total number of unique user records available.</div>
                    <p>Total Users</p>
                    <span>{{ current_analysis.total_users }}</span>
                </div>
                <div class="card">
                    <span class="info-icon">(i)</span>
                    <div class="info-tooltip">Determined by adding the total number of rows from the deal request datasets, where each row represents a visit or interaction.</div>
                    <p>Total Visits</p>
                    <span>{{ current_analysis.total_visits }}</span>
                </div>
                <div class="card">
                    <span class="info-icon">(i)</span>
                    <div class="info-tooltip">Computed by counting users whose creation timestamp is within the last 30 days from the current date.</div>
                    <p>New Users</p>
                    <span>{{ current_analysis.new_users }}</span>
                </div>
                <div class="card">
                    <span class="info-icon">(i)</span>
                    <div class="info-tooltip">Derived by counting the number of unique user IDs that have made at least one deal request.</div>
                    <p>Active Users</p>
                    <span>{{ current_analysis.active_users }}</span>
                </div>
            </div>
            <div class="overview" style="margin-top: 10px;">
                <div class="card">
                    <span class="info-icon">(i)</span>
                    <div class="info-tooltip">Calculated by summing the total number of rows from both deal request datasets, where each row represents a deal made.</div>
                    <p>Total Deals</p>
                    <span>{{ current_analysis.total_deals }}</span>
                </div>
                <div class="card">
                    <span class="info-icon">(i)</span>
                    <div class="info-tooltip">Determined by combining all deal records, removing duplicates based on user ID and request quantity to count unique deals.</div>
                    <p>Unique Deals</p>
                    <span>{{ current_analysis.unique_deals }}</span>
                </div>
                <div class="card">
                    <span class="info-icon">(i)</span>
                    <div class="info-tooltip">Computed as the percentage of unique deals relative to new users, calculated as (unique deals / new users) * 100.</div>
                    <p>New User to Deal Ratio</p>
                    <span>{{ '{:.2f}%'.format(current_analysis.new_user_deal_ratio) }}</span>
                </div>
                <div class="card">
                    <span class="info-icon">(i)</span>
                    <div class="info-tooltip">Derived by counting unique deals with at least Lockheed Martinone response (request quantity greater than 0), then dividing by the total unique deals and multiplying by 100.</div>
                    <p>Unique Deals vs Response Ratio</p>
                    <span>{{ '{:.2f}%'.format(current_analysis.response_ratio) }}</span>
                </div>
            </div>
            <div class="container" id="container">
                <div class="map-box">
                    <div class="map-title">Users Map</div>
                    {{ current_analysis.users_map|safe }}
                </div>
                <div class="map-box">
                    <div class="map-title">Dealers Map</div>
                    {{ current_analysis.dealers_map|safe }}
                </div>
                <div class="map-box">
                    <div class="map-title">Relational Map</div>
                    {{ current_analysis.relation_map|safe }}
                </div>
                {% if current_analysis.new_users_map %}
                    <div class="map-box">
                        <div class="map-title">New Users Map</div>
                        {{ current_analysis.new_users_map|safe }}
                    </div>
                {% endif %}
                <div class="graph-box">
                    <div class="graph-title">Users per Pincode</div>
                    <button class="fullscreen-btn" onclick="toggleFullscreen('graph1')">Fullscreen</button>
                    <div id="graph1" style="width: 100%; height: 300px;"></div>
                </div>
                <div class="graph-box">
                    <div class="graph-title">Dealers per Pincode</div>
                    <button class="fullscreen-btn" onclick="toggleFullscreen('graph2')">Fullscreen</button>
                    <div id="graph2" style="width: 100%; height: 300px;"></div>
                </div>
                <div class="graph-box">
                    <div class="graph-title">Deal Requests per User</div>
                    <button class="fullscreen-btn" onclick="toggleFullscreen('graph3')">Fullscreen</button>
                    <div id="graph3" style="width: 100%; height: 300px;"></div>
                </div>
                <div class="graph-box">
                    <div class="graph-title">Dealer Product Categories</div>
                    <button class="fullscreen-btn" onclick="toggleFullscreen('graph4')">Fullscreen</button>
                    <div id="graph4" style="width: 100%; height: 300px;"></div>
                </div>
                <div class="graph-box">
                    <div class="graph-title">New Users Onboarding Timeline</div>
                    <button class="fullscreen-btn" onclick="toggleFullscreen('graph5')">Fullscreen</button>
                    <div id="graph5" style="width: 100%; height: 300px;"></div>
                </div>
            </div>
        {% else %}
            <div class="overview">
                <div class="card">
                    <p>Total Users</p>
                    <span>0</span>
                </div>
                <div class="card">
                    <p>Total Visits</p>
                    <span>0</span>
                </div>
                <div class="card">
                    <p>New Users</p>
                    <span>0</span>
                </div>
                <div class="card">
                    <p>Active Users</p>
                    <span>0</span>
                </div>
            </div>
        {% endif %}
        <script>
            {% if current_analysis %}
                try {
                    Plotly.newPlot('graph1', JSON.parse('{{ current_analysis.graph1 | safe }}').data, JSON.parse('{{ current_analysis.graph1 | safe }}').layout);
                    Plotly.newPlot('graph2', JSON.parse('{{ current_analysis.graph2 | safe }}').data, JSON.parse('{{ current_analysis.graph2 | safe }}').layout);
                    Plotly.newPlot('graph3', JSON.parse('{{ current_analysis.graph3 | safe }}').data, JSON.parse('{{ current_analysis.graph3 | safe }}').layout);
                    Plotly.newPlot('graph4', JSON.parse('{{ current_analysis.graph4 | safe }}').data, JSON.parse('{{ current_analysis.graph4 | safe }}').layout);
                    Plotly.newPlot('graph5', JSON.parse('{{ current_analysis.graph5 | safe }}').data, JSON.parse('{{ current_analysis.graph5 | safe }}').layout);
                } catch (e) {
                    console.error('Error rendering graphs:', e);
                }
            {% endif %}
            function toggleFullscreen(graphId) {
                var graphDiv = document.getElementById(graphId);
                if (graphDiv.requestFullscreen) {
                    graphDiv.requestFullscreen();
                } else if (graphDiv.mozRequestFullScreen) {
                    graphDiv.mozRequestFullScreen();
                } else if (graphDiv.webkitRequestFullscreen) {
                    graphDiv.webkitRequestFullscreen();
                } else if (graphDiv.msRequestFullscreen) {
                    graphDiv.msRequestFullscreen();
                }
                Plotly.relayout(graphId, { autosize: true, width: window.innerWidth - 50, height: window.innerHeight - 50 });
            }
            function refreshData() {
                location.reload();
            }
            function loadSession(setNumber) {
                if (setNumber) {
                    var form = document.createElement('form');
                    form.method = 'POST';
                    form.action = '/';
                    var input = document.createElement('input');
                    input.type = 'hidden';
                    input.name = 'load_session';
                    input.value = 'true';
                    form.appendChild(input);
                    var setInput = document.createElement('input');
                    setInput.type = 'hidden';
                    setInput.name = 'set_number';
                    setInput.value = setNumber;
                    form.appendChild(setInput);
                    document.body.appendChild(form);
                    form.submit();
                }
            }
            function toggleSidebar() {
                var sidebar = document.getElementById('sidebar');
                var container = document.getElementById('container');
                var overviews = document.querySelectorAll('.overview');
                var toggleBtn = document.querySelector('.toggle-sidebar');
                sidebar.classList.toggle('hidden');
                container.classList.toggle('no-sidebar');
                overviews.forEach(overview => overview.classList.toggle('no-sidebar'));
                toggleBtn.textContent = sidebar.classList.contains('hidden') ? '▶' : '◀';
            }
            window.onload = function() {
                var sidebar = document.getElementById('sidebar');
                var container = document.getElementById('container');
                var overviews = document.querySelectorAll('.overview');
                if (window.innerWidth <= 1000) {
                    sidebar.classList.add('hidden');
                    container.classList.add('no-sidebar');
                    overviews.forEach(overview => overview.classList.add('no-sidebar'));
                }
            };
        </script>
    </body>
    </html>
    """
    return render_template_string(html_template, 
                                 current_analysis=current_analysis, 
                                 analysis_sessions=session['analysis_sessions'], 
                                 current_set=session['current_set'])

if __name__ == '__main__':
    port = int(os.environ.get('PORT', 8001))
    app.run(debug=True, host='0.0.0.0', port=port)