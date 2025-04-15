from flask import Flask, render_template_string, request
import folium
import pandas as pd
import plotly.express as px
import json
import os
from werkzeug.utils import secure_filename
from datetime import datetime

app = Flask(__name__)
UPLOAD_FOLDER = 'uploads'
app.config['UPLOAD_FOLDER'] = UPLOAD_FOLDER
os.makedirs(UPLOAD_FOLDER, exist_ok=True)

# Pincode lat/long data (for users map)
pincode_coords = {
    "400078": [19.1011, 72.8376], "410206": [19.0330, 73.0297], "401105": [19.3000, 72.8500],
    "360001": [22.3039, 70.8022], "421301": [19.2333, 73.1333], "400003": [18.9500, 72.8333],
    "401303": [19.7000, 72.7667], "400705": [19.0330, 73.0150], "421204": [19.2167, 73.1500],
    "400072": [19.1667, 72.8333], "400092": [19.1167, 72.9167], "400104": [19.1667, 72.8667],
    "400607": [19.2167, 72.9667], "400089": [19.1333, 72.8167], "400701": [19.0330, 73.0667],
    "400602": [19.2167, 72.9833], "401101": [19.3000, 72.8667], "400065": [19.0667, 72.8833],
    "400601": [19.1950, 72.9770]
}

# City lat/long fallback (for users map)
city_coords = {
    "Mumbai": [19.0760, 72.8777], "Thane": [19.2183, 72.9781], "Navi Mumbai": [19.0330, 73.0297],
    "Rajkot": [22.3039, 70.8022], "Kalyan": [19.2403, 73.1305], "Mira Bhayandar": [19.2952, 72.8544],
    "Virar": [19.4657, 72.8114], "Adai": [19.0330, 73.0297], "Vashi": [19.0771, 72.9986], "Sion": [19.0400, 72.8600]
}

def process_users_data(file_path):
    df = pd.read_csv(file_path)
    df['pincode'] = df['pincode'].astype(str).str.extract(r'(\d{6})')
    df['city'] = df['locality'].fillna(df.get('state', 'Mumbai'))
    df['latitude'] = df['pincode'].map(lambda x: pincode_coords.get(x, [None, None])[0])
    df['longitude'] = df['pincode'].map(lambda x: pincode_coords.get(x, [None, None])[1])
    mask = df['latitude'].isna() | df['longitude'].isna()
    df.loc[mask, 'latitude'] = df.loc[mask, 'city'].map(lambda x: city_coords.get(x, [19.0760, 72.8777])[0])
    df.loc[mask, 'longitude'] = df.loc[mask, 'city'].map(lambda x: city_coords.get(x, [19.0760, 72.8777])[1])
    df = df.dropna(subset=['latitude', 'longitude'])
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
    df.loc[:, 'latitude'] = df['pincode'].map(lambda x: pincode_coords.get(x, [None, None])[0] if pd.notna(x) else None)
    df.loc[:, 'longitude'] = df['pincode'].map(lambda x: pincode_coords.get(x, [None, None])[1] if pd.notna(x) else None)
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
    print(f"Dealers processed: {len(df)}")
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
    for _, row in dealers_df.iterrows():
        folium.CircleMarker(
            location=[row['latitude'], row['longitude']],
            radius=5,
            popup=f"Dealer: {row.get('coname', 'Unknown')}<br>Pincode: {row['pincode']}",
            color="#ff7f0e",
            fill=True,
            fill_opacity=0.7
        ).add_to(relation_map)
    return relation_map._repr_html_()

def create_new_users_map(users_df):
    current_epoch = int(datetime.now().timestamp())
    new_users = users_df[users_df['createEpoch'] >= current_epoch - 30 * 24 * 3600]
    if new_users.empty:
        print("No new users found in the last 30 days.")
        return None
    new_users_map = folium.Map(location=[19.0760, 72.8777], zoom_start=11, tiles="cartodbpositron")
    for _, row in new_users.iterrows():
        popup_content = (
            f"Name: {row['name']}<br>"
            f"User ID: {row['userid']}<br>"
            f"Phone: {row['phone']}<br>"
            f"Pincode: {row['pincode']}<br>"
            f"Created: {datetime.fromtimestamp(row['createEpoch']).strftime('%Y-%m-%d')}"
        )
        folium.CircleMarker(
            location=[row['latitude'], row['longitude']],
            radius=5,
            popup=popup_content,
            color="#17becf",  # Cyan for new users
            fill=True,
            fill_opacity=0.7
        ).add_to(new_users_map)
    return new_users_map._repr_html_()

def create_graphs(deals_df, dealers_df, users_df):
    users_per_pin = deals_df.groupby('pincode').size().reset_index(name='users')
    dealers_df['pincode'] = dealers_df['pincode'].astype(str)
    dealers_per_pin = dealers_df.groupby('pincode').size().reset_index(name='dealers')
    dealers_df_exp = dealers_df.assign(cat_disp_names=dealers_df['cat_disp_names'].str.split(' \| ')).explode('cat_disp_names')
    
    current_epoch = int(datetime.now().timestamp())
    new_users = users_df[users_df['createEpoch'] >= current_epoch - 30 * 24 * 3600].copy()
    if not new_users.empty:
        new_users['onboarding_date'] = new_users['createEpoch'].apply(lambda x: datetime.fromtimestamp(x).strftime('%Y-%m-%d'))

    fig1 = px.bar(users_per_pin, x='pincode', y='users', title="Users per Pincode", 
                  color_discrete_sequence=['#1f77b4'], template='plotly_dark')
    fig2 = px.bar(dealers_per_pin, x='pincode', y='dealers', title="Dealers per Pincode", 
                  color_discrete_sequence=['#ff7f0e'], template='plotly_dark')
    fig3 = px.bar(deals_df, x='user_id', y='req_qty', title="Deal Requests per User", 
                  color='pincode', template='plotly_dark')
    fig4 = px.bar(dealers_df_exp, x='coname' if 'coname' in dealers_df_exp.columns else '_id', y='cat_disp_names', 
                  title="Dealer Product Categories",
                  color='pincode', hover_data=['phone_no', 'addr1', 'addr2', 'landmark', 'city', 'subcat_disp_names', 'Imgurl'],
                  template='plotly_dark')
    fig5 = px.scatter(new_users, x='userid', y='onboarding_date', title="New Users Onboarding Timeline", 
                      color='pincode', hover_data=['name', 'phone', 'pincode'], template='plotly_dark')

    return fig1.to_json(), fig2.to_json(), fig3.to_json(), fig4.to_json(), fig5.to_json()

@app.route('/', methods=['GET', 'POST'])
def dashboard():
    if request.method == 'POST':
        deals_file = request.files.get('deals_file')
        dealers_file = request.files.get('dealers_file')
        users_file = request.files.get('users_file')
        deals_full_file = request.files.get('deals_full_file')
        if deals_file and dealers_file and users_file and deals_full_file:
            deals_path = os.path.join(app.config['UPLOAD_FOLDER'], secure_filename(deals_file.filename))
            dealers_path = os.path.join(app.config['UPLOAD_FOLDER'], secure_filename(dealers_file.filename))
            users_path = os.path.join(app.config['UPLOAD_FOLDER'], secure_filename(users_file.filename))
            deals_full_path = os.path.join(app.config['UPLOAD_FOLDER'], secure_filename(deals_full_file.filename))
            deals_file.save(deals_path)
            dealers_file.save(dealers_path)
            users_file.save(users_path)
            deals_full_file.save(deals_full_path)
            
            deals_df = process_deals_data(deals_path)
            dealers_df = process_dealers_data(dealers_path)
            users_df = process_users_data(users_path)
            deals_full_df = process_deals_full_data(deals_full_path)
            
            if deals_df.empty or dealers_df.empty or users_df.empty or deals_full_df.empty:
                return f"Error: No valid data found. Dealers: {len(dealers_df)}, Deals: {len(deals_df)}, Users: {len(users_df)}, Deals Full: {len(deals_full_df)}"
            
            users_map = create_users_map(deals_df)
            dealers_map = create_dealers_map(dealers_df)
            relation_map = create_relational_map(deals_df, dealers_df)
            new_users_map = create_new_users_map(users_df)
            graph1, graph2, graph3, graph4, graph5 = create_graphs(deals_df, dealers_df, users_df)
            
            html_template = """
            <!DOCTYPE html>
            <html>
            <head>
                <title>Business Analytics Portal</title>
                <script src="https://cdn.plot.ly/plotly-latest.min.js"></script>
                <style>
                    body { margin: 0; font-family: 'Segoe UI', sans-serif; background: #1a1a1a; color: #fff; }
                    .header { background: #2c3e50; padding: 20px; text-align: center; box-shadow: 0 2px 5px rgba(0,0,0,0.3); }
                    .header h1 { margin: 0; font-size: 28px; }
                    .upload { padding: 20px; text-align: center; background: #34495e; margin: 20px; border-radius: 10px; }
                    .container { display: grid; grid-template-columns: repeat(auto-fit, minmax(400px, 1fr)); gap: 20px; padding: 20px; }
                    .map, .graph { background: #2c3e50; border-radius: 10px; padding: 10px; box-shadow: 0 4px 10px rgba(0,0,0,0.2); }
                    .map { height: 600px; }
                    .graph { height: 400px; }
                    .upload label { display: block; margin: 10px 0 5px; font-weight: bold; }
                    input[type=file], button { padding: 10px; margin: 5px; border-radius: 5px; border: none; background: #3498db; color: #fff; cursor: pointer; width: 200px; }
                    button:hover { background: #2980b9; }
                </style>
            </head>
            <body>
                <div class="header"><h1>Business Analytics Portal</h1></div>
                <div class="upload">
                    <form method="post" enctype="multipart/form-data">
                        <label for="users_file">Upload Updated Users List CSV</label>
                        <input type="file" name="users_file" id="users_file" accept=".csv" required>
                        <label for="deals_full_file">Upload Deals Full Dump CSV</label>
                        <input type="file" name="deals_full_file" id="deals_full_file" accept=".csv" required>
                        <label for="deals_file">Upload Deals vs Dealers CSV</label>
                        <input type="file" name="deals_file" id="deals_file" accept=".csv" required>
                        <label for="dealers_file">Upload Dealer Onboarded CSV</label>
                        <input type="file" name="dealers_file" id="dealers_file" accept=".csv" required>
                        <button type="submit">Upload & Analyze</button>
                    </form>
                </div>
                <div class="container">
                    <div class="map">{{ users_map|safe }}</div>
                    <div class="map">{{ dealers_map|safe }}</div>
                    <div class="map">{{ relation_map|safe }}</div>
                    {% if new_users_map %}
                        <div class="map">{{ new_users_map|safe }}</div>
                    {% endif %}
                    <div class="graph" id="graph1"></div>
                    <div class="graph" id="graph2"></div>
                    <div class="graph" id="graph3"></div>
                    <div class="graph" id="graph4"></div>
                    <div class="graph" id="graph5"></div>
                </div>
                <script>
                    var graph1 = {{ graph1|safe }};
                    var graph2 = {{ graph2|safe }};
                    var graph3 = {{ graph3|safe }};
                    var graph4 = {{ graph4|safe }};
                    var graph5 = {{ graph5|safe }};
                    Plotly.newPlot('graph1', graph1.data, graph1.layout);
                    Plotly.newPlot('graph2', graph2.data, graph2.layout);
                    Plotly.newPlot('graph3', graph3.data, graph3.layout);
                    Plotly.newPlot('graph4', graph4.data, graph4.layout);
                    Plotly.newPlot('graph5', graph5.data, graph5.layout);
                </script>
            </body>
            </html>
            """
            return render_template_string(html_template, users_map=users_map, dealers_map=dealers_map, 
                                        relation_map=relation_map, new_users_map=new_users_map, 
                                        graph1=graph1, graph2=graph2, graph3=graph3, graph4=graph4, graph5=graph5)
    return render_template_string("""
        <!DOCTYPE html>
        <html>
        <head>
            <title>Business Analytics Portal</title>
            <style>
                body { margin: 0; font-family: 'Segoe UI', sans-serif; background: #1a1a1a; color: #fff; }
                .header { background: #2c3e50; padding: 20px; text-align: center; box-shadow: 0 2px 5px rgba(0,0,0,0.3); }
                .header h1 { margin: 0; font-size: 28px; }
                .upload { padding: 20px; text-align: center; background: #34495e; margin: 20px; border-radius: 10px; }
                .upload label { display: block; margin: 10px 0 5px; font-weight: bold; }
                input[type=file], button { padding: 10px; margin: 5px; border-radius: 5px; border: none; background: #3498db; color: #fff; cursor: pointer; width: 200px; }
                button:hover { background: #2980b9; }
            </style>
        </head>
        <body>
            <div class="header"><h1>Business Analytics Portal</h1></div>
            <div class="upload">
                <form method="post" enctype="multipart/form-data">
                    <label for="users_file">Upload Updated Users List CSV</label>
                    <input type="file" name="users_file" id="users_file" accept=".csv" required>
                    <label for="deals_full_file">Upload Deals Full Dump CSV</label>
                    <input type="file" name="deals_full_file" id="deals_full_file" accept=".csv" required>
                    <label for="deals_file">Upload Deals vs Dealers CSV</label>
                    <input type="file" name="deals_file" id="deals_file" accept=".csv" required>
                    <label for="dealers_file">Upload Dealer Onboarded CSV</label>
                    <input type="file" name="dealers_file" id="dealers_file" accept=".csv" required>
                    <button type="submit">Upload & Analyze</button>
                </form>
            </div>
        </body>
        </html>
    """)

if __name__ == '__main__':
    app.run(debug=True, port=8001)