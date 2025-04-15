from flask import Flask, render_template_string, request, session
import folium
import pandas as pd
import plotly.express as px
import json
import os
from werkzeug.utils import secure_filename
from datetime import datetime, timedelta
import shutil

app = Flask(__name__)
app.secret_key = 'your_secret_key_here'  # Required for session handling
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
    "400601": [19.1950, 72.9770], "400706": [19.0330, 73.0297], "400101": [19.0760, 72.8777],
    "421202": [19.2403, 73.1305], "833214": [22.8000, 85.3333]
}

# City lat/long fallback (for users map)
city_coords = {
    "Mumbai": [19.0760, 72.8777], "Thane": [19.2183, 72.9781], "Navi Mumbai": [19.0330, 73.0297],
    "Rajkot": [22.3039, 70.8022], "Kalyan": [19.2403, 73.1305], "Dombivli": [19.2133, 73.0833],
    "Mira Bhayandar": [19.2952, 72.8544], "Virar": [19.4657, 72.8114], "Adai": [19.0330, 73.0297],
    "Vashi": [19.0771, 72.9986], "Sion": [19.0400, 72.8600], "Ambdiha": [22.8000, 85.3333]
}

def get_next_set_number():
    """Get the next available set number by checking existing folders"""
    existing_folders = [d for d in os.listdir(UPLOAD_FOLDER) if d.startswith('set_')]
    if not existing_folders:
        return 1
    return max(int(folder.split('_')[1]) for folder in existing_folders) + 1

def save_uploaded_files(files, set_number):
    """Save uploaded files to a numbered set folder"""
    set_folder = os.path.join(app.config['UPLOAD_FOLDER'], f'set_{set_number}')
    os.makedirs(set_folder, exist_ok=True)
    
    file_paths = {}
    for file_type in ['deals_file', 'dealers_file', 'users_file', 'deals_full_file']:
        if file_type in files:
            file = files[file_type]
            filename = secure_filename(file.filename)
            file_path = os.path.join(set_folder, filename)
            file.save(file_path)
            file_paths[file_type] = file_path
    
    return file_paths

def load_analysis_data(set_number):
    """Load analysis data from a specific set folder"""
    set_folder = os.path.join(app.config['UPLOAD_FOLDER'], f'set_{set_number}')
    if not os.path.exists(set_folder):
        return None
    
    # Find the files in the set folder (since we don't know their exact names)
    files = {}
    for file_type, pattern in [
        ('deals_file', 'deals'),
        ('dealers_file', 'dealers'),
        ('users_file', 'users'),
        ('deals_full_file', 'deals_full')
    ]:
        matching_files = [f for f in os.listdir(set_folder) if pattern in f.lower()]
        if matching_files:
            files[file_type] = os.path.join(set_folder, matching_files[0])
    
    if len(files) != 4:
        return None
    
    return process_data(files)

def process_data(file_paths):
    """Process the data from file paths and return analysis results"""
    deals_df = process_deals_data(file_paths['deals_file'])
    dealers_df = process_dealers_data(file_paths['dealers_file'])
    users_df = process_users_data(file_paths['users_file'])
    deals_full_df = process_deals_full_data(file_paths['deals_full_file'])
    
    if deals_df.empty or dealers_df.empty or users_df.empty or deals_full_df.empty:
        return None
    
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
        'graphs': {
            'graph1': graph1,
            'graph2': graph2,
            'graph3': graph3,
            'graph4': graph4,
            'graph5': graph5
        },
        'metrics': {
            'total_users': total_users,
            'total_visits': total_visits,
            'new_users': new_users,
            'active_users': active_users,
            'total_deals': total_deals,
            'unique_deals': unique_deals,
            'new_user_deal_ratio': new_user_deal_ratio,
            'response_ratio': response_ratio
        }
    }

# [Keep all your existing processing functions: process_users_data, process_deals_full_data, 
# process_deals_data, process_dealers_data, create_users_map, create_dealers_map, 
# create_relational_map, create_new_users_map, create_graphs exactly as they are]

@app.route('/', methods=['GET', 'POST'])
def dashboard():
    if request.method == 'POST':
        if 'load_set' in request.form:
            # User wants to load a previous analysis
            set_number = int(request.form['load_set'])
            session['current_set'] = set_number
            analysis_data = load_analysis_data(set_number)
            
            if analysis_data:
                return render_analysis_template(analysis_data, session.get('available_sets', []), set_number)
            return "Error: Could not load analysis data for the selected set."
        
        # User is uploading new files
        files = {
            'deals_file': request.files.get('deals_file'),
            'dealers_file': request.files.get('dealers_file'),
            'users_file': request.files.get('users_file'),
            'deals_full_file': request.files.get('deals_full_file')
        }
        
        if all(files.values()):
            set_number = get_next_set_number()
            file_paths = save_uploaded_files(files, set_number)
            analysis_data = process_data(file_paths)
            
            if analysis_data:
                # Update session with available sets
                available_sets = session.get('available_sets', [])
                if set_number not in available_sets:
                    available_sets.append(set_number)
                    session['available_sets'] = available_sets
                session['current_set'] = set_number
                
                return render_analysis_template(analysis_data, available_sets, set_number)
            return "Error: Could not process the uploaded files."
    
    # GET request or no files uploaded yet
    available_sets = session.get('available_sets', [])
    current_set = session.get('current_set')
    
    if current_set:
        analysis_data = load_analysis_data(current_set)
        if analysis_data:
            return render_analysis_template(analysis_data, available_sets, current_set)
    
    return render_upload_template(available_sets)

def render_analysis_template(analysis_data, available_sets, current_set):
    """Render the template with analysis results"""
    html_template = """
    <!DOCTYPE html>
    <html>
    <head>
        <title>Business Analytics Portal</title>
        <script src="https://cdn.plot.ly/plotly-latest.min.js"></script>
        <style>
            /* [Keep all your existing CSS styles exactly as they are] */
            .set-selector {
                position: absolute;
                top: 10px;
                left: 220px;
                padding: 5px 10px;
                border-radius: 5px;
                border: none;
                background: #3498db;
                color: #000;
                cursor: pointer;
            }
            .set-selector:hover {
                background: #2980b9;
            }
        </style>
    </head>
    <body>
        <div class="header">
            <h1>Business Analytics Portal</h1>
            <div class="controls">
                <select><option>Today</option><option>Week</option><option>Month</option></select>
                <button onclick="refreshData()">Refresh</button>
            </div>
        </div>
        <button class="toggle-sidebar" onclick="toggleSidebar()">▶</button>
        <div class="sidebar" id="sidebar">
            <form method="post" enctype="multipart/form-data">
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
        {% if available_sets %}
        <select class="set-selector" onchange="loadSet(this.value)">
            <option value="">Select Analysis Set</option>
            {% for set_num in available_sets %}
            <option value="{{ set_num }}" {% if set_num == current_set %}selected{% endif %}>Set {{ set_num }}</option>
            {% endfor %}
        </select>
        {% endif %}
        <div class="overview">
            <div class="card">
                <span class="info-icon">(i)</span>
                <div class="info-tooltip">Calculated by counting the total number of unique user records available.</div>
                <p>Total Users</p>
                <span>{{ metrics.total_users }}</span>
            </div>
            <div class="card">
                <span class="info-icon">(i)</span>
                <div class="info-tooltip">Determined by adding the total number of rows from the deal request datasets, where each row represents a visit or interaction.</div>
                <p>Total Visits</p>
                <span>{{ metrics.total_visits }}</span>
            </div>
            <div class="card">
                <span class="info-icon">(i)</span>
                <div class="info-tooltip">Computed by counting users whose creation timestamp is within the last 30 days from the current date.</div>
                <p>New Users</p>
                <span>{{ metrics.new_users }}</span>
            </div>
            <div class="card">
                <span class="info-icon">(i)</span>
                <div class="info-tooltip">Derived by counting the number of unique user IDs that have made at least one deal request.</div>
                <p>Active Users</p>
                <span>{{ metrics.active_users }}</span>
            </div>
        </div>
        <div class="overview" style="margin-top: 10px;">
            <div class="card">
                <span class="info-icon">(i)</span>
                <div class="info-tooltip">Calculated by summing the total number of rows from both deal request datasets, where each row represents a deal made.</div>
                <p>Total Deals</p>
                <span>{{ metrics.total_deals }}</span>
            </div>
            <div class="card">
                <span class="info-icon">(i)</span>
                <div class="info-tooltip">Determined by combining all deal records, removing duplicates based on user ID and request quantity to count unique deals, and then identifying those made by users onboarded in the last 30 days.</div>
                <p>Unique Deals</p>
                <span>{{ metrics.unique_deals }}</span>
            </div>
            <div class="card">
                <span class="info-icon">(i)</span>
                <div class="info-tooltip">Computed as the percentage of unique deals relative to new users, with the expectation that every 10 new users should create at least one deal, calculated as (unique deals / new users) * 100.</div>
                <p>New User to Deal Ratio</p>
                <span>{{ '{:.2f}%'.format(metrics.new_user_deal_ratio) }}</span>
            </div>
            <div class="card">
                <span class="info-icon">(i)</span>
                <div class="info-tooltip">Derived by counting unique deals with at least one response (request quantity greater than 0), then dividing by the total unique deals and multiplying by 100 to get the response percentage.</div>
                <p>Unique Deals vs Response Ratio</p>
                <span>{{ '{:.2f}%'.format(metrics.response_ratio) }}</span>
            </div>
        </div>
        <div class="container" id="container">
            <div class="map-box">
                <div class="map-title">Users Map</div>
                {{ users_map|safe }}
            </div>
            <div class="map-box">
                <div class="map-title">Dealers Map</div>
                {{ dealers_map|safe }}
            </div>
            <div class="map-box">
                <div class="map-title">Relational Map</div>
                {{ relation_map|safe }}
            </div>
            {% if new_users_map %}
                <div class="map-box">
                    <div class="map-title">New Users Map</div>
                    {{ new_users_map|safe }}
                </div>
            {% endif %}
            <div class="graph-box">
                <div class="graph-title">Users per Pincode</div>
                <button class="fullscreen-btn" onclick="toggleFullscreen('graph1')">Fullscreen</button>
                <div id="graph1"></div>
            </div>
            <div class="graph-box">
                <div class="graph-title">Dealers per Pincode</div>
                <button class="fullscreen-btn" onclick="toggleFullscreen('graph2')">Fullscreen</button>
                <div id="graph2"></div>
            </div>
            <div class="graph-box">
                <div class="graph-title">Deal Requests per User</div>
                <button class="fullscreen-btn" onclick="toggleFullscreen('graph3')">Fullscreen</button>
                <div id="graph3"></div>
            </div>
            <div class="graph-box">
                <div class="graph-title">Dealer Product Categories</div>
                <button class="fullscreen-btn" onclick="toggleFullscreen('graph4')">Fullscreen</button>
                <div id="graph4"></div>
            </div>
            <div class="graph-box">
                <div class="graph-title">New Users Onboarding Timeline</div>
                <button class="fullscreen-btn" onclick="toggleFullscreen('graph5')">Fullscreen</button>
                <div id="graph5"></div>
            </div>
        </div>
        <script>
            var graph1 = {{ graphs.graph1|safe }};
            var graph2 = {{ graphs.graph2|safe }};
            var graph3 = {{ graphs.graph3|safe }};
            var graph4 = {{ graphs.graph4|safe }};
            var graph5 = {{ graphs.graph5|safe }};
            Plotly.newPlot('graph1', graph1.data, graph1.layout);
            Plotly.newPlot('graph2', graph2.data, graph2.layout);
            Plotly.newPlot('graph3', graph3.data, graph3.layout);
            Plotly.newPlot('graph4', graph4.data, graph4.layout);
            Plotly.newPlot('graph5', graph5.data, graph5.layout);

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

            function toggleSidebar() {
                var sidebar = document.getElementById('sidebar');
                var container = document.getElementById('container');
                var toggleBtn = document.querySelector('.toggle-sidebar');
                sidebar.classList.toggle('hidden');
                container.classList.toggle('sidebar-visible');
                toggleBtn.textContent = sidebar.classList.contains('hidden') ? '▶' : '◀';
            }

            function loadSet(setNumber) {
                if (setNumber) {
                    var form = document.createElement('form');
                    form.method = 'POST';
                    form.action = '/';
                    
                    var input = document.createElement('input');
                    input.type = 'hidden';
                    input.name = 'load_set';
                    input.value = setNumber;
                    
                    form.appendChild(input);
                    document.body.appendChild(form);
                    form.submit();
                }
            }

            window.onload = function() {
                var sidebar = document.getElementById('sidebar');
                var container = document.getElementById('container');
                if (window.innerWidth > 1000) {
                    container.classList.add('sidebar-visible');
                }
            };
        </script>
    </body>
    </html>
    """
    return render_template_string(
        html_template,
        users_map=analysis_data['users_map'],
        dealers_map=analysis_data['dealers_map'],
        relation_map=analysis_data['relation_map'],
        new_users_map=analysis_data.get('new_users_map'),
        graphs=analysis_data['graphs'],
        metrics=analysis_data['metrics'],
        available_sets=available_sets,
        current_set=current_set
    )

def render_upload_template(available_sets):
    """Render the initial upload template"""
    return render_template_string("""
    <!DOCTYPE html>
    <html>
    <head>
        <title>Business Analytics Portal</title>
        <style>
            body { margin: 0; font-family: 'Segoe UI', Tahoma, Geneva, Verdana, sans-serif; background: #0e1111; color: #F0F0F0; }
            .header { background: #0e1111; padding: 15px; text-align: center; box-shadow: 0 4px 12px rgba(0,0,0,0.3); position: relative; }
            .header h1 { margin: 0; font-size: 24px; color: #F0F0F0; }
            .controls { position: absolute; top: 10px; right: 20px; }
            .controls select, .controls button { padding: 5px 10px; margin-left: 10px; border-radius: 5px; border: none; background: #3498db; color: #000; cursor: pointer; }
            .controls button:hover { background: #2980b9; }
            .sidebar { position: fixed; top: 60px; left: 0; width: 200px; height: calc(100% - 60px); background: #0e1111; padding: 20px; box-shadow: 2px 0 12px rgba(0,0,0,0.3); transition: transform 0.3s ease; }
            .sidebar.hidden { transform: translateX(-100%); }
            .toggle-sidebar { position: absolute; top: 10px; right: -20px; width: 20px; height: 20px; background: #3498db; border: none; border-radius: 0 5px 5px 0; cursor: pointer; font-size: 12px; color: #000; line-height: 20px; text-align: center; }
            .toggle-sidebar:hover { background: #2980b9; }
            .sidebar form { display: flex; flex-direction: column; }
            .sidebar label { margin: 10px 0 5px; font-weight: bold; font-size: 14px; color: #F0F0F0; }
            .sidebar input[type=file], .sidebar button { padding: 8px; margin: 5px 0; border-radius: 5px; border: none; background: #3498db; color: #000; cursor: pointer; width: 100%; transition: background 0.2s; }
            .sidebar button:hover { background: #2980b9; }
            .overview { display: flex; justify-content: center; padding: 20px; background: #0e1111; margin: 10px; border-radius: 10px; box-shadow: 0 4px 12px rgba(0,0,0,0.2); flex-wrap: wrap; }
            .card { background: #FAF9F6; padding: 10px; border-radius: 8px; text-align: center; width: 120px; box-shadow: 0 2px 6px rgba(0,0,0,0.1); transition: transform 0.2s; margin: 5px; }
            .card:hover { transform: translateY(-3px); }
            .card p { margin: 5px 0; font-size: 12px; color: #09141C; }
            .card span { font-size: 18px; color: #000; font-weight: bold; }
            .set-selector {
                position: absolute;
                top: 10px;
                left: 220px;
                padding: 5px 10px;
                border-radius: 5px;
                border: none;
                background: #3498db;
                color: #000;
                cursor: pointer;
            }
            .set-selector:hover {
                background: #2980b9;
            }
        </style>
    </head>
    <body>
        <div class="header">
            <h1>Business Analytics Portal</h1>
            <div class="controls">
                <select><option>Today</option><option>Week</option><option>Month</option></select>
                <button onclick="refreshData()">Refresh</button>
            </div>
        </div>
        {% if available_sets %}
        <select class="set-selector" onchange="loadSet(this.value)">
            <option value="">Select Analysis Set</option>
            {% for set_num in available_sets %}
            <option value="{{ set_num }}">Set {{ set_num }}</option>
            {% endfor %}
        </select>
        {% endif %}
        <button class="toggle-sidebar" onclick="toggleSidebar()">▶</button>
        <div class="sidebar" id="sidebar">
            <form method="post" enctype="multipart/form-data">
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
        <script>
            function refreshData() {
                location.reload();
            }

            function toggleSidebar() {
                var sidebar = document.getElementById('sidebar');
                var container = document.getElementById('container');
                var toggleBtn = document.querySelector('.toggle-sidebar');
                sidebar.classList.toggle('hidden');
                container.classList.toggle('sidebar-visible');
                toggleBtn.textContent = sidebar.classList.contains('hidden') ? '▶' : '◀';
            }

            function loadSet(setNumber) {
                if (setNumber) {
                    var form = document.createElement('form');
                    form.method = 'POST';
                    form.action = '/';
                    
                    var input = document.createElement('input');
                    input.type = 'hidden';
                    input.name = 'load_set';
                    input.value = setNumber;
                    
                    form.appendChild(input);
                    document.body.appendChild(form);
                    form.submit();
                }
            }

            window.onload = function() {
                var sidebar = document.getElementById('sidebar');
                var container = document.getElementById('container');
                if (window.innerWidth > 1000) {
                    container.classList.add('sidebar-visible');
                }
            };
        </script>
    </body>
    </html>
    """, available_sets=available_sets)

if __name__ == '__main__':
    app.run(debug=True, port=8005)