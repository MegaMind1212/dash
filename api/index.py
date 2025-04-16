from flask import Flask, render_template_string, request
from flask_session import Session
import os
import worker

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

@app.route('/', methods=['GET', 'POST'])
def dashboard():
    if 'analysis_sessions' not in session:
        session['analysis_sessions'] = []
        session['current_set'] = None
    filtered_data = None
    filter_type = None
    filter_value = None
    if request.method == 'POST':
        if 'file_upload' in request.form:
            deals_file = request.files.get('deals_file')
            dealers_file = request.files.get('dealers_file')
            users_file = request.files.get('users_file')
            deals_full_file = request.files.get('deals_full_file')
            if all([deals_file, dealers_file, users_file, deals_full_file]):
                set_number = worker.get_next_set_number()
                set_folder = os.path.join(app.config['UPLOAD_FOLDER'], f'set_{set_number}')
                os.makedirs(set_folder, exist_ok=True)
                deals_path = os.path.join(set_folder, worker.secure_filename(deals_file.filename))
                dealers_path = os.path.join(set_folder, worker.secure_filename(dealers_file.filename))
                users_path = os.path.join(set_folder, worker.secure_filename(users_file.filename))
                deals_full_path = os.path.join(set_folder, worker.secure_filename(deals_full_file.filename))
                deals_file.save(deals_path)
                dealers_file.save(dealers_path)
                users_file.save(users_path)
                deals_full_file.save(deals_full_path)
                analysis_data, error = worker.perform_analysis(deals_path, dealers_path, users_path, deals_full_path)
                if error:
                    return error
                session['analysis_sessions'].append({'set_number': set_number, 'analysis': analysis_data})
                session['current_set'] = set_number
                session.modified = True
        elif 'load_session' in request.form:
            set_number = int(request.form['set_number'])
            session['current_set'] = set_number
            session.modified = True
        elif 'filter' in request.form:
            filter_type = request.form.get('filter_type')
            filter_value = request.form.get('filter_value')
            if session['current_set']:
                for s in session['analysis_sessions']:
                    if s['set_number'] == session['current_set']:
                        current_analysis = s['analysis']
                        if filter_type == 'deal_date':
                            filtered_data = worker.filter_deals_by_date(current_analysis['deals_df'], current_analysis['deals_full_df'], filter_value)
                        elif filter_type == 'dealer_category':
                            filtered_data = worker.filter_dealers_by_category(current_analysis['dealers_df'], filter_value)
                        elif filter_type == 'dealer_pincode':
                            filtered_data = worker.filter_dealers_by_pincode(current_analysis['dealers_df'], filter_value)
                        elif filter_type == 'deals_pincode':
                            filtered_data = worker.filter_deals_by_pincode(current_analysis['deals_df'], current_analysis['deals_full_df'], current_analysis['dealers_df'], filter_value)
                        break
    current_analysis = next((s['analysis'] for s in session['analysis_sessions'] if s['set_number'] == session['current_set']), None)
    html_template = """
    <!DOCTYPE html>
    <html>
    <head><title>Business Analytics Portal</title><script src="https://cdn.plot.ly/plotly-latest.min.js"></script><style>body{margin:0;font-family:'Segoe UI',Tahoma,Geneva,Verdana,sans-serif;background:#0e1111;color:#F0F0F0}.header{background:#0e1111;padding:15px;text-align:center;box-shadow:0 4px 12px rgba(0,0,0,0.3)}.header h1{margin:0;font-size:24px;color:#F0F0F0}.controls{position:absolute;top:10px;right:20px}.controls select,.controls button{padding:5px 10px;margin-left:10px;border-radius:5px;border:none;background:#3498db;color:#000;cursor:pointer}.controls button:hover{background:#2980b9}.sidebar{position:fixed;top:60px;left:0;width:200px;height:calc(100%-60px);background:#0e1111;padding:20px;box-shadow:2px 0 12px rgba(0,0,0,0.3);transition:transform 0.3s ease;z-index:1000}.sidebar.hidden{transform:translateX(-100%)}.toggle-sidebar{position:absolute;top:10px;right:-20px;width:20px;height:20px;background:#3498db;border:none;border-radius:0 5px 5px 0;cursor:pointer;font-size:12px;color:#000;line-height:20px;text-align:center}.toggle-sidebar:hover{background:#2980b9}.sidebar form{display:flex;flex-direction:column}.sidebar label{margin:10px 0 5px;font-weight:bold;font-size:14px;color:#F0F0F0}.sidebar input[type=file],.sidebar button{padding:8px;margin:5px 0;border-radius:5px;border:none;background:#3498db;color:#000;cursor:pointer;width:100%;transition:background 0.2s}.sidebar button:hover{background:#2980b9}.overview{display:flex;justify-content:center;padding:20px;background:#0e1111;margin:10px 10px 10px 220px;border-radius:10px;box-shadow:0 4px 12px rgba(0,0,0,0.2);flex-wrap:wrap}.overview.no-sidebar{margin-left:10px}.card{background:#FAF9F6;padding:10px;border-radius:8px;text-align:center;width:120px;box-shadow:0 2px 6px rgba(0,0,0,0.1);transition:transform 0.2s;position:relative;margin:5px}.card:hover{transform:translateY(-3px)}.card p{margin:5px 0;font-size:12px;color:#09141C}.card span{font-size:18px;color:#000;font-weight:bold}.info-icon{position:absolute;top:5px;right:5px;font-size:14px;cursor:pointer;color:#3498db}.info-tooltip{display:none;position:absolute;top:25px;right:5px;background:#fff;color:#000;padding:5px 10px;border-radius:5px;box-shadow:0 2px 6px rgba(0,0,0,0.1);z-index:1000;width:200px;font-size:12px}.card:hover .info-tooltip{display:block}.container{display:flex;flex-wrap:wrap;justify-content:center;gap:15px;padding:20px;margin-left:220px;max-width:1400px;margin-right:auto;transition:margin-left 0.3s ease}.container.no-sidebar{margin-left:0}.map-box,.graph-box{background:#0e1111;border-radius:10px;overflow:hidden;box-shadow:0 4px 12px rgba(0,0,0,0.2);text-align:center}.map-box{height:400px;width:45%;min-width:300px}.graph-box{height:350px;width:45%;min-width:300px;position:relative}.map-title,.graph-title{padding:10px;background:#FAF9F6;text-align:center;font-size:16px;font-weight:bold;color:#09141C}.fullscreen-btn{position:absolute;top:10px;right:10px;padding:5px 10px;background:#3498db;border:none;border-radius:5px;color:#000;cursor:pointer}.fullscreen-btn:hover{background:#2980b9}.level2-section{width:90%;margin:20px auto;padding:20px;background:#0e1111;border-radius:10px;box-shadow:0 4px 12px rgba(0,0,0,0.2)}.level2-section h2{text-align:center;color:#F0F0F0}.filter-form{display:flex;gap:10px;margin-bottom:20px;justify-content:center;flex-wrap:wrap}.filter-form select,.filter-form input,.filter-form button{padding:8px;border-radius:5px;border:none;background:#3498db;color:#000}.filter-form button:hover{background:#2980b9}.data-table{width:100%;border-collapse:collapse;background:#FAF9F6;color:#09141C}.data-table th,.data-table td{border:1px solid #ddd;padding:8px;text-align:left}.data-table th{background:#3498db;color:#000}.data-table tr:nth-child(even){background:#f2f2f2}.data-table tr:hover{background:#ddd}.export-btn{display:block;margin:10px auto;padding:8px 16px;background:#3498db;color:#000;border:none;border-radius:5px;cursor:pointer}.export-btn:hover{background:#2980b9}@media (max-width:1000px){.container,.overview{margin-left:0}.sidebar{position:static;width:100%;height:auto;transform:none}.toggle-sidebar{display:none}.map-box,.graph-box{width:90%}.level2-section{width:95%}}</style></head>
    <body>
        <div class="header"><h1>Business Analytics Portal</h1><div style="font-size:12px;color:red;font-style:italic;font-weight:300;margin-top:5px">(Under Testing)</div><div class="controls"><select onchange="loadSession(this.value)"><option value="">Select Previous Set</option>{% for s in analysis_sessions %}<option value="{{ s.set_number }}" {% if s.set_number == current_set %}selected{% endif %}>Set {{ s.set_number }}</option>{% endfor %}</select><button onclick="refreshData()">Refresh</button></div></div>
        <button class="toggle-sidebar" onclick="toggleSidebar()">▶</button>
        <div class="sidebar" id="sidebar"><form method="post" enctype="multipart/form-data"><input type="hidden" name="file_upload" value="true"><label for="users_file">Updated Users CSV</label><input type="file" name="users_file" id="users_file" accept=".csv" required><label for="deals_full_file">Deals Full Dump CSV</label><input type="file" name="deals_full_file" id="deals_full_file" accept=".csv" required><label for="deals_file">Deals vs Dealers CSV</label><input type="file" name="deals_file" id="deals_file" accept=".csv" required><label for="dealers_file">Dealer Onboarded CSV</label><input type="file" name="dealers_file" id="dealers_file" accept=".csv" required><button type="submit">Analyze</button></form></div>
        {% if current_analysis %}
            <div class="overview"><div class="card"><span class="info-icon">(i)</span><div class="info-tooltip">Calculated by counting the total number of unique user records available.</div><p>Total Users</p><span>{{ current_analysis.total_users }}</span></div><div class="card"><span class="info-icon">(i)</span><div class="info-tooltip">Determined by adding the total number of rows from the deal request datasets, where each row represents a visit or interaction.</div><p>Total Visits</p><span>{{ current_analysis.total_visits }}</span></div><div class="card"><span class="info-icon">(i)</span><div class="info-tooltip">Computed by counting users whose creation timestamp is within the last 30 days from the current date.</div><p>New Users</p><span>{{ current_analysis.new_users }}</span></div><div class="card"><span class="info-icon">(i)</span><div class="info-tooltip">Derived by counting the number of unique user IDs that have made at least one deal request.</div><p>Active Users</p><span>{{ current_analysis.active_users }}</span></div></div>
            <div class="overview" style="margin-top:10px"><div class="card"><span class="info-icon">(i)</span><div class="info-tooltip">Calculated by summing the total number of rows from both deal request datasets, where each row represents a deal made.</div><p>Total Deals</p><span>{{ current_analysis.total_deals }}</span></div><div class="card"><span class="info-icon">(i)</span><div class="info-tooltip">Determined by combining all deal records, removing duplicates based on user ID and request quantity to count unique deals.</div><p>Unique Deals</p><span>{{ current_analysis.unique_deals }}</span></div><div class="card"><span class="info-icon">(i)</span><div class="info-tooltip">Computed as the percentage of unique deals relative to new users, calculated as (unique deals / new users) * 100.</div><p>New User to Deal Ratio</p><span>{{ '{:.2f}%'.format(current_analysis.new_user_deal_ratio) }}</span></div><div class="card"><span class="info-icon">(i)</span><div class="info-tooltip">Derived by counting unique deals with at least one response (request quantity greater than 0), then dividing by the total unique deals and multiplying by 100.</div><p>Unique Deals vs Response Ratio</p><span>{{ '{:.2f}%'.format(current_analysis.response_ratio) }}</span></div></div>
            <div class="container" id="container"><div class="map-box"><div class="map-title">Users Map</div>{{ current_analysis.users_map|safe }}</div><div class="map-box"><div class="map-title">Dealers Map</div>{{ current_analysis.dealers_map|safe }}</div><div class="map-box"><div class="map-title">Relational Map</div>{{ current_analysis.relation_map|safe }}</div>{% if current_analysis.new_users_map %}<div class="map-box"><div class="map-title">New Users Map</div>{{ current_analysis.new_users_map|safe }}</div>{% endif %}<div class="graph-box"><div class="graph-title">Users per Pincode</div><button class="fullscreen-btn" onclick="toggleFullscreen('graph1')">Fullscreen</button><div id="graph1" style="width:100%;height:300px;"></div></div><div class="graph-box"><div class="graph-title">Dealers per Pincode</div><button class="fullscreen-btn" onclick="toggleFullscreen('graph2')">Fullscreen</button><div id="graph2" style="width:100%;height:300px;"></div></div><div class="graph-box"><div class="graph-title">Deal Requests per User</div><button class="fullscreen-btn" onclick="toggleFullscreen('graph3')">Fullscreen</button><div id="graph3" style="width:100%;height:300px;"></div></div><div class="graph-box"><div class="graph-title">Dealer Product Categories</div><button class="fullscreen-btn" onclick="toggleFullscreen('graph4')">Fullscreen</button><div id="graph4" style="width:100%;height:300px;"></div></div><div class="graph-box"><div class="graph-title">New Users Onboarding Timeline</div><button class="fullscreen-btn" onclick="toggleFullscreen('graph5')">Fullscreen</button><div id="graph5" style="width:100%;height:300px;"></div></div><div class="level2-section"><h2>Level 2 Dashboard: Record-Level Intelligence</h2><form class="filter-form" method="post"><input type="hidden" name="filter" value="true"><select name="filter_type"><option value="deal_date">Deals by Date</option><option value="dealer_category">Dealers by Category</option><option value="dealer_pincode">Dealers by Pincode</option><option value="deals_pincode">Deals by Pincode</option></select><input type="text" name="filter_value" placeholder="e.g., 2025-04-01 or Kitchen" required><button type="submit">Filter</button></form>{% if filtered_data %}<table class="data-table"><thead><tr>{% if filter_type == 'deal_date' %}<th>User ID</th><th>User Name</th><th>Pincode</th><th>Request Quantity</th><th>Deal Date</th>{% elif filter_type == 'dealer_category' or filter_type == 'dealer_pincode' %}<th>Dealer Name</th><th>Pincode</th><th>Phone</th><th>Categories</th><th>Subcategories</th>{% elif filter_type == 'deals_pincode' %}<th>User ID</th><th>User Name</th><th>Dealer Name</th><th>Pincode</th><th>Request Quantity</th><th>Deal Date</th>{% endif %}</tr></thead><tbody>{% for row in filtered_data %}<tr>{% if filter_type == 'deal_date' %}<td>{{ row.user_id }}</td><td>{{ row.user_name }}</td><td>{{ row.pincode }}</td><td>{{ row.req_qty }}</td><td>{{ row.deal_date }}</td>{% elif filter_type == 'dealer_category' or filter_type == 'dealer_pincode' %}<td>{{ row.coname }}</td><td>{{ row.pincode }}</td><td>{{ row.phone_no }}</td><td>{{ row.cat_disp_names }}</td><td>{{ row.subcat_disp_names }}</td>{% elif filter_type == 'deals_pincode' %}<td>{{ row.user_id }}</td><td>{{ row.user_name }}</td><td>{{ row.dealer_name }}</td><td>{{ row.pincode }}</td><td>{{ row.req_qty }}</td><td>{{ row.deal_date }}</td>{% endif %}</tr>{% endfor %}</tbody></table><button class="export-btn" onclick="exportTableToCSV()">Export to CSV</button>{% endif %}</div></div>
        {% else %}
            <div class="overview no-sidebar"><div class="card"><p>Total Users</p><span>0</span></div><div class="card"><p>Total Visits</p><span>0</span></div><div class="card"><p>New Users</p><span>0</span></div><div class="card"><p>Active Users</p><span>0</span></div></div>
        {% endif %}
        <script>
            {% if current_analysis %}var graphData={{ current_analysis.graph1|tojson|default('{"data":[],"layout":{}}')|safe }},{{ current_analysis.graph2|tojson|default('{"data":[],"layout":{}}')|safe }},{{ current_analysis.graph3|tojson|default('{"data":[],"layout":{}}')|safe }},{{ current_analysis.graph4|tojson|default('{"data":[],"layout":{}}')|safe }},{{ current_analysis.graph5|tojson|default('{"data":[],"layout":{}}')|safe }};{% else %}var graphData={"graph1":{"data":[],"layout":{}},"graph2":{"data":[],"layout":{}},"graph3":{"data":[],"layout":{}},"graph4":{"data":[],"layout":{}},"graph5":{"data":[],"layout":{}}};{% endif %}
            function renderGraphs(){if(!graphData||Object.keys(graphData).length===0){console.warn('No graph data available');return}try{['graph1','graph2','graph3','graph4','graph5'].forEach(function(graphId){var data=graphData[graphId];if(data&&data.data&&Array.isArray(data.data)){Plotly.newPlot(graphId,data.data,data.layout||{}, {responsive: true})}else{console.warn('Invalid data for '+graphId+':',data);Plotly.newPlot(graphId,[],{}, {responsive: true})}})}catch(e){console.error('Error rendering graphs:',e,'Graph data:',graphData)}}
            function toggleFullscreen(graphId){var graphDiv=document.getElementById(graphId);if(graphDiv){if(graphDiv.requestFullscreen){graphDiv.requestFullscreen()}else if(graphDiv.mozRequestFullScreen){graphDiv.mozRequestFullScreen()}else if(graphDiv.webkitRequestFullscreen){graphDiv.webkitRequestFullscreen()}else if(graphDiv.msRequestFullscreen){graphDiv.msRequestFullscreen()}Plotly.relayout(graphId,{autosize:true})}else{console.error('Graph div not found:',graphId)}}
            function refreshData(){location.reload()}
            function loadSession(setNumber){if(setNumber){var form=document.createElement('form');form.method='POST';form.action='/';var input=document.createElement('input');input.type='hidden';input.name='load_session';input.value='true';form.appendChild(input);var setInput=document.createElement('input');setInput.type='hidden';setInput.name='set_number';setInput.value=setNumber;form.appendChild(setInput);document.body.appendChild(form);form.submit()}}
            function toggleSidebar(){var sidebar=document.getElementById('sidebar'),container=document.getElementById('container'),overviews=document.querySelectorAll('.overview'),toggleBtn=document.querySelector('.toggle-sidebar');if(sidebar&&container&&toggleBtn){sidebar.classList.toggle('hidden');container.classList.toggle('no-sidebar');overviews.forEach(overview=>overview.classList.toggle('no-sidebar'));toggleBtn.textContent=sidebar.classList.contains('hidden')?'▶':'◀';setTimeout(renderGraphs,300)}}else{console.error('Sidebar, container, or toggle button not found')}}
            function exportTableToCSV(){var table=document.querySelector('.data-table');if(!table){alert('No data to export.');console.warn('No data-table element found');return}var rows=table.querySelectorAll('tr'),csv=[];for(var i=0;i<rows.length;i++){var row=[],cols=rows[i].querySelectorAll('td,th');for(var j=0;j<cols.length;j++)row.push('"'+cols[j].innerText.replace(/"/g,'""')+'"');csv.push(row.join(','))}var csvContent='data:text/csv;charset=utf-8,'+csv.join('\n'),encodedUri=encodeURI(csvContent),link=document.createElement('a');link.setAttribute('href',encodedUri);link.setAttribute('download','filtered_data.csv');document.body.appendChild(link);link.click();document.body.removeChild(link)}
            window.onload=function(){var sidebar=document.getElementById('sidebar'),container=document.getElementById('container'),overviews=document.querySelectorAll('.overview');if(window.innerWidth<=1000){if(sidebar&&container){sidebar.classList.add('hidden');container.classList.add('no-sidebar');overviews.forEach(overview=>overview.classList.add('no-sidebar'))}}renderGraphs()}
            window.onresize=function(){renderGraphs()}
        </script>
    </body>
    </html>
    """
    return render_template_string(html_template, 
                                 current_analysis=current_analysis, 
                                 analysis_sessions=session.get('analysis_sessions', []),
                                 current_set=session.get('current_set'),
                                 filtered_data=filtered_data,
                                 filter_type=filter_type,
                                 filter_value=filter_value)

if __name__ == '__main__':
    app.run(debug=True, host='0.0.0.0', port=8001)