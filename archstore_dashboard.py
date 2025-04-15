from flask import Flask, request, render_template_string, flash, redirect, url_for, session, jsonify
import pandas as pd
import folium
from folium.plugins import MarkerCluster
import plotly.express as px
import os
import logging

# Set up logging
logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)

app = Flask(__name__)
app.secret_key = "supersecretkey"  # Required for session and flash messages

# In-memory storage for the data
data_store = {
    'users': None,
    'deals': None,
    'dealers': None,
    'deals_vs_dealers': None
}

# Mumbai center coordinates for the map
MUMBAI_CENTER = [19.0760, 72.8777]

# HTML template for the dashboard with improved UI
HTML_TEMPLATE = '''
<!DOCTYPE html>
<html>
<head>
    <title>Archstore Dashboard</title>
    <style>
        body { font-family: Arial, sans-serif; margin: 20px; }
        .container { max-width: 1200px; margin: auto; }
        .upload-form { margin-bottom: 20px; }
        .upload-form label { display: block; margin: 10px 0 5px; font-weight: bold; }
        .upload-form input[type="file"] { margin-bottom: 10px; }
        .stats { display: flex; gap: 20px; margin-bottom: 20px; }
        .stat-box { padding: 10px; border: 1px solid #ccc; border-radius: 5px; }
        iframe { width: 100%; height: 500px; border: none; }
        .message { padding: 10px; margin: 10px 0; border-radius: 5px; }
        .success { background-color: #d4edda; color: #155724; }
        .error { background-color: #f8d7da; color: #721c24; }
        .btn { padding: 10px 20px; background-color: #007bff; color: white; border: none; border-radius: 5px; cursor: pointer; }
        .btn:hover { background-color: #0056b3; }
        .uploaded-files { margin-top: 10px; padding: 10px; border: 1px solid #ccc; border-radius: 5px; }
        .uploaded-files ul { list-style-type: none; padding: 0; }
        .uploaded-files li { margin: 5px 0; }
    </style>
    <script>
        // Prevent file input fields from resetting after form submission
        document.addEventListener('DOMContentLoaded', function() {
            const form = document.querySelector('form');
            if (form) {
                form.addEventListener('submit', function(event) {
                    // Prevent default form submission behavior
                    event.preventDefault();
                    
                    // Create a FormData object to handle the file uploads
                    const formData = new FormData(form);
                    
                    // Submit the form via fetch to avoid page reload
                    fetch('/upload', {
                        method: 'POST',
                        body: formData
                    })
                    .then(response => {
                        // Check if the response is JSON
                        const contentType = response.headers.get('content-type');
                        if (!contentType || !contentType.includes('application/json')) {
                            throw new Error('Server returned a non-JSON response. Check the server logs for errors.');
                        }
                        return response.json();
                    })
                    .then(data => {
                        if (data.success) {
                            // Display success message
                            const messageDiv = document.createElement('div');
                            messageDiv.className = 'message success';
                            messageDiv.textContent = data.message;
                            document.querySelector('.upload-form').after(messageDiv);
                            
                            // Show the "Generate/Update Dashboard" button
                            const generateButton = document.createElement('form');
                            generateButton.method = 'post';
                            generateButton.action = '/generate';
                            generateButton.innerHTML = '<input type="submit" class="btn" value="Generate/Update Dashboard">';
                            document.querySelector('.upload-form').after(generateButton);
                            
                            // Update the "Uploaded Files" section
                            const uploadedFilesDiv = document.querySelector('.uploaded-files') || document.createElement('div');
                            uploadedFilesDiv.className = 'uploaded-files';
                            uploadedFilesDiv.innerHTML = '<h3>Uploaded Files:</h3><ul>' +
                                '<li>Users File: ' + formData.get('users_file').name + '</li>' +
                                '<li>Deals File: ' + formData.get('deals_file').name + '</li>' +
                                '<li>Dealers File: ' + formData.get('dealers_file').name + '</li>' +
                                '<li>Deals vs Dealers File: ' + formData.get('deals_vs_dealers_file').name + '</li>' +
                                '</ul>';
                            document.querySelector('.upload-form').after(uploadedFilesDiv);
                        } else {
                            // Display error message
                            const messageDiv = document.createElement('div');
                            messageDiv.className = 'message error';
                            messageDiv.textContent = data.message;
                            document.querySelector('.upload-form').after(messageDiv);
                        }
                    })
                    .catch(error => {
                        console.error('Error uploading files:', error);
                        const messageDiv = document.createElement('div');
                        messageDiv.className = 'message error';
                        messageDiv.textContent = 'Error uploading files: ' + error.message + '. Please check the server logs for more details.';
                        document.querySelector('.upload-form').after(messageDiv);
                    });
                });
            }
        });
    </script>
</head>
<body>
    <div class="container">
        <h1>Archstore Dashboard</h1>
        <div class="upload-form">
            <form method="post" enctype="multipart/form-data" action="/upload">
                <label for="users_file">Upload Updated Users List (e.g., Updated Users List 2ndApr25.csv):</label>
                <input type="file" name="users_file" id="users_file" accept=".csv" required>
                
                <label for="deals_file">Upload Deals Full Dump (e.g., deals full dump 2ndApr25.csv):</label>
                <input type="file" name="deals_file" id="deals_file" accept=".csv" required>
                
                <label for="dealers_file">Upload Dealer Onboarded Report (e.g., Dealer Onboarded Report 2ndApr25.csv):</label>
                <input type="file" name="dealers_file" id="dealers_file" accept=".csv" required>
                
                <label for="deals_vs_dealers_file">Upload Deals vs Dealers in Depth (e.g., Deals vs Dealers in depth 2ndApr25.csv):</label>
                <input type="file" name="deals_vs_dealers_file" id="deals_vs_dealers_file" accept=".csv" required>
                
                <input type="submit" class="btn" value="Upload Files">
            </form>
            {% if uploaded_files %}
                <div class="uploaded-files">
                    <h3>Uploaded Files:</h3>
                    <ul>
                        {% for file_type, file_name in uploaded_files.items() %}
                            <li>{{ file_type }}: {{ file_name }}</li>
                        {% endfor %}
                    </ul>
                </div>
            {% endif %}
        </div>
        {% with messages = get_flashed_messages(with_categories=true) %}
            {% if messages %}
                {% for category, message in messages %}
                    <div class="message {{ category }}">{{ message }}</div>
                {% endfor %}
            {% endif %}
        {% endwith %}
        {% if data_uploaded %}
            <form method="post" action="/generate">
                <input type="submit" class="btn" value="Generate/Update Dashboard">
            </form>
        {% else %}
            <p>If you don't see the "Generate/Update Dashboard" button after uploading, please refresh the page or check the console logs for errors.</p>
        {% endif %}
        {% if map_html %}
            <h2>Mumbai Footprint</h2>
            <iframe srcdoc="{{ map_html | safe }}"></iframe>
            <h2>Statistics</h2>
            <div class="stats">
                <div class="stat-box">Total Users: {{ total_users }}</div>
                <div class="stat-box">Total Dealers: {{ total_dealers }}</div>
                <div class="stat-box">Total Deals: {{ total_deals }}</div>
                <div class="stat-box">Deals Accepted: {{ deals_accepted }}</div>
            </div>
            <h2>Pincode Breakdown</h2>
            <div>{{ pincode_graph | safe }}</div>
            <h2>Deals by Category</h2>
            <div>{{ category_graph | safe }}</div>
            <h2>Deal Details</h2>
            <div>{{ details_table | safe }}</div>
        {% endif %}
    </div>
</body>
</html>
'''

def process_files(files):
    try:
        # Load CSV files into DataFrames with error handling
        data_store['users'] = pd.read_csv(files['users_file'], na_values=['', 'NaN', 'nan', '#N/A'], keep_default_na=False) if 'users_file' in files else data_store['users']
        data_store['deals'] = pd.read_csv(files['deals_file'], na_values=['', 'NaN', 'nan', '#N/A'], keep_default_na=False) if 'deals_file' in files else data_store['deals']
        data_store['dealers'] = pd.read_csv(files['dealers_file'], na_values=['', 'NaN', 'nan', '#N/A'], keep_default_na=False) if 'dealers_file' in files else data_store['dealers']
        data_store['deals_vs_dealers'] = pd.read_csv(files['deals_vs_dealers_file'], na_values=['', 'NaN', 'nan', '#N/A'], keep_default_na=False) if 'deals_vs_dealers_file' in files else data_store['deals_vs_dealers']
        
        # Log the initial shapes and column names of the DataFrames
        logger.debug(f"Users DataFrame shape: {data_store['users'].shape}")
        logger.debug(f"Users DataFrame columns: {list(data_store['users'].columns)}")
        logger.debug(f"Deals DataFrame shape: {data_store['deals'].shape}")
        logger.debug(f"Deals DataFrame columns: {list(data_store['deals'].columns)}")
        logger.debug(f"Dealers DataFrame shape: {data_store['dealers'].shape}")
        logger.debug(f"Dealers DataFrame columns: {list(data_store['dealers'].columns)}")
        logger.debug(f"Deals vs Dealers DataFrame shape: {data_store['deals_vs_dealers'].shape}")
        logger.debug(f"Deals vs Dealers DataFrame columns: {list(data_store['deals_vs_dealers'].columns)}")
        
        # Clean data: Drop rows with missing critical columns
        if data_store['users'] is not None:
            if 'phone' not in data_store['users'].columns or 'pincode' not in data_store['users'].columns:
                logger.error("Users DataFrame missing required columns: 'phone' or 'pincode'")
                return False, "Users DataFrame missing required columns: 'phone' or 'pincode'"
            data_store['users'] = data_store['users'].dropna(subset=['phone', 'pincode'])
            logger.debug(f"Users DataFrame shape after cleaning: {data_store['users'].shape}")
            logger.debug(f"Sample Users DataFrame:\n{data_store['users'].head().to_string()}")
        
        if data_store['deals'] is not None:
            if 'phone' not in data_store['deals'].columns or 'deal_name' not in data_store['deals'].columns or '_id' not in data_store['deals'].columns:
                logger.error("Deals DataFrame missing required columns: 'phone', 'deal_name', or '_id'")
                return False, "Deals DataFrame missing required columns: 'phone', 'deal_name', or '_id'"
            data_store['deals'] = data_store['deals'].dropna(subset=['phone', 'deal_name', '_id'])
            logger.debug(f"Deals DataFrame shape after cleaning: {data_store['deals'].shape}")
            logger.debug(f"Sample Deals DataFrame:\n{data_store['deals'].head().to_string()}")
        
        if data_store['dealers'] is not None:
            if '_id' not in data_store['dealers'].columns or 'pincode' not in data_store['dealers'].columns or 'lat' not in data_store['dealers'].columns or 'long' not in data_store['dealers'].columns:
                logger.error("Dealers DataFrame missing required columns: '_id', 'pincode', 'lat', or 'long'")
                return False, "Dealers DataFrame missing required columns: '_id', 'pincode', 'lat', or 'long'"
            data_store['dealers'] = data_store['dealers'].dropna(subset=['_id', 'pincode', 'lat', 'long'])
            logger.debug(f"Dealers DataFrame shape after cleaning: {data_store['dealers'].shape}")
            logger.debug(f"Sample Dealers DataFrame:\n{data_store['dealers'].head().to_string()}")
        
        if data_store['deals_vs_dealers'] is not None:
            if 'user_phone' not in data_store['deals_vs_dealers'].columns or 'deal_id' not in data_store['deals_vs_dealers'].columns or 'Dealer_id' not in data_store['deals_vs_dealers'].columns or 'dealerinfo.phone_no' not in data_store['deals_vs_dealers'].columns:
                logger.error("Deals vs Dealers DataFrame missing required columns: 'user_phone', 'deal_id', 'Dealer_id', or 'dealerinfo.phone_no'")
                return False, "Deals vs Dealers DataFrame missing required columns: 'user_phone', 'deal_id', 'Dealer_id', or 'dealerinfo.phone_no'"
            data_store['deals_vs_dealers'] = data_store['deals_vs_dealers'].dropna(subset=['user_phone', 'deal_id', 'Dealer_id', 'dealerinfo.phone_no'])
            logger.debug(f"Deals vs Dealers DataFrame shape after cleaning: {data_store['deals_vs_dealers'].shape}")
            logger.debug(f"Sample Deals vs Dealers DataFrame:\n{data_store['deals_vs_dealers'].head().to_string()}")
        
        return True, "Files processed successfully"
    except Exception as e:
        logger.error(f"Error processing files: {str(e)}")
        return False, f"Error processing files: {str(e)}"

def create_mumbai_map():
    # Check if users and dealers DataFrames exist and are not empty
    if data_store['users'] is None or data_store['users'].empty or data_store['dealers'] is None or data_store['dealers'].empty:
        logger.warning("Users or Dealers data is missing or empty. Cannot create Mumbai map.")
        return None
    
    logger.debug("Creating Mumbai map...")
    m = folium.Map(location=MUMBAI_CENTER, zoom_start=11)
    marker_cluster = MarkerCluster().add_to(m)
    
    user_pincode_counts = data_store['users']['pincode'].value_counts().to_dict()
    dealer_pincode_counts = data_store['dealers']['pincode'].value_counts().to_dict()
    
    markers_added = 0
    for pincode in set(list(user_pincode_counts.keys()) + list(dealer_pincode_counts.keys())):
        dealers_in_pincode = data_store['dealers'][data_store['dealers']['pincode'] == pincode]
        if not dealers_in_pincode.empty:
            lat = dealers_in_pincode['lat'].iloc[0]
            lon = dealers_in_pincode['long'].iloc[0]
        else:
            lat, lon = MUMBAI_CENTER
        
        if pd.isna(lat) or pd.isna(lon) or lat == 0 or lon == 0:
            logger.warning(f"Invalid lat/lon for pincode {pincode}: lat={lat}, lon={lon}. Skipping marker.")
            continue
            
        users = user_pincode_counts.get(pincode, 0)
        dealers = dealer_pincode_counts.get(pincode, 0)
        popup_text = f"Pincode: {pincode}<br>Users: {users}<br>Dealers: {dealers}"
        folium.Marker([lat, lon], popup=popup_text).add_to(marker_cluster)
        markers_added += 1
    
    if markers_added == 0:
        logger.warning("No valid markers added to the map. Map will not be displayed.")
        return None
    
    logger.debug("Mumbai map created successfully.")
    return m._repr_html_()

def create_pincode_graph():
    if data_store['users'] is None or data_store['users'].empty or data_store['dealers'] is None or data_store['dealers'].empty:
        logger.warning("Users or Dealers data is missing or empty. Cannot create pincode graph.")
        return None
    
    logger.debug("Creating pincode graph...")
    user_counts = data_store['users']['pincode'].value_counts().reset_index()
    user_counts.columns = ['pincode', 'count_users']
    dealer_counts = data_store['dealers']['pincode'].value_counts().reset_index()
    dealer_counts.columns = ['pincode', 'count_dealers']
    merged = pd.merge(user_counts, dealer_counts, on='pincode', how='outer').fillna(0)
    
    fig = px.bar(merged, x='pincode', y=['count_users', 'count_dealers'], barmode='group',
                 labels={'count_users': 'Users', 'count_dealers': 'Dealers'}, title='Users and Dealers by Pincode')
    logger.debug("Pincode graph created successfully.")
    return fig.to_html(full_html=False)

def create_category_graph():
    if data_store['deals_vs_dealers'] is None or data_store['deals_vs_dealers'].empty:
        logger.warning("Deals vs Dealers data is missing or empty. Cannot create category graph.")
        return None
    
    logger.debug("Creating category graph...")
    category_counts = data_store['deals_vs_dealers']['cat_disp_name'].value_counts().reset_index()
    category_counts.columns = ['Category', 'Count']
    
    fig = px.bar(category_counts, x='Category', y='Count', title='Deals by Category')
    logger.debug("Category graph created successfully.")
    return fig.to_html(full_html=False)

def create_details_table():
    # Check if all DataFrames exist and are not empty
    if not all(df is not None and not df.empty for df in data_store.values()):
        logger.warning("Some data is missing or empty. Cannot create details table.")
        return None
    
    logger.debug("Creating details table...")
    # Step 1: Merge Deals with Users on phone
    deals_with_users = data_store['deals'].merge(data_store['users'], 
                                                 left_on='phone', right_on='phone', 
                                                 how='left', suffixes=('_deal', '_user'))
    logger.debug(f"Deals with Users DataFrame shape: {deals_with_users.shape}")
    logger.debug(f"Columns after Deals+Users merge: {list(deals_with_users.columns)}")
    
    # Step 2: Merge with Deals vs Dealers on deal_id
    deals_with_dvd = deals_with_users.merge(data_store['deals_vs_dealers'], 
                                            left_on='_id', right_on='deal_id', 
                                            how='left')
    logger.debug(f"Deals with Deals vs Dealers DataFrame shape: {deals_with_dvd.shape}")
    logger.debug(f"Columns after Deals+DVD merge: {list(deals_with_dvd.columns)}")
    
    # Step 3: Merge with Dealers on Dealer_id
    merged = deals_with_dvd.merge(data_store['dealers'], 
                                  left_on='Dealer_id', right_on='_id', 
                                  how='left', suffixes=('_dvd', '_dealer'))
    logger.debug(f"Final merged DataFrame shape: {merged.shape}")
    logger.debug(f"Columns after final merge: {list(merged.columns)}")
    
    # Select available columns, adjusting for actual names
    available_columns = merged.columns
    display_columns = {
        'user_phone': 'User Phone',
        'name': 'User Name',  # From users, not 'user_name'
        'deal_name': 'Deal Name',
        'coname': 'Dealer Name',
        'pincode_user': 'User Pincode',  # Adjusted from 'user_pincode'
        'pincode_dealer': 'Dealer Pincode',  # Adjusted from 'pincode'
        'deal_stage': 'Deal Stage'
    }
    
    # Map actual column names to display names, using only what's available
    selected_cols = {}
    for col, display_name in display_columns.items():
        if col in available_columns:
            selected_cols[col] = display_name
        elif col == 'pincode_user' and 'pincode' in available_columns and 'pincode_user' not in available_columns:
            selected_cols['pincode'] = 'User Pincode'  # Fallback to 'pincode' from users
        elif col == 'pincode_dealer' and 'pincode_dealer' not in available_columns and 'pincode' in available_columns:
            selected_cols['pincode'] = 'Dealer Pincode'  # Fallback to 'pincode' from dealers with suffix
    
    if not selected_cols:
        logger.error("No matching columns found for details table.")
        return "<p>No data available for details table.</p>"
    
    sample = merged[list(selected_cols.keys())].head(5)
    sample.columns = [selected_cols[col] for col in sample.columns]
    logger.debug("Details table created successfully.")
    return sample.to_html(index=False)

def get_statistics():
    total_users = len(data_store['users']) if data_store['users'] is not None and not data_store['users'].empty else 0
    total_dealers = len(data_store['dealers']) if data_store['dealers'] is not None and not data_store['dealers'].empty else 0
    total_deals = len(data_store['deals']) if data_store['deals'] is not None and not data_store['deals'].empty else 0
    
    deals_accepted = 0
    if data_store['deals_vs_dealers'] is not None and not data_store['deals_vs_dealers'].empty:
        deals_accepted = len(data_store['deals_vs_dealers'][data_store['deals_vs_dealers']['deal_stage'] == 'Deal'])
    
    logger.debug(f"Statistics: Users={total_users}, Dealers={total_dealers}, Deals={total_deals}, Deals Accepted={deals_accepted}")
    return total_users, total_dealers, total_deals, deals_accepted

@app.route('/', methods=['GET'])
def index():
    data_uploaded = request.args.get('data_uploaded', 'False') == 'True'
    logger.debug(f"Rendering index page with data_uploaded={data_uploaded}")
    uploaded_files = session.get('uploaded_files', None)
    return render_template_string(HTML_TEMPLATE, data_uploaded=data_uploaded, uploaded_files=uploaded_files)

@app.route('/upload', methods=['POST'])
def upload():
    files = {
        'users_file': request.files['users_file'],
        'deals_file': request.files['deals_file'],
        'dealers_file': request.files['dealers_file'],
        'deals_vs_dealers_file': request.files['deals_vs_dealers_file']
    }
    
    # Store the names of the uploaded files in the session
    uploaded_files = {
        'Users File': files['users_file'].filename,
        'Deals File': files['deals_file'].filename,
        'Dealers File': files['dealers_file'].filename,
        'Deals vs Dealers File': files['deals_vs_dealers_file'].filename
    }
    session['uploaded_files'] = uploaded_files
    
    success, message = process_files(files)
    return jsonify({'success': success, 'message': message})

@app.route('/generate', methods=['POST'])
def generate():
    logger.debug("Generating dashboard...")
    map_html = create_mumbai_map() if data_store['users'] is not None and not data_store['users'].empty else None
    total_users, total_dealers, total_deals, deals_accepted = get_statistics()
    pincode_graph = create_pincode_graph() if data_store['users'] is not None and not data_store['users'].empty and data_store['dealers'] is not None and not data_store['dealers'].empty else None
    category_graph = create_category_graph() if data_store['deals_vs_dealers'] is not None and not data_store['deals_vs_dealers'].empty else None
    details_table = create_details_table() if all(df is not None and not df.empty for df in data_store.values()) else None
    
    if not any([map_html, pincode_graph, category_graph, details_table]):
        logger.warning("No dashboard components were generated. Check data availability and integrity.")
        flash("No dashboard components were generated. Please check the uploaded files for missing or invalid data.", "error")
    
    logger.debug("Dashboard generation complete.")
    return render_template_string(HTML_TEMPLATE, map_html=map_html, total_users=total_users,
                                 total_dealers=total_dealers, total_deals=total_deals,
                                 deals_accepted=deals_accepted, pincode_graph=pincode_graph,
                                 category_graph=category_graph, details_table=details_table,
                                 data_uploaded=True, uploaded_files=session.get('uploaded_files', None))

if __name__ == '__main__':
    app.run(debug=True, host='localhost', port=5001)