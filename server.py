from flask import Flask, request, jsonify, send_from_directory , flash , render_template ,  redirect, url_for, session, flash, jsonify
from werkzeug.utils import secure_filename
from flask_limiter import Limiter
from flask_login import LoginManager, UserMixin, login_user, logout_user, login_required, current_user
from flask_limiter.util import get_remote_address
from datetime import datetime , timedelta
import os
import itertools
import redis
import sys
import time
import threading
import base64
import logging
from waitress import serve
from threading import Lock
from logging.handlers import RotatingFileHandler
from db import PersistentTaskScheduler ,delete_user,retrieve_from_mongo,save_to_mongo, login_validation,get_user_devices,get_device_info,save_merge_info,get_user_by_username ,update_user_without_new_id,update_user_with_new_id,get_device_details_for_edit ,update_device_details  ,get_device_log_table_data,get_device_log_chart_data,get_device_details_db, get_user_details ,get_devices_by_user_id, add_device_and_update_user ,  save_user_info, load_dash, admin_dash_data_recive, get_ai, save_new_user, update_user_photo,get_all_users
from bson import ObjectId
from bson.errors import InvalidId
from functools import wraps
from PIL import Image
from io import BytesIO
RLOCK = Lock()
r = redis.StrictRedis(host='127.0.0.1', port=6379, db=0)
redis_url = "redis://localhost:6379/0"

app = Flask(__name__)
limiter = Limiter(
    get_remote_address,
    app=app,
    storage_uri=redis_url,
    default_limits=["200 per day", "50 per hour"]
)

server_logger = logging.getLogger("server_logger")
server_logger.setLevel(logging.INFO)  

file_handler = RotatingFileHandler(
    'logs/server.log', 
    maxBytes=1024 * 1024,  # 1 MB
    backupCount=5,  # Keep up to 5 backup logs
    encoding='utf-8'
)
file_handler.setFormatter(logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s'))

server_logger.addHandler(file_handler)
server_logger.propagate = False

def allowed_file_for_upload(filename , file_type):
    ALLOWED_EXTENSIONS_normal = {'png', 'jpg', 'jpeg'  , 'mp3' , 'wave' , 'wav',  'gif' , 'webp'}
    ALLOWED_EXTENSIONS_OTA = {'bin'}
    if file_type == 'normal':
        return '.' in filename and filename.rsplit('.', 1)[1].lower() in ALLOWED_EXTENSIONS_normal
    elif file_type == 'ota':
        return '.' in filename and filename.rsplit('.', 1)[1].lower() in ALLOWED_EXTENSIONS_OTA
    else:
        return False
    
def convert_datetime(obj):
    if isinstance(obj, datetime):
        return obj.isoformat()
    raise TypeError("Type not serializable")

def convert_to_datetime(data_dict):
    for key, value in data_dict.items():
        if isinstance(value, str):
            try:
                # Try converting ISO 8601 string to datetime
                data_dict[key] = datetime.fromisoformat(value)
            except ValueError:
                pass  # If conversion fails, keep the original string
    return data_dict
        
class SpinnerProgressBar:
    def __init__(self, total, bar_length=40, delay=0.1):
        self.spinner = itertools.cycle(['|', '/', '-', '\\'])
        self.total = total
        self.bar_length = bar_length
        self.delay = delay
        self.running = False
        self.progress = 0
        self.thread = None

    def start(self):
        self.running = True
        self.thread = threading.Thread(target=self.animate)
        self.thread.start()

    def animate(self):
        while self.running:
            fraction = self.progress / self.total
            arrow = int(fraction * self.bar_length) * '='
            padding = int(self.bar_length - len(arrow)) * ' '
            ending = '>' if self.progress < self.total else ''
            percentage = round(fraction * 100, 1)
            spinner_char = next(self.spinner)
            sys.stdout.write(f'\r[{arrow}{ending}{padding}] {percentage}% {spinner_char}')
            sys.stdout.flush()
            time.sleep(self.delay)
            sys.stdout.write('\b')

    def stop(self):
        self.running = False
        self.thread.join()
        sys.stdout.write('\b')

    def update_progress(self, progress):
        self.progress = progress

def animation():
    total = 10  # Simulate 10 steps
    spinner_progress_bar = SpinnerProgressBar(total)
    spinner_progress_bar.start()
    try:
        for i in range(total + 1):
            spinner_progress_bar.update_progress(i)
            time.sleep(0.5)  # Simulate some work being done
    finally:
        spinner_progress_bar.stop()
    server_logger.info("\nServer startup complete!")

#region webapp
WEB_APP_UPLOAD_FOLDER = 'static/image'  
app.config['WEB_APP_UPLOAD_FOLDER'] = WEB_APP_UPLOAD_FOLDER
app.secret_key = 'awesome_bardia'  

ALLOWED_IMAGE_EXTENSIONS = {'png', 'jpg', 'jpeg', 'gif' , 'webp'}
ALLOWED_AUDIO_EXTENSIONS = {'mp3', 'wav', 'ogg'}
os.makedirs(WEB_APP_UPLOAD_FOLDER, exist_ok=True)
login_manager = LoginManager()
login_manager.init_app(app)
login_manager.login_view = 'login'  
app.config['SESSION_COOKIE_HTTPONLY'] = True
app.config['PERMANENT_SESSION_LIFETIME'] = timedelta(minutes=45)

def admin_required(f):
    @wraps(f)
    def decorated_function(*args, **kwargs):
        if not current_user.is_authenticated or current_user.role != 'admin':
            return redirect(url_for('login'))  
        return f(*args, **kwargs)
    return decorated_function

class User(UserMixin):
    def __init__(self, id, username, role):
        self.id = id
        self.username = username
        self.role = role

    def get_id(self):
        return str(self.id)

# User Loader
@login_manager.user_loader
def load_user(user_id):
    try:
        # Fetch user from MongoDB
        user_info = admin_dash_data_recive(user_id)
        if user_info:
            return User(id=user_id, username=user_info.get('username'), role=user_info.get('role'))
        return None
    except Exception as e:
        server_logger.error(f"Error loading user: {e}")
        return None

# Login Route
@app.route('/')
def login():
    return render_template('login.html')

@app.route('/api/authorization', methods=['POST'])
@limiter.limit("5 per minute")
def validation():
    server_logger.info("Login request received")  

    # Check if form data exists
    if not request.form or 'username' not in request.form or 'password' not in request.form:
        server_logger.warning("Invalid request: Missing 'username' or 'password'")  
        return jsonify({'success': False, 'error': 'Invalid request'})

    username = request.form['username']
    password = request.form['password']
    server_logger.info(f"Username: {username}, Password: {password}")  


    status = login_validation(username, password)
    server_logger.info(f"Login validation status: {status}")    

    if status == 'admin' or status == 'user':
        user_id = get_ai(username)  # Get user ID from MongoDB
        user = User(id=user_id, username=username, role=status)
        login_user(user)  # Log the user in
        session['logged_as'] = status  # Store role in session (optional)
        server_logger.info(f"User logged in as {status}")  
        return jsonify({
            'success': True,
            'redirect_url': url_for('dashboard_admin' if status == 'admin' else 'dashboard_user')
        })
    else:
        server_logger.warning("Login validation failed")  
        return jsonify({'success': False, 'error': 'Invalid username or password'})

# Admin Dashboard Route
@app.route('/api/admin/dashboard', methods=['GET'])
@login_required
@admin_required
def dashboard_admin():
    if current_user.role != 'admin':
        return redirect(url_for('login'))  
    return render_template('dashboard_admin.html')

# User Dashboard Route
@app.route('/api/user/dashboard', methods=['GET'])
@login_required
def dashboard_user():
    if current_user.role != 'user':
        return redirect(url_for('login'))  
    return render_template('dashboard_user.html')

# Register Route
@app.route('/api/register', methods=['POST'])
def register_user():
    try:
        server_logger.info("Entered register_user function")  

        
        form_data = request.form
        server_logger.info(f"Form data received: {form_data}")  

        name = form_data.get('fname')
        lastname = form_data.get('lname')
        username = form_data.get('username')
        email = form_data.get('email')
        phone_number = form_data.get('phone')
        role = 'user'  # Default role is 'user'
        password = form_data.get('password')

        server_logger.info(f"Extracted fields - Name: {name}, Lastname: {lastname}, Username: {username}, Email: {email}, Phone: {phone_number}, Password: {password}")  

        # Validate required fields
        if not all([name, lastname, username, email, phone_number, password]):
            server_logger.warning("Validation failed: Missing required fields")  
            return jsonify({
                'success': False,
                'error': 'All fields are required.'
            }), 400

        server_logger.info("All required fields are present")  

        # Save user info to the database
        server_logger.info("Calling save_user_info function")  
        user_id = save_user_info(name, lastname, username, email, phone_number, role, password)

        if user_id:
            server_logger.info(f"User saved successfully with ID: {user_id}")  
            # Return success response
            return jsonify({
                'success': True,
                'redirect_url': url_for('login')  # Redirect to the login page
            })
        else:
            server_logger.warning("Failed to save user: Username may already exist")  
            # Return error response if user creation failed
            return jsonify({
                'success': False,
                'error': 'Failed to create user. Username may already exist.'
            }), 400

    except Exception as e:
        server_logger.error(f"An exception occurred: {e}")  
        server_logger.error(f"An error occurred during registration: {e}")
        return jsonify({
            'success': False,
            'error': 'An internal server error occurred. Please try again later.'
        }), 500

# Admin Dashboard Info Route
@app.route('/api/admin/dashboard/info', methods=['GET'])
@login_required
@admin_required
def admin_dashboard_info():
    if current_user.role != 'admin':
        return jsonify({'error': 'Unauthorized'}), 403

    user_id = current_user.id
    user_data = admin_dash_data_recive(user_id)
    if not user_data:
        return jsonify({'error': 'User not found'}), 404

    profile_pic_name = user_data.get('photo')
    if profile_pic_name:
        static_folder = os.path.join(app.root_path, 'static')
        profile_pic_path = os.path.join(static_folder, 'image', profile_pic_name)
        if os.path.exists(profile_pic_path):
            profile_pic_url = f"/static/image/{profile_pic_name}"
        else:
            profile_pic_url = 'https://via.placeholder.com/60'
    else:
        profile_pic_url = 'https://via.placeholder.com/60'

    return jsonify({
        'name': f"{user_data.get('name', 'Unknown')} {user_data.get('lastname', 'Unknown')}",
        'role': 'مدیر سیستم',
        'photo': profile_pic_url
    })

@app.route('/api/admin/dashboard-data', methods=['GET'])
@login_required
@admin_required
def admin_dashboard_data():
    """
    Endpoint to fetch dashboard data for the admin.
    Returns all users with the role 'user'.
    """
    try:
        # Check if the user is an admin
        if current_user.role != 'admin':
            return jsonify({'error': 'Unauthorized'}), 403

        users = get_all_users()
        formatted_data = []
        for user in users:
            formatted_data.append({
                'id': str(user['_id']),  # Convert ObjectId to string
                'name': user.get('name', 'Unknown'),
                'lastname': user.get('lastname', 'Unknown'),
                'industry': user.get('industry', 'Unknown'),
                'province': user.get('province', 'Unknown'),
                'city': user.get('city', 'Unknown'),
                'part_number': user.get('part_number', 'Unknown'),
                'contract_start': user.get('start_subscription', 'Unknown'),
                'contract_end': user.get('end_subscription', 'Unknown')
            })

        # Return the formatted data
        return jsonify({
            'message': 'Data retrieved successfully',
            'data': formatted_data
        })

    except Exception as e:
        server_logger.error(f"An error occurred while fetching dashboard data: {e}")
        return jsonify({'error': 'An internal server error occurred'}), 500


def save_base64_image(base64_data, filename):
    try:
        # Decode the base64 data
        image_data = base64.b64decode(base64_data)
        # Save the image to a file
        file_path = os.path.join(app.config['WEB_APP_UPLOAD_FOLDER'], filename)
        with open(file_path, 'wb') as f:
            f.write(image_data)
        return filename
    except Exception as e:
        server_logger.error(f"Error saving image: {e}")
        return None

@app.route('/api/admin/create-user', methods=['POST'])
@login_required
@admin_required
def add_user():
    try:
        data = request.get_json()
        server_logger.info("Parsed JSON data:", data)
        photo = None

        # Process form data
        name = data.get('name')
        lastname = data.get('lastname')
        username = data.get('username')
        start_subscription = data.get('startSubscription')
        end_subscription = data.get('endSubscription')
        part_number = data.get('partNumber')
        industry = data.get('industry')
        province = data.get('province')
        city = data.get('city')
        additional_info = data.get('additionalInfo')
        password = data.get('password')  
        pnumber = data.get('phone_number')

        # Validate required fields
        required_fields = {
            "name": name,
            "lastname": lastname,
            "username": username,
            "startSubscription": start_subscription,
            "endSubscription": end_subscription,
            "partNumber": part_number,
            "industry": industry,
            "province": province,
            "password": password,  
            "phone_number": pnumber,
            "city": city
        }

        missing_fields = [field for field, value in required_fields.items() if not value]
        if missing_fields:
            server_logger.warning(f"Missing required fields: {missing_fields}")
            return jsonify({'success': False, 'error': 'All fields are required.', 'missing_fields': missing_fields}), 400

        # Save user info to the database
        server_logger.info("Saving user to the database...")
        user_id = save_new_user(
            name=name,
            lastname=lastname,
            username=username,
            start_subscription=start_subscription,
            end_subscription=end_subscription,
            part_number=part_number,
            industry=industry,
            province=province,
            city=city,
            phone_number=pnumber,
            password=password,  
            additional_info=additional_info
        )

        if 'photoBase64' in data:  # Assuming the photo is sent as a base64 string
            server_logger.info("Photo data found in request.")
            filename = f"{str(user_id)}.jpg"  # Generate a unique filename
            server_logger.info(f"Saving photo with filename: {filename}")
            photo = save_base64_image(data['photoBase64'], filename)
            server_logger.info("Photo saved successfully:", photo)
        else:
            server_logger.warning("No photo data found in request.")

        if user_id:
            server_logger.info(f"User saved successfully with ID: {user_id}")
            return jsonify({'success': True, 'message': 'User added successfully.', 'user_id': str(user_id)})
        else:
            server_logger.warning("Failed to save user to the database.")
            return jsonify({'success': False, 'error': 'Failed to add user.'}), 400

    except Exception as e:
        server_logger.error(f"An error occurred while adding user: {e}")
        server_logger.error(f"Exception occurred: {e}")
        return jsonify({'success': False, 'error': 'An internal server error occurred.'}), 500
    
@app.route('/api/admin/new_device', methods=['POST'])
@login_required
@admin_required
def add_new_device():
    """Add a new device to the system and associate it with a user.
    
    Required JSON fields:
        deviceName, deviceType, startDate, endDate, routine, userId
    Optional fields:
        days, repeats
    
    Returns:
        - On success: 201 with device ID
        - On missing fields: 400
        - On database error: 500
        - On other errors: 500
    """
    try:
        server_logger.info("Processing new device request")
        
        # Validate request contains JSON data
        data = request.get_json()
        if not data:
            server_logger.warning("No data provided in request")
            return jsonify({"error": "No data provided"}), 400

        # Validate all required fields are present
        required_fields = ["deviceName", "deviceType", "startDate", "endDate", "routine", "userId"]
        for field in required_fields:
            if field not in data:
                server_logger.warning(f"Missing required field in request: {field}")
                return jsonify({"error": f"Missing required field: {field}"}), 400

        # Prepare device data structure
        device_data = {
            "deviceName": data["deviceName"],
            "deviceType": data["deviceType"],
            "startDate": data["startDate"],
            "endDate": data["endDate"],
            "routine": data["routine"],
            "userId": data["userId"],
            "days": data.get("days"),       # Optional
            "repeats": data.get("repeats")  # Optional
        }

        # Add device to database and update user record
        result = add_device_and_update_user(device_data["userId"], device_data)
        
        if result["status"] == "success":
            server_logger.info(f"Successfully added device {result['device_id']}")
            return jsonify({
                "message": "Device added successfully",
                "device_id": result["device_id"]
            }), 201
        else:
            server_logger.error(f"Failed to add device: {result['message']}")
            return jsonify({"error": result["message"]}), 500

    except Exception as e:
        server_logger.error(f"Unexpected error adding device: {str(e)}", exc_info=True)
        return jsonify({"error": "An unexpected error occurred"}), 500

def resize_and_compress_photo(photo_path, size=(100, 100), quality=60):
    """
    Resize and compress a photo.

    Args:
        photo_path (str): Path to the original photo.
        size (tuple): Target size (width, height).
        quality (int): Compression quality (0-100).

    Returns:
        str: Base64-encoded resized photo.
    """
    try:
        with Image.open(photo_path) as img:
            img = img.resize(size, Image.Resampling.LANCZOS)
            buffer = BytesIO()
            img.save(buffer, format="JPEG", quality=quality)
            buffer.seek(0)
            return base64.b64encode(buffer.getvalue()).decode('utf-8')
    except Exception as e:
        server_logger.error(f"Error resizing photo: {e}")
        return None

@app.route('/api/admin/user-details/<user_id>', methods=['GET'])
@login_required
@admin_required
def user_details(user_id):
    """
    Endpoint to fetch user details by user_id.
    """
    try:
        if current_user.role != 'admin':
            return jsonify({'error': 'Unauthorized'}), 403

        user = get_user_details(user_id)

        if user:
            # Convert ObjectId to string
            user['_id'] = str(user['_id'])

            # Handle the photo field
            if 'photo' in user and user['photo']:
                photo_path = os.path.join('static', 'image', user['photo'])
                if os.path.exists(photo_path):
                    resized_photo_base64 = resize_and_compress_photo(photo_path)
                    user['photoBase64'] = f"data:image/jpeg;base64,{resized_photo_base64}" if resized_photo_base64 else None
                else:
                    user['photoBase64'] = None
            else:
                user['photoBase64'] = None

            # Return the entire user object with all fields
            return jsonify({
                'message': 'User details retrieved successfully',
                'data': user  # Return the full user object
            })
        else:
            return jsonify({'error': 'User not found'}), 404

    except Exception as e:
        server_logger.error(f"An error occurred while fetching user details: {e}")
        return jsonify({'error': 'An internal server error occurred'}), 500

@app.route('/api/users/devices', methods=['GET'])
@login_required
def get_show_user_devices():
    try:
        import jdatetime

        server_logger.info("Entered get_user_devices function")  
        user_id = request.args.get('userId')
        if not user_id:
            server_logger.warning("userId is missing in query parameters")  
            return jsonify({"error": "userId is required"}), 400

        devices = get_devices_by_user_id(user_id)
        device_list = []
        for device in devices:
            device_list.append({
                "id" : str(device.get("_id" , "N/A")),
                "name": device.get("deviceName", "N/A"),
                "type": device.get("deviceType", "N/A"),
                "status": device.get("status", "N/A"),
                "endDate": device.get("endDate", "N/A"),
                "routine": device.get("routine", "N/A"),
                "created_at": jdatetime.datetime.fromgregorian(datetime=device["created_at"]).strftime("%Y/%m/%d %H:%M") if device.get("created_at") else "N/A"
            })

        return jsonify({
            "status": "success",
            "data": device_list
        }), 200

    except Exception as e:
        server_logger.error(f"Error fetching devices: {str(e)}")  
        return jsonify({"error": str(e)}), 500
    
@app.route('/api/admin/devices/details/<device_id>', methods=['GET'])
@login_required
@admin_required
def get_device_details_endpoint(device_id):
    try:
        server_logger.info(f"Entered get_device_details_endpoint for device_id: {device_id}")  
        if not device_id or device_id == "undefined":
            server_logger.warning("Invalid device_id: device_id is missing or undefined")  
            return jsonify({"status": "error", "message": "device_id is required"}), 400

        try:
            ObjectId(device_id)
        except InvalidId:
            server_logger.warning(f"Invalid device_id: {device_id} is not a valid ObjectId")  
            return jsonify({"status": "error", "message": "Invalid device_id"}), 400
        device_data = get_device_details_db(device_id)

        if device_data:
            server_logger.info("Returning device details")  
            return jsonify({"status": "success", "data": device_data}), 200
        else:
            server_logger.warning("Device not found")  
            return jsonify({"status": "error", "message": "Device not found"}), 404
    except Exception as e:
        server_logger.error(f"Error in get_device_details_endpoint: {str(e)}")  
        return jsonify({"status": "error", "message": str(e)}), 500

@app.route('/api/admin/devices/logchart/<device_id>', methods=['GET'])
@login_required
@admin_required
def get_device_log_chart(device_id):
    try:
        log_chart_data = get_device_log_chart_data(device_id)
        server_logger.info(f"Log chart data fetched: {log_chart_data}")  

        if not log_chart_data.get("labels") or not log_chart_data.get("values"):
            log_chart_data = {"labels": [], "values": []}  # Default structure

        return jsonify({"status": "success", "data": log_chart_data}), 200
    except Exception as e:
        server_logger.error(f"Error in get_device_log_chart_endpoint: {str(e)}")  
        return jsonify({"status": "error", "message": str(e)}), 500

@app.route('/api/admin/devices/logtable/<device_id>', methods=['GET'])
@admin_required
def get_device_log_table(device_id):
    try:
        log_table_data = get_device_log_table_data(device_id)
        server_logger.info(f"Log table data fetched: {log_table_data}")  

        return jsonify({"status": "success", "data": log_table_data}), 200
    except Exception as e:
        server_logger.error(f"Error in get_device_log_table_endpoint: {str(e)}")  
        return jsonify({"status": "error", "message": str(e)}), 500
    
@app.route('/api/admin/edit_device/<device_id>', methods=['GET'])
@login_required
@admin_required
def edit_device(device_id):
    """
    Endpoint to fetch device details for editing.
    """
    try:
        # Validate device_id
        if not device_id or device_id == "undefined":
            return jsonify({"status": "error", "message": "device_id is required"}), 400

        try:
            ObjectId(device_id)
        except InvalidId:
            return jsonify({"status": "error", "message": "Invalid device_id"}), 400

        device_data = get_device_details_for_edit(device_id)

        if device_data:
            return jsonify({"status": "success", "data": device_data}), 200
        else:
            return jsonify({"status": "error", "message": "Device not found"}), 404
    except Exception as e:
        return jsonify({"status": "error", "message": str(e)}), 500
        
@app.route('/api/admin/devices/update/<device_id>', methods=['PUT'])
@login_required
@admin_required
def update_device_endpoint(device_id):
    """
    Endpoint to update device details after form submission.
    """
    try:
        # Validate device_id
        if not device_id or device_id == "undefined":
            return jsonify({"status": "error", "message": "device_id is required"}), 400

        try:
            # Convert device_id to ObjectId
            ObjectId(device_id)
        except InvalidId:
            return jsonify({"status": "error", "message": "Invalid device_id"}), 400

        # Get updated data from the request
        updated_data = request.get_json()
        if not updated_data:
            return jsonify({"status": "error", "message": "No data provided"}), 400

        # Validate required fields
        required_fields = ["deviceName", "deviceType", "startDate", "endDate", "routine", "status"]
        for field in required_fields:
            if field not in updated_data:
                return jsonify({"status": "error", "message": f"Missing required field: {field}"}), 400

        # Handle days and repeats based on routine
        if updated_data["routine"] == "custom":
            if "days" not in updated_data or "repeats" not in updated_data:
                return jsonify({"status": "error", "message": "days and repeats are required for custom routine"}), 400
        else:
            updated_data["days"] = None
            updated_data["repeats"] = None

        # Update device details in the database
        success = update_device_details(device_id, updated_data)

        if success:
            return jsonify({"status": "success", "message": "Device updated successfully"}), 200
        else:
            return jsonify({"status": "error", "message": "Failed to update device"}), 500
    except Exception as e:
        return jsonify({"status": "error", "message": str(e)}), 500
      
@app.route('/api/admin/update-user/<user_id>', methods=['PUT'])
@login_required
@admin_required
def update_user(user_id):
    """
    Endpoint to update user information, including the _id field.
    """
    try:
        server_logger.info(f"Received request to update user with ID: {user_id}")  

        # Check if the user is an admin
        if current_user.role != 'admin':
            server_logger.warning("Unauthorized access: User is not an admin")  
            return jsonify({'error': 'Unauthorized'}), 403

        # Get updated data from the request
        updated_data = request.get_json()
        server_logger.info(f"Received updated data: {updated_data}")  

        if not updated_data:
            server_logger.warning("No data provided in the request")  
            return jsonify({'error': 'No data provided'}), 400

        # Validate required fields
        required_fields = ['name', 'lastname', 'username', 'phone_number', 'start_subscription', 'end_subscription', 'part_number', 'industry', 'province', 'city', 'additional_info']
        missing_fields = [field for field in required_fields if field not in updated_data]
        if missing_fields:
            server_logger.warning(f"Missing required fields: {missing_fields}")  
            return jsonify({'error': f'Missing required fields: {missing_fields}'}), 400

        # Check if the _id is being updated
        new_id = str(updated_data.get('_id'))

        if new_id and new_id != user_id:
            server_logger.warning(f"User ID is being changed from {user_id} to {new_id}")
            result = update_user_with_new_id(user_id, new_id, updated_data)
        else:
            server_logger.info("Updating user without changing the ID")  
            result = update_user_without_new_id(user_id, updated_data)

        if 'photoBase64' in updated_data:  
            server_logger.info("Photo data found in request.")  
            filename = f"{new_id if new_id else user_id}.jpg"  
            server_logger.info(f"Saving photo with filename: {filename}")  

            # Remove the old photo if it exists
            photo_path = os.path.join("static/image", filename)
            if os.path.exists(photo_path):
                server_logger.info(f"Removing old photo: {photo_path}")  
                os.remove(photo_path)

            # Save the new photo
            photo = save_base64_image(updated_data['photoBase64'], filename)
            server_logger.info("Photo saved successfully:", photo)  
        else:
            server_logger.warning("No photo data found in request.")  

        if result:
            server_logger.info("User updated successfully")  
            return jsonify({'success': True, 'message': 'User updated successfully'}), 200
        else:
            server_logger.warning("Failed to update user")  
            return jsonify({'error': 'Failed to update user'}), 500

    except Exception as e:
        server_logger.error(f"An error occurred while updating user: {e}")
        return jsonify({'error': 'An internal server error occurred'}), 500
       
@app.route('/api/user-info', methods=['GET'])
@login_required
def get_user_info():
    """
    Endpoint to fetch user information by username.
    """
    try:
        username = request.args.get('username')
        if not username:
            return jsonify({'error': 'Username is required'}), 400

        user_data = get_user_by_username(username)
        if not user_data:
            return jsonify({'error': 'User not found'}), 404

        user_data['_id'] = str(user_data['_id'])
        if user_data.get('photo'):
            user_data['photo'] = f"/static/image/{user_data['photo']}"
        else:
            user_data['photo'] = 'https://via.placeholder.com/60'  # Default photo

        # Return the user data as JSON
        return jsonify({
            'message': 'User data retrieved successfully',
            'data': user_data
        })

    except Exception as e:
        server_logger.error(f"An error occurred while fetching user info: {e}")
        return jsonify({'error': 'An internal server error occurred'}), 500
        
@app.route('/api/logout')
@login_required
def logout():
    logout_user()
    flash('You have been logged out', 'info')
    return redirect(url_for('login'))

@app.route('/api/admin/send', methods=['PUT'])
def handle_device_image():
    """Handle device image upload and update associated device information.
    
    Accepts JSON with:
    - Required: device_id, image (base64), format
    - Optional: routine, days, repeats
    
    Returns:
        - 200: Success with image_path
        - 400: Missing/invalid fields
        - 500: File or database operation failed
    """
    try:
        data = request.get_json()
        if not data:
            return jsonify({'error': 'No data provided'}), 400

        # Validate required fields
        required_fields = ['device_id', 'image', 'format']
        for field in required_fields:
            if field not in data:
                return jsonify({'error': f'Missing required field: {field}'}), 400

        device_id = data['device_id']
        image_data = data['image']
        fmt = data['format'].lower()

        if not fmt.isalnum():
            return jsonify({'error': 'Invalid format specified'}), 400

        # Setup file paths
        filename = f'1.{fmt}'
        device_dir = os.path.join('static', device_id)
        file_path = os.path.join(device_dir, filename)

        # Remove existing file if present
        if os.path.exists(file_path):
            try:
                os.remove(file_path)
            except OSError as e:
                server_logger.error(f"Error removing old file: {e}")
                return jsonify({'error': 'Failed to remove existing file'}), 500


        if not save_base64_image(image_data, file_path):
            return jsonify({'error': 'Failed to save image'}), 500
        merge_data = {
            field: data[field] 
            for field in ['routine', 'days', 'repeats'] 
            if field in data
        }

        # Update database with new image and metadata
        image_address = f"{device_id}/{filename}"
        if not save_merge_info(device_id, image_address, merge_data):
            return jsonify({'error': 'Database update failed'}), 500

        return jsonify({
            'message': 'Update successful',
            'image_path': image_address
        }), 200

    except Exception as e:
        server_logger.error(f"Unexpected error in handle_device_image: {e}", exc_info=True)
        return jsonify({'error': 'Internal server error'}), 500

@app.route('/api/admin/command', methods=['POST'])
@login_required
@admin_required
def command():
    data = request.get_json()    
    if 'command' not in data:
        return jsonify({"error": "Missing 'command' in request"}), 400
    if 'deviceId' not in data:
        return jsonify({"error": "Missing 'deviceId' in request"}), 400
    
    command = data['command']
    device_id = data['deviceId']

    try:
        if command == 'pause':
            # Call the pause function for the given device ID
            result = scheduler._pause_device(device_id)
            if result.get("success"):
                return jsonify({"status": "paused", "deviceId": device_id}), 200
            else:
                return jsonify({"error": result.get("warning", "Failed to pause device")}), 400

        elif command == 'start':
            # Call the start function for the given device ID
            result = scheduler._start_device(device_id)
            if result.get("success"):
                return jsonify({"status": "started", "deviceId": device_id}), 200
            else:
                return jsonify({"error": result.get("warning", "Failed to start device")}), 400

        else:
            return jsonify({"error": "Unknown command"}), 404

    except Exception as e:
        # Log the error and return a 500 response
        server_logger.info(f"Error processing command '{command}' for device {device_id}: {e}")
        return jsonify({"error": "Internal server error"}), 500
    
def allowed_file(filename, allowed_extensions):
    return '.' in filename and \
           filename.rsplit('.', 1)[1].lower() in allowed_extensions

@app.route('/api/admin/upload/check', methods=['POST'])
def upload_files():
    try:
        import uuid
        # Get form data
        device_id = request.form.get('deviceId')
        instruction = request.form.get('instruction')
        server_logger.info(f"Received request: device_id={device_id}, instruction={instruction}")
        
        if not device_id or not instruction:
            server_logger.warning("Missing device_id or instruction")
            return jsonify({
                'error': 'Missing required fields: deviceId and instruction are required'
            }), 400

        # Retrieve existing data to delete old files
        previous_data = retrieve_from_mongo(device_id)
        if previous_data:
            old_audio_path = previous_data.get('audio_path')
            old_image_path = previous_data.get('image_path')
            
            # Delete old audio file if it exists
            if old_audio_path and os.path.exists(old_audio_path):
                try:
                    os.remove(old_audio_path)
                    server_logger.info(f"Deleted old audio file: {old_audio_path}")
                except OSError as e:
                    server_logger.error(f"Error deleting old audio file {old_audio_path}: {str(e)}")
            
            # Delete old image file if it exists
            if old_image_path and os.path.exists(old_image_path):
                try:
                    os.remove(old_image_path)
                    server_logger.info(f"Deleted old image file: {old_image_path}")
                except OSError as e:
                    server_logger.error(f"Error deleting old image file {old_image_path}: {str(e)}")
        else:
            server_logger.info(f"No previous data found for device_id {device_id}, no files to delete")

        # Create a unique folder for this upload
        upload_session = str(uuid.uuid4())
        upload_path = os.path.join(app.config['WEB_APP_UPLOAD_FOLDER'], upload_session)
        os.makedirs(upload_path, exist_ok=True)
        server_logger.info(f"Created upload folder: {upload_path}")

        saved_files = []
        image_file = request.files.get('image')
        image_path = None
        if image_file and image_file.filename:
            server_logger.info(f"Image file received: {image_file.filename}")
            
            if not allowed_file(image_file.filename, ALLOWED_IMAGE_EXTENSIONS):
                server_logger.warning(f"Invalid image file format: {image_file.filename}")
                return jsonify({
                    'error': f'Invalid image file format. Allowed: {", ".join(ALLOWED_IMAGE_EXTENSIONS)}'
                }), 400
                
            image_filename = secure_filename(image_file.filename)
            image_path = os.path.join(upload_path, f"image_{device_id}_{image_filename}")
            image_file.save(image_path)
            server_logger.info(f"Image saved to: {image_path}")
            
            saved_files.append({
                'type': 'image',
                'path': image_path,
                'original_name': image_filename
            })
        else:
            server_logger.warning("No image file received or empty filename")

        # Handle audio file
        audio_file = request.files.get('audio')
        audio_path = None
        if audio_file and audio_file.filename:
            server_logger.info(f"Audio file received: {audio_file.filename}")
            if not allowed_file(audio_file.filename, ALLOWED_AUDIO_EXTENSIONS):
                server_logger.warning(f"Invalid audio file format: {audio_file.filename}")
                return jsonify({
                    'error': f'Invalid audio file format. Allowed: {", ".join(ALLOWED_AUDIO_EXTENSIONS)}'
                }), 400
                
            audio_filename = secure_filename(audio_file.filename)
            audio_path = os.path.join(upload_path, f"audio_{device_id}_{audio_filename}")
            audio_file.save(audio_path)
            server_logger.info(f"Audio saved to: {audio_path}")
            
            saved_files.append({
                'type': 'audio',
                'path': audio_path,
                'original_name': audio_filename
            })
        else:
            server_logger.warning("No audio file received or empty filename")

        # Validate based on instruction
        server_logger.info(f"Validating instruction: {instruction}")
        if instruction in ['instruction-S', 'instruction-A']:
            if not image_file or not audio_file:
                server_logger.warning("Validation failed: Both image and audio required for S or A")
                return jsonify({
                    'error': 'Both image and audio files are required for instructions S and A'
                }), 400
        
        elif instruction in ['instruction-L', 'instruction-P']:
            if not audio_file:
                server_logger.warning("Validation failed: Audio required for L or P")
                return jsonify({
                    'error': 'Audio file is required for instructions L and P'
                }), 400
      
        elif instruction == 'custom':
            if not audio_file:
                server_logger.warning("Validation failed: Audio required for custom")
                return jsonify({
                    'error': 'Audio file is required for custom instruction'
                }), 400

        # Save to MongoDB
        if not save_to_mongo(instruction, device_id, audio_path, image_path):
            server_logger.warning("Failed to save to MongoDB")
            return jsonify({
                'error': 'Failed to save upload details to database'
            }), 500

        # Retrieve the updated data after saving
        updated_data = retrieve_from_mongo(device_id)
        if not updated_data:
            server_logger.info("Failed to retrieve updated data from MongoDB")
            return jsonify({
                'error': 'Failed to retrieve updated upload details from database'
            }), 500

        # Prepare response
        response = {
            'message': 'فایل‌ها با موفقیت آپلود شدند',  
            'deviceId': device_id,
            'instruction': instruction,
            'previous_data': previous_data, 
        }
        server_logger.info(f"upload_files() success")
        return jsonify(response), 200

    except Exception as e:
        server_logger.error(f"Exception occurred: {str(e)}")
        app.logger.error(f"Upload error: {str(e)}")
        return jsonify({
            'error': f'خطا در آپلود فایل‌ها: {str(e)}'  # "Error uploading files"
        }), 500

@app.route('/api/admin/devices/remove', methods=['POST'])
@login_required
@admin_required
def remove_device_endpoint():
    data = request.get_json()

    if 'deviceId' not in data:
        return jsonify({"error": "Missing 'deviceId' in request"}), 400
    device_id = data['deviceId']

    try:
        if scheduler.remove_device(device_id):
            return jsonify({"status": "success", "message": f"Device {device_id} removed successfully"}), 200
        else:
            return jsonify({"error": f"Failed to remove device {device_id}"}), 400

    except Exception as e:
        server_logger.info(f"Error removing device {device_id}: {e}")
        return jsonify({"error": "Internal server error"}), 500


@app.route('/api/user/<user_id>', methods=['DELETE'])
def delete_user_endpoint(user_id):
    """Endpoint for completely deleting a user and all associated data"""
    if not ObjectId.is_valid(user_id):
        return jsonify({'error': 'Invalid user ID format'}), 400
    
    try:
        print(user_id)
        if delete_user(user_id):
            return jsonify({'message': 'User and all associated data deleted successfully'}), 200
        return jsonify({'error': 'User not found or could not be deleted'}), 404
    except Exception as e:
        return jsonify({'error': f'Server error during deletion: {str(e)}'}), 500

#endregion

if __name__ == '__main__':
    # animation()
    scheduler = PersistentTaskScheduler()
    serve(app, host='127.0.0.1', port=5000, threads=1)
    server_logger.info("Flask app started")
