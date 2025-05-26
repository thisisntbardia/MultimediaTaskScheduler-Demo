from datetime import datetime
from pymongo import MongoClient, IndexModel
from bson.objectid import ObjectId
from M import merger , open_video_file
import bcrypt  
import os
import requests
from concurrent.futures import ThreadPoolExecutor
from datetime import timedelta
from pymongo.errors import DuplicateKeyError
import threading
import heapq
import time
from logging.handlers import RotatingFileHandler
import logging
from pymongo import errors as pymongo_errors
import sys
import io

sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8')
sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding='utf-8')

client = MongoClient('mongodb://localhost:27017/')
db = client['webapp_dashboard']

# Configure logging to use UTF-8 encoding
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(sys.stdout),  # Use stdout with UTF-8 encoding
    ]
)

# Set encoding for the StreamHandler
for handler in logging.root.handlers:
    if isinstance(handler, logging.StreamHandler):
        handler.setStream(sys.stdout)  # Ensure stdout is used
        handler.encoding = 'utf-8'  # Set encoding to UTF-8

def initialize_database():
    """
    Initialize the database and create necessary indexes (e.g., unique username).
    This function should be called once when the application starts.
    """
    try:
        # Access the 'users' collection
        collection = db['users']
        # Create a unique index on the 'username' field
        collection.create_index("username", unique=True)
        logging.info("Unique index created on the 'username' field.")
    except Exception as e:
        logging.error(f"An error occurred while initializing the database: {e}")

initialize_database()
os.makedirs("logs", exist_ok=True)

formatter = logging.Formatter(
    '%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)

class_logs = logging.getLogger("class_logs")
class_logs.setLevel(logging.DEBUG)  
function_handler = RotatingFileHandler(
    "logs/CLASS.log",
    maxBytes=10*1024*1024,  # 10MB per file
    backupCount=5           # Keep 5 backup files
)
function_handler.setLevel(logging.DEBUG)
function_handler.setFormatter(formatter)
class_logs.addHandler(function_handler)
class_logs.propagate = False

server_log = logging.getLogger("server_log")
server_log.setLevel(logging.DEBUG)
mongo_handler = RotatingFileHandler(
    "logs/SERVER.log",
    maxBytes=10*1024*1024,
    backupCount=5
)
mongo_handler.setLevel(logging.DEBUG)
mongo_handler.setFormatter(formatter)
server_log.addHandler(mongo_handler)

database_log = logging.getLogger("database_log")
database_log.setLevel(logging.DEBUG)
database_handler = RotatingFileHandler(
    "logs/DATABASE.log",
    maxBytes=10*1024*1024,
    backupCount=5
)
database_handler.setLevel(logging.DEBUG)
database_handler.setFormatter(formatter)
database_log.addHandler(database_handler)

console_handler = logging.StreamHandler()
console_handler.setLevel(logging.INFO)  
console_handler.setFormatter(formatter)
server_log.addHandler(console_handler)

client = MongoClient('mongodb://localhost:27017/')
db = client['webapp_dashboard']

scheduler_initialized = False
scheduler_lock = threading.Lock()

class PersistentTaskScheduler:
        _instance = None  
        _lock = threading.Lock()  

        def __new__(cls, *args, **kwargs):
            """
            Override the __new__ method to enforce Singleton behavior.
            """
            if cls._instance is None:
                with cls._lock:
                    if cls._instance is None:
                        cls._instance = super(PersistentTaskScheduler, cls).__new__(cls)
                        cls._instance._initialized = False  
            return cls._instance

        def __init__(self):
            """
            Initialize the instance only once.
            """
            if self._initialized:
                return
            self._initialized = True

            # Initialize MongoDB collections and other attributes
            self._initialize()
            
        def _initialize(self):
            """Initialize the scheduler with all required components."""
            # MongoDB connection with timeout using environment variable

            print("Scheduler initialized")
            # Collections
            self.purgatory_col = db['purgatory_col']
            self.tasks_col = db['tasks']
            self.processd_col = db['processd_col']
            self.devices_col = db['devices']
            self.paused_process_col = db['paused_process']
            self.device_logs_col = db['device_logs']
            self.bad_tasks_col = db['bad_tasks']
            self.server_col = db['MS']
            
            
            # System state
            self.heap = []
            self.heap_lock = threading.RLock()
            self._shutdown_flag = False
            self.executor = ThreadPoolExecutor(max_workers=4)

            # Clock skew handling
            self._clock_skew = timedelta(0)
            self._last_clock_check = datetime.min

            # Max retries before marking as bad task
            self.MAX_RETRIES = 3
            self.MAX_SER_RETRIES = 10

            # Startup sequence
            self.init_instructions()
            self._create_collections()
            self._init()
            self.initialize_local_server()
            self._start_scheduler()
            self._verify_heap()

        def _create_collections(self):
            """Create necessary collections with indexes."""
            try:
                # Tasks collection
                self.tasks_col.create_indexes([
                    IndexModel([('device_id', 1)]),
                    IndexModel([('next_execution', 1)]),
                    IndexModel([('expiration', 1)], expireAfterSeconds=0)
                ])

                self.bad_tasks_col.create_index([('device_id', 1)])
                self.bad_tasks_col.create_index([('failed_at', 1)])
                self.bad_tasks_col.create_index([('errors.timestamp', 1)])

            except Exception as e:
                class_logs.critical(f"Collection creation failed: {e}", exc_info=True)
                raise

        def _init(self):
            """
            Load tasks from MongoDB into the in-memory heap, excluding paused tasks.
            
            Returns:
                bool: True if tasks were successfully loaded, False otherwise.
            """
            try:
                # Fetch all non-paused tasks from the MongoDB collection
                tasks = list(self.tasks_col.find({'paused': {'$ne': True}}))
                if not tasks:
                    # Log if no tasks are found
                    class_logs.info("No active tasks found in MongoDB.")
                    return True  # Return True since this is not an error, just an empty collection

                for task in tasks:
                    routine = task['routine']
                    task_id_str = str(task['_id'])

                    if routine == 'custom':
                    #    self._handle_expired_task(task_id_str , task['device_id'] , routine)                               #fix me later
                        pass
                    
                    elif routine == 'instruction-A' and task['phase'] >= 29:
                        self._handle_expired_task(task_id_str , task['device_id'] , routine , task['image_path'])

                    # elif routine == 'instruction-P' and task['phase'] >= 29:
                    #     self._handle_expired_task(task_id_str , task['device_id'] , routine ,task['image_path'])

                    elif routine == 'instruction-S' and task['phase'] >= 6:
                        self._handle_expired_task(task_id_str , task['device_id'] , routine , task['image_path'])

                    elif routine == 'instruction-L' and task['phase'] >= 18:
                        self._handle_expired_task(task_id_str , task['device_id'] , routine ,task['image_path'])

                    
                    heapq.heappush(self.heap, (
                        task['next_execution'].timestamp(),  
                        task_id_str,  
                        task['device_id'],  
                        task['expiration'].timestamp(),  
                        task['repeat_hours'],  
                        task['image_path'],  
                        task['routine']  
                    ))
                    class_logs.info(f"Successfully loaded device: {task['device_id']} active tasks from MongoDB for:  {task['next_execution']}.")
                    

                # Log the number of tasks loaded
                class_logs.info(f"Successfully loaded {len(self.heap)} active tasks from MongoDB.")
                return True

            except Exception as e:
                # Log any exceptions that occur during the initialization process
                class_logs.error(f"Error loading tasks from MongoDB: {e}", exc_info=True)
                return False
                
        def _start_scheduler(self):
            """Start the background scheduler thread."""
            def scheduler_loop():
                class_logs.info("Scheduler started and running continuously")
                while not self._shutdown_flag:
                    try:
                        current_time = datetime.now()

                        with self.heap_lock:
                            self._process_due_tasks(current_time)

                        time.sleep(5)  # Sleep for 5 seconds before rechecking
                    except Exception as e:
                        class_logs.error(f"Scheduler error: {e}", exc_info=True)
                        time.sleep(1)  # Prevent log flooding in case of repeated errors

            thread = threading.Thread(target=scheduler_loop, daemon=False)
            thread.start()

        def _process_due_tasks(self, current_time):
            """Process tasks due for execution."""
            while self.heap and not self._shutdown_flag:
                try:
                    with self.heap_lock:
                        if not self.heap:
                            class_logs.info("No tasks in the heap. Waiting for new tasks.")
                            break

                        task = self.heap[0]
                        task_due_time = task[0]
                        task_description = task[1]

                        if task_due_time > current_time.timestamp():
                            break

                        # Pop the task from the heap
                        task = heapq.heappop(self.heap)
                        class_logs.info(f"Processing task: {task_description} (due at {task_due_time})")

                        # Verify the heap integrity
                        try:
                            self._verify_heap()
                            class_logs.info("Heap verified successfully.")
                        except Exception as e:
                            class_logs.warning(f"Heap verification failed: {e}")
                            raise  # Re-raise the exception to handle it in the outer try-except block

                    # Submit the task for execution in the thread pool
                    try:
                        self.executor.submit(self._process_task, task, current_time)
                        class_logs.info(f"Task submitted for execution: {task_description}")
                    except Exception as e:
                        class_logs.error(f"Error submitting task to executor: {e}", exc_info=True)
                        class_logs.warning(f"Task could not be submitted: {task_description}")

                except Exception as e:
                    class_logs.error(f"Error processing task: {e}", exc_info=True)
                    class_logs.warning(f"Skipping task due to error: {task_description}")

        def _process_task(self, task, current_time):
            """
            Process a task with retries and logging.

            Args:
                task (tuple): A tuple containing task details (execution_time, task_id, device_id, expiration_ts, repeat_hours, image_path, routine).
                current_time (datetime): The current time for task processing.
            """
            # Unpack the task tuple
            execution_time, task_id, device_id, expiration_ts, repeat_hours, image_path, routine = task
            expiration = datetime.fromtimestamp(expiration_ts)  # Convert expiration timestamp to datetime

            try:
                # Fetch the task document from MongoDB
                task_doc = self.tasks_col.find_one({'_id': ObjectId(task_id)})
                if not task_doc:
                    class_logs.error(f"Task {task_id} not found in MongoDB")
                    return
                
                retries = task_doc.get('retries', 0)  
                data = self._get_mod_status(device_id)                

                if data is None:
                    class_logs.error(f'Something went wrong, mod status not found for device id {device_id}')
                    raise Exception(f"Mod status not found for device id {device_id}")
                new_execution = datetime.now() + timedelta(hours=repeat_hours)                        
                
                
                if data['status'] == 'no':
                    class_logs.info(f"status is flase for device: {device_id} , at: {datetime.fromtimestamp(execution_time)}")
                    doc = {
                        'device_id': device_id,
                        'time': current_time,
                        'predefined_time': execution_time,
                        'next_execution': new_execution,
                        'kind': data.get('kind', 'not available'),
                        'status': 'successfully accomplished without merge',
                        'merge': False
                    }
                    self.processd_col.insert_one(doc)
                    store_log(device_id , "گزارش با موفقیت انجام شد" , "اطلاع")
                    threading.Thread(target=play, args=(device_id,), daemon=True).start()


                elif data['status'] == 'yes':
                    class_logs.info(f"status is ON for device: {device_id} , at: {datetime.fromtimestamp(execution_time)}")

                    class_logs.info(f"Executing task {task_id} for {device_id}")
                    phase = self.check_device_phase(device_id, routine, nchange=True)  # Get or create phase
                    phase_name = self.get_phase_name(routine, phase) + '.mp3'

                    # Send merge request based on image status
                    if data['kind'] == 'with_image':
                        class_logs.info("sending merge request with image")
                        _send_merge_request_offline(device_id , image_path , audio_path=data['audio_path'])

              
                    elif data['kind'] == 'audio_only':
                        class_logs.info("sending merge request with_out image")
                        _send_merge_request_offline(device_id , image_path= None  , audio_path=data['audio_path'])

                    doc = {
                        'device_id': device_id,
                        'time': current_time,
                        'predefined_time': execution_time,
                        'next_execution': new_execution,
                        'kind': data.get('kind', 'not available'),
                        'status': 'successfully accomplished with merge',
                        'merge': True
                    }
                    
                    self.processd_col.insert_one(doc)

                else:
                    class_logs.warning(f"somthing went wrong getting the status value for device {device_id}")                   
                    self._pause_device(device_id)
                    
                class_logs.info(f"this is expiration: {expiration} and this is next execute time {new_execution} , for device {device_id}")
                

                if new_execution >= expiration:
                    self.add_task(
                        device_id=device_id,
                        expiration=expiration,
                        repeat_hours=repeat_hours,
                        image_path=image_path,
                        routine=routine,
                        next_execution=new_execution
                    )
                    class_logs.info(f"Successfully executed {device_id} at: {datetime.now()} and the next execution at: {new_execution}")
                    
                else:
                    heapq.heappush(self.heap, 
                        (new_execution.timestamp(),
                        task_id,
                        device_id,
                        expiration_ts,
                        repeat_hours,
                        image_path,
                        routine
                    ))

                    self.tasks_col.update_one(
                        {'_id': ObjectId(task_id)},
                        {'$set': {'next_execution': new_execution}}
                    )



            except Exception as e:
                # Handle task execution failure
                class_logs.error(f"Task execution failed for {device_id}: {e}", exc_info=True)
                retries += 1  # Increment retry count

                if retries >= self.MAX_RETRIES:
                    # Move the task to the bad tasks collection if max retries are exceeded
                    class_logs.error(f'Task: {task_id} for device: {device_id} failed and set to bad tasks')
                    self._handle_failed_task(task_id, device_id, task_doc, e)
                else:
                    # Update retry count in MongoDB and reschedule the task
                    self.tasks_col.update_one(
                        {'_id': ObjectId(task_id)},
                        {'$set': {'retries': retries}}
                    )
                    self._log_task_retry(device_id, task_id, e)

                    # Reschedule the task with a delay of 5 minutes
                    heapq.heappush(self.heap, (
                        (datetime.now() + timedelta(minutes=5)).timestamp(),
                        task_id,
                        device_id,
                        expiration_ts,
                        repeat_hours,
                        image_path,
                        routine
                    ))
                    
        def _get_mod_status(self, device_id):
                """
                Check and reset the modification status for a device in the purgatory collection.

                Args:
                    device_id (str): The unique identifier for the device.

                Returns:
                    false if the mod is not false return none if the collection is not found for the device id.
                """
                try:
                    data = self.purgatory_col.find_one(
                        {'device_id': device_id},
                        {'mod': 1, 'audio_path': 1, 'image_path': 1 , 'kind' : 1}
                    )

                    if data and data.get('mod', False):
                        # Update mod to False
                        result = self.purgatory_col.update_one(
                            {'device_id': device_id},
                            {'$set': {'mod': False}}
                        )
                        
                        if result.modified_count > 0:
                            class_logs.info(f"Reset mod status to False for device_id: {device_id}")
                            data['status'] = 'yes'
                            return data
                        
                        else:
                            class_logs.warning(f"Failed to reset mod status for device_id: {device_id}")
                            return None
                    
                    else:
                        class_logs.info(f"No mod status or mod is False for device_id: {device_id}")
                        data['status'] = 'no'
                        return data

                except Exception as e:
                    class_logs.error(f"Error in _get_mod_status for device_id {device_id}: {str(e)}")
                    return None

        def add_task(self, device_id,  image_path, routine, expiration = None, repeat_hours = None , next_execution=None):
            """
            Add a new task to the scheduler.
            
            Args:
                device_id (str): The ID of the device.
                expiration (datetime): The expiration time of the task.
                repeat_hours (int): The number of hours between repeats.
                image_path (str): The path to the image for the task.
                routine (str): The routine to execute.
                next_execution (datetime, optional): The next execution time. Defaults to now + repeat_hours.
            
            Returns:
                bool: True if the task was added successfully, False otherwise.
            """
            
            with self.heap_lock:
                try:
                    if routine == 'custom': 
                        if not isinstance(expiration, datetime):
                            raise ValueError("Expiration must be a datetime object")
                        if repeat_hours < 0:
                            raise ValueError("repeat_hours must be non-negative")
                        if not image_path or not routine:
                            raise ValueError("image_path and routine are required")

                        # Calculate next execution time
                        next_exec = next_execution or self._adjusted_time() + timedelta(hours=repeat_hours)
                        if next_exec >= expiration:
                            class_logs.warning(f"Task for {device_id} beyond expiration, not scheduling")
                            return False

                        # Create task document
                        task_doc = {
                            'device_id': device_id,
                            'expiration': expiration,
                            'repeat_hours': repeat_hours,
                            'image_path': image_path,
                            'routine': routine,
                            'next_execution': next_exec,
                            'created_at': datetime.now(),
                            'retries': 0,
                            'errors': []
                        }

                        # Insert into MongoDB first
                        task_id = self.tasks_col.insert_one(task_doc).inserted_id
                        task_id_str = str(task_id)

                        # Add to heap
                        heapq.heappush(self.heap, (
                            next_exec.timestamp(),
                            task_id_str,
                            device_id,
                            expiration.timestamp(),
                            repeat_hours,
                            image_path,
                            routine
                        ))

                        class_logs.debug(f"Added task {task_id_str} for {device_id}")
                        self._verify_heap()
                        return True

                    elif routine == 'instruction-A':
                        self.instruction_a( device_id, image_path, routine)

                    elif routine == 'instruction-P':
                        self.instruction_p(device_id, image_path, routine)

                    elif routine == 'instruction-S':
                        self.instruction_s( device_id, image_path, routine)

                    elif routine == 'instruction-L':
                        self.instruction_l( device_id, image_path, routine)

                    elif routine == 'start':
                        self._start_device(device_id)  

                    elif routine == 'pause':
                        self._pause_device(device_id)  

                    elif routine == 'delete':
                        self.delete_device(device_id, image_path)

                    else :
                        return {"error" : "command not found"}
                    
                except Exception as e:
                    class_logs.error(f"Error adding task for {device_id}: {e}", exc_info=True)
                    if 'task_id' in locals():
                        self.tasks_col.delete_one({'_id': task_id})
                    return False

        def initialize_local_server(self):
            """
            Initialize the local server and add it to the MongoDB collection.

            :param ip: The IP address of the server (e.g., "127.0.0.1")
            :param port: The port of the server (e.g., "5000")
            :param endpoint: The endpoint of the server (e.g., "app/merger")
            :param name: The name of the server (e.g., "my_server")
            :return: None
            """
            port = '5000'
            ip   = '127.0.0.1'
            name = 'Local'
            endpoint = 'app/merger'
            server_url = f"http://{ip}:{port}/{port}/{name}/{endpoint}"

            # Add the local server to the collection
            try:
                result = self.modify_ser(command='add', ip=ip, port=port, endpoint=endpoint, name=name)
                if result:
                    print(f"Successfully added local server: {server_url}")
                else:
                    print(f"Local server already exists: {server_url}")
            except ValueError as e:
                print(f"Error: {e}")
                
        def init_instructions(self):
            collection = db["instructions"]   # Collection name

            # Define the plans to update or insert
            plans = [
                {
                    "plan": "a",
                    "phase": {
                        "1":  {"days": 5, "repeats": 12, 'name' : '100'},
                        "2":  {"days": 5, "repeats": 24, 'name' : '101'},
                        "3":  {"days": 5, "repeats": 24, 'name' : '121'},
                        "4":  {"days": 5, "repeats": 24, 'name' : '102'},
                        "5":  {"days": 5, "repeats": 24, 'name' : '121'},
                        "6":  {"days": 1, "repeats": 1 , 'name' : '103'},
                        "7":  {"days": 5, "repeats": 12, 'name' : '100'},
                        "8":  {"days": 5, "repeats": 24, 'name' : '121'},
                        "9":  {"days": 1, "repeats": 1,  'name' : '104'},
                        "10": {"days": 5, "repeats": 12, 'name' : '100'},
                        "11": {"days": 5, "repeats": 24, 'name' : '121'},
                        "12": {"days": 1, "repeats": 1,  'name' : '105'},
                        "13": {"days": 5, "repeats": 12, 'name' : '106'},
                        "14": {"days": 5, "repeats": 24, 'name' : '121'},
                        "15": {"days": 1, "repeats": 1,  'name' : '107'},
                        "16": {"days": 5, "repeats": 12, 'name' : '106'},
                        "17": {"days": 5, "repeats": 24, 'name' : '121'},
                        "18": {"days": 1, "repeats": 1,  'name' : '108'},
                        "19": {"days": 5, "repeats": 12, 'name' : '106'},
                        "20": {"days": 5, "repeats": 24, 'name' : '121'},
                        "21": {"days": 1, "repeats": 1,  'name' : '109'},
                        "22": {"days": 5, "repeats": 12, 'name' : '106'},
                        "23": {"days": 5, "repeats": 24, 'name' : '121'},
                        "24": {"days": 1, "repeats": 1,  'name' : '110'},
                        "25": {"days": 5, "repeats": 12, 'name' : '106'},
                        "26": {"days": 5, "repeats": 24, 'name' : '121'},
                        "27": {"days": 1, "repeats": 1,  'name' : '111'},
                        "28": {"days": 0, "repeats": 0,  'name' : None}
                    },
                },
                {
                    "plan": "s",
                    "phase": {
                        "1": {"days": 3, "repeats": 4,  'name' : '036'},
                        "2": {"days": 5, "repeats": 4,  'name' : '056'},
                        "3": {"days": 1, "repeats": 1,  'name' : '001'},
                        "4": {"days": 51,"repeats": 3,  'name' : '023'},  
                        "5": {"days": 0, "repeats": 0,  'name' : None},
                        "6": {"days": 0, "repeats": 0,  'name' : None}

                    },
                },
                {
                    "plan": "p",
                    "phase": {
                        "1": {"days": 3, "repeats": 365,'name' : '000'},
                        "2": {"days": 0, "repeats": 0,  'name' : None},
                        "3": {"days": 0, "repeats": 0,  'name' : None}

                    },
                },
                {
                    "plan": "l",
                    "phase": {
                        "1":  {"days": 3, "repeats": 48 , 'name' : '010'},
                        "2":  {"days": 3, "repeats": 48 , 'name' : '010'},
                        "3":  {"days": 3, "repeats": 48 , 'name' : '010'},
                        "4":  {"days": 3, "repeats": 48 , 'name' : '010'},
                        "5":  {"days": 3, "repeats": 48 , 'name' : '010'},
                        "6":  {"days": 3, "repeats": 48 , 'name' : '010'},
                        "7":  {"days": 3, "repeats": 48 , 'name' : '010'},
                        "8":  {"days": 3, "repeats": 48 , 'name' : '010'},
                        "9":  {"days": 3, "repeats": 48 , 'name' : '010'},
                        "10": {"days": 3, "repeats": 48 , 'name' : '010'},
                        "11": {"days": 3, "repeats": 48 , 'name' : '010'},
                        "12": {"days": 3, "repeats": 48 , 'name' : '010'},
                        "13": {"days": 3, "repeats": 48 , 'name' : '010'},
                        "14": {"days": 3, "repeats": 48 , 'name' : '010'},
                        "15": {"days": 1, "repeats": 24 , 'name' : '030'},
                        "16": {"days": 1, "repeats": 24 , 'name' : '030'},
                        "17": {"days": 0, "repeats": 0  , 'name' : None },
                        "18": {"days": 0, "repeats": 0,   'name' : None}

                    }
                },           
            ]

            try:
                # Update or insert each plan
                for plan in plans:
                    query = {"plan": plan["plan"]}  # Query to find the document
                    update = {"$set": plan}         # Update the entire document
                    result = collection.update_one(query, update, upsert=True)
                    if result.upserted_id:
                        class_logs.info(f"Inserted new plan: {plan['plan']}")
                    else:
                        class_logs.info(f"Updated existing plan: {plan['plan']}")
            except Exception as e:
                print(f"Error updating or inserting documents: {e}")

        def _handle_expired_task(self, task_id, device_id, routine, image_path):
            """Handle expired task cleanup and logging."""
            try:
                class_logs.info(f"[DEBUG] Starting expired task handling for task_id: {task_id}, device: {device_id}, routine: {routine}")
                       
                if routine == 'custom':
                    class_logs.info("[DEBUG] Handling custom routine")
                    
                    document = self.purgatory_col.find_one({'device_id': device_id})
                    if document.get('image_path') and document.get('image_path') is not None:
                        class_logs.info(f"[_handle_expired_task]: image file found for device: {device_id}\npath:{document.get('image_path')}")
                        os.remove(document.get('image_path'))
                    else:
                        class_logs.info(f"[_handle_expired_task]: no image file found for device: {device_id}")
                        
                    if document.get('audio_path') and document.get('audio_path') is not None:
                        class_logs.info(f"[_handle_expired_task]: audio file found for device: {device_id}\npath:{document.get('audio_path')}")
                        os.remove(document.get('audio_path'))
                    else:
                        class_logs.info(f"[_handle_expired_task]: no audio file found for device: {device_id}")
                        
                        


                    class_logs.info(f"[DEBUG] Attempting to delete task {task_id} from tasks collection")
                    self.tasks_col.delete_one({'_id': ObjectId(task_id)})
                    class_logs.info("[DEBUG] Task deleted from tasks collection")

                    class_logs.info("[DEBUG] Preparing to insert log document")
                    self.device_logs_col.insert_one({
                        'device_id': device_id,
                        'message': f"Custom task {task_id} expired successfully"
                    })
                   
                    
                    update_data = {'status': 'غیرفعال'}
                    # Update the document
                    result = self.devices_col.update_one(
                        {'_id': ObjectId(device_id)},  # Filter by device_id
                        {'$set': update_data}          # Use $set to update specific fields
                    )

                    result_purgatory_col = self.purgatory_col.delete_one({'device_id': device_id})
                    result_tasks_col     = self.tasks_col.delete_one({'device_id': device_id})

                    if result_purgatory_col.deleted_count == 1:
                        class_logs.info(f"purgatory_col: Successfully deleted device_id: {device_id} ")
                    else:
                        class_logs.info(f"purgatory_col: No document found with device_id: {device_id}")

                    if result_tasks_col.deleted_count == 1:
                        class_logs.info(f"tasks_col: Successfully deleted device_id: {device_id} ")
                    else:
                        class_logs.info(f"tasks_col: No document found with device_id: {device_id}")



                    class_logs.info("[DEBUG] Log document inserted successfully")

                    class_logs.info(f"Task {task_id} for device {device_id} expired and cleaned up.")

                elif routine == 'instruction-A':

                    document = self.purgatory_col.find_one({'device_id': device_id})
                    if document.get('image_path') and document.get('image_path') is not None:
                        class_logs.info(f"[_handle_expired_task]: image file found for device: {device_id}\npath:{document.get('image_path')}")
                        os.remove(document.get('image_path'))
                    else:
                        class_logs.info(f"[_handle_expired_task]: no image file found for device: {device_id}")
                        
                    if document.get('audio_path') and document.get('audio_path') is not None:
                        class_logs.info(f"[_handle_expired_task]: audio file found for device: {device_id}\npath:{document.get('audio_path')}")
                        os.remove(document.get('audio_path'))
                    else:
                        class_logs.info(f"[_handle_expired_task]: no audio file found for device: {device_id}")
                            

                    

                    class_logs.info("[DEBUG] Preparing to insert log document")
                    self.device_logs_col.insert_one({
                        'device_id': device_id,
                        'message': f"Instruction-A task {task_id} expired successfully"
                    })
                    
                    update_data = {'status': 'غیرفعال'}
                    # Update the document
                    result = self.devices_col.update_one(
                        {'_id': ObjectId(device_id)},  # Filter by device_id
                        {'$set': update_data}          # Use $set to update specific fields
                    )
                                
                    result_purgatory_col = self.purgatory_col.delete_one({'device_id': device_id})
                    result_tasks_col     = self.tasks_col.delete_one({'device_id': device_id})

                    if result_purgatory_col.deleted_count == 1:
                        class_logs.info(f"purgatory_col: Successfully deleted device_id: {device_id} ")
                    else:
                        class_logs.info(f"purgatory_col: No document found with device_id: {device_id}")

                    if result_tasks_col.deleted_count == 1:
                        class_logs.info(f"tasks_col: Successfully deleted device_id: {device_id} ")
                    else:
                        class_logs.info(f"tasks_col: No document found with device_id: {device_id}")



                    class_logs.info("[DEBUG] Log document inserted successfully")

                    class_logs.info(f"Task {task_id} for device {device_id} expired. Cleanup for routine 'A' completed.")

                elif routine == 'instruction-P':
                    class_logs.info("[DEBUG] Handling instruction-P routine")

                    document = self.purgatory_col.find_one({'device_id': device_id})
                    if document.get('image_path') and document.get('image_path') is not None:
                        class_logs.info(f"[_handle_expired_task]: image file found for device: {device_id}\npath:{document.get('image_path')}")
                        os.remove(document.get('image_path'))
                    else:
                        class_logs.info(f"[_handle_expired_task]: no image file found for device: {device_id}")
                        
                    if document.get('audio_path') and document.get('audio_path') is not None:
                        class_logs.info(f"[_handle_expired_task]: audio file found for device: {device_id}\npath:{document.get('audio_path')}")
                        os.remove(document.get('audio_path'))
                    else:
                        class_logs.info(f"[_handle_expired_task]: no audio file found for device: {device_id}")

                    class_logs.info("[DEBUG] Preparing to insert log document")
                    self.device_logs_col.insert_one({
                        'device_id': device_id,
                        'message': f"Instruction-P task {task_id} expired successfully"
                    })
                    
                    update_data = {'status': 'غیرفعال'}
                    # Update the document
                    result = self.devices_col.update_one(
                        {'_id': ObjectId(device_id)},  # Filter by device_id
                        {'$set': update_data}          # Use $set to update specific fields
                    )

                    result = self.purgatory_col.delete_one({'device_id': device_id})
                    

                    if result.deleted_count == 1:
                        class_logs.info(f"Successfully deleted device_id: {device_id}")
                    else:
                        class_logs.info(f"No document found with device_id: {device_id}")

                    class_logs.info("[DEBUG] Log document inserted successfully")

                    class_logs.info(f"Task {task_id} for device {device_id} expired. Cleanup for routine 'P' completed.")

                elif routine == 'instruction-S':

                    document = self.purgatory_col.find_one({'device_id': device_id})
                    if document.get('image_path') and document.get('image_path') is not None:
                        class_logs.info(f"[_handle_expired_task]: image file found for device: {device_id}\npath:{document.get('image_path')}")
                        os.remove(document.get('image_path'))
                    else:
                        class_logs.info(f"[_handle_expired_task]: no image file found for device: {device_id}")
                        
                    if document.get('audio_path') and document.get('audio_path') is not None:
                        class_logs.info(f"[_handle_expired_task]: audio file found for device: {device_id}\npath:{document.get('audio_path')}")
                        os.remove(document.get('audio_path'))
                    else:
                        class_logs.info(f"[_handle_expired_task]: no audio file found for device: {device_id}")

                    self.device_logs_col.insert_one({
                        'device_id': device_id,
                        'message': f"Instruction-S task {task_id} expired successfully"
                    })
                   
                    
                    update_data = {'status': 'غیرفعال'}
                    # Update the document
                    result = self.devices_col.update_one(
                        {'_id': ObjectId(device_id)},  # Filter by device_id
                        {'$set': update_data}          # Use $set to update specific fields
                    )
                    result_purgatory_col = self.purgatory_col.delete_one({'device_id': device_id})
                    result_tasks_col     = self.tasks_col.delete_one({'device_id': device_id})

                    if result_purgatory_col.deleted_count == 1:
                        class_logs.info(f"purgatory_col: Successfully deleted device_id: {device_id} ")
                    else:
                        class_logs.info(f"purgatory_col: No document found with device_id: {device_id}")

                    if result_tasks_col.deleted_count == 1:
                        class_logs.info(f"tasks_col: Successfully deleted device_id: {device_id} ")
                    else:
                        class_logs.info(f"tasks_col: No document found with device_id: {device_id}")


                    class_logs.info(f"Task {task_id} for device {device_id} expired. Cleanup for routine 'S' completed.")

                elif routine == 'instruction-L':
                    class_logs.info("[DEBUG] Handling instruction-L routine")
                    # if os.path.exists(image_path):
                    #     class_logs.info(f"[DEBUG] Found image at {image_path}, attempting to delete")
                    #     os.remove(image_path)
                    #     class_logs.info(f"Deleted image file for device {device_id} (routine 'L').")

                    class_logs.info("[DEBUG] Preparing to insert log document")
                    self.device_logs_col.insert_one({
                        'device_id': device_id,
                        'message': f"Instruction-L task {task_id} expired successfully"
                    })
                   
                    update_data = {'status': 'غیرفعال'}
                    # Update the document
                    result = self.devices_col.update_one(
                        {'_id': ObjectId(device_id)},  # Filter by device_id
                        {'$set': update_data}          # Use $set to update specific fields
                    )

                    result_purgatory_col = self.purgatory_col.delete_one({'device_id': device_id})
                    result_tasks_col     = self.tasks_col.delete_one({'device_id': device_id})

                    if result_purgatory_col.deleted_count == 1:
                        class_logs.info(f"purgatory_col: Successfully deleted device_id: {device_id} ")
                    else:
                        class_logs.info(f"purgatory_col: No document found with device_id: {device_id}")

                    if result_tasks_col.deleted_count == 1:
                        class_logs.info(f"tasks_col: Successfully deleted device_id: {device_id} ")
                    else:
                        class_logs.info(f"tasks_col: No document found with device_id: {device_id}")



                    class_logs.info(f"Task {task_id} for device {device_id} expired. Cleanup for routine 'L' completed.")

                else:
                    class_logs.error(f"Unknown routine '{routine}' for task {task_id}. No cleanup performed.")
                    self._pause_device(device_id=device_id)
                
                store_log(device_id , "اتمام فرایند با موفقیت" , "اطلاع")
                print("It's Officially Done")
                

            except Exception as e:
                class_logs.error(f"Error handling expired task {task_id}: {e}", exc_info=True)

        def _handle_failed_task(self, task_id, device_id, task_doc, error_msg):
            """Move failed task to bad_tasks collection and clean up."""
            try:
                # Create bad task document
                bad_task_doc = {
                    'original_task': task_doc,
                    'device_id': device_id,
                    'final_error': error_msg,
                    'failed_at': datetime.now(),
                    'retries': task_doc.get('retries', 0)
                }

                # Insert into bad tasks collection
                self.bad_tasks_col.insert_one(bad_task_doc)

                # Remove from main collection
                self.tasks_col.delete_one({'_id': ObjectId(task_id)})

                # Update device log
                self._log_task_failure(device_id, task_id, error_msg)
                self.tasks_failed += 1

                class_logs.warning(f"Moved failed task {task_id} to bad_tasks collection")

            except Exception as e:
                class_logs.error(f"Error handling failed task {task_id}: {e}", exc_info=True)

        def device_logs(self, device_id, update):
            """Update device log with atomic operations."""
            try:
                self.device_logs_col.update_one(
                    {'device_id': device_id},
                    update,
                    upsert=True
                )
            except Exception as e:
                class_logs.error(f"Failed to update device log for {device_id}: {e}", exc_info=True)

        def _log_server_failure(self, ip, endpoint, e):
            """
            Log server failure by incrementing the 'failed' field and logging the error.

            Args:
                ip (str): The IP address of the server.
                endpoint (str): The endpoint of the server.
                e (Exception or str): The error that occurred.
            """
            try:
                # Update the server collection to increment the failure count
                self.server_col.update_one(
                    {"ip": ip, "endpoint": endpoint},
                    {"$inc": {"failed": 1}}
                )
                
                # Log the failure with the error message
                server_log.error(f"Status failure for server {ip}/{endpoint}. Error: {e}")
            except Exception as ex:
                # Log any errors that occur during the logging process
                server_log.error(f"Failed to log server failure for {ip}/{endpoint}: {ex}", exc_info=True)
            
        def _log_server_success(self, ip, endpoint):
            """
            Log server success by incrementing the 'success' field.

            Args:
                ip (str): The IP address of the server.
                endpoint (str): The endpoint of the server.
            """
            try:
                # Update the server collection to increment the success count
                self.server_col.update_one(
                    {"ip": ip, "endpoint": endpoint},
                    {"$inc": {"success": 1}}
                )
                
                # Log the success
                server_log.info(f"Status success for server {ip}/{endpoint}.")
            except Exception as e:
                # Log any errors that occur during the logging process
                server_log.error(f"Failed to log server success for {ip}/{endpoint}: {e}", exc_info=True)
                
        def _log_task_success(self, device_id, task_id):
            """
            Log successful task completion.

            Args:
                device_id (str): The ID of the device.
                task_id (str): The ID of the task.
            """
            try:
                # Update the device logs collection
                self.device_logs_col.update_one(
                    {"device_id": device_id},
                    {
                        '$inc': {'tasks_completed': 1, 'tasks_in_progress': -1},
                        '$push': {
                            'task_history': {
                                'task_id': task_id,
                                'status': 'completed',
                                'timestamp': datetime.now()
                            }
                        }
                    },
                    upsert=True  # Create the document if it doesn't exist
                )
                
                # Log the task success
                class_logs.info(f"Task {task_id} for device {device_id} completed successfully.")
            except Exception as e:
                # Log any errors that occur during the logging process
                class_logs.error(f"Failed to log task success for device {device_id}, task {task_id}: {e}", exc_info=True)

        def _log_task_retry(self, device_id, task_id, error):
            """Log task failure and retry attempt."""
            self.device_logs_col(device_id, {
                '$push': {
                    'errors': {
                        'task_id': task_id,
                        'error': str(error),
                        'timestamp': datetime.now(),
                        'retry': True
                    }
                }
            })

        def _log_task_failure(self, device_id, task_id, error):
            """Log final task failure."""
            self.device_logs_col(device_id, {
                '$inc': {'tasks_failed': 1, 'tasks_in_progress': -1},
                '$push': {
                    'errors': {
                        'task_id': task_id,
                        'error': str(error),
                        'timestamp': datetime.now(),
                        'final': True
                    }
                }
            })

        def get_device_stats(self, device_id):
            """Get detailed statistics for a device."""
            log = self.device_logs_col.find_one({'device_id': device_id})
            if not log:
                return None

            return {
                'total_tasks': log.get('total_tasks', 0),
                'completed': log.get('tasks_completed', 0),
                'failed': log.get('tasks_failed', 0),
                'in_progress': log.get('tasks_in_progress', 0),
                'expired': log.get('tasks_expired', 0),
                'recent_errors': log.get('errors', [])[-10:],  # Last 10 errors
                'last_updated': log.get('_id').generation_time
            }

        def graceful_shutdown(self, signum=None, frame=None):
            """Handle shutdown signals safely."""
            class_logs.info("Initiating graceful shutdown...")
            self._shutdown_flag = True

            # Wait for current tasks to complete
            self.executor.shutdown(wait=True)

            # Final sync with MongoDB
            self._sync_with_mongodb()
            class_logs.info("Shutdown complete")

        def _sync_with_mongodb(self):
            """Synchronize the in-memory heap with MongoDB."""
            with self.heap_lock:
                try:
                    # Get all task IDs from MongoDB
                    mongo_ids = set(str(doc['_id']) for doc in self.tasks_col.find({}, {'_id': 1}))

                    # Get all task IDs from heap
                    heap_ids = set(task[1] for task in self.heap)

                    # Find orphaned MongoDB entries
                    orphans = mongo_ids - heap_ids
                    if orphans:
                        class_logs.warning(f"Cleaning up {len(orphans)} orphaned MongoDB tasks")
                        self.tasks_col.delete_many({'_id': {'$in': [ObjectId(oid) for oid in orphans]}})
                except Exception as e:
                    class_logs.error(f"Error during MongoDB sync: {e}", exc_info=True)

        def _verify_heap(self):
            """Validate heap structure integrity."""
            if __debug__:
                with self.heap_lock:
                    for i in range(len(self.heap)):
                        left = 2 * i + 1
                        right = 2 * i + 2
                        if left < len(self.heap):
                            assert self.heap[i][0] <= self.heap[left][0], \
                                f"Heap violation at index {i} -> {left}"
                        if right < len(self.heap):
                            assert self.heap[i][0] <= self.heap[right][0], \
                                f"Heap violation at index {i} -> {right}"

        def _get_mongo_time(self):
            """Get authoritative time from MongoDB server."""
            try:
                return db.command('serverStatus')['localTime']
            except Exception as e:
                class_logs.error(f"Clock sync failed: {e}", exc_info=True)
                return datetime.utcnow()

        def _check_clock_skew(self):
            """Validate and compensate for clock differences."""
            if (datetime.utcnow() - self._last_clock_check).total_seconds() < 300:
                return

            mongo_time = self._get_mongo_time()
            local_time = datetime.utcnow()
            self._clock_skew = local_time - mongo_time
            self._last_clock_check = local_time

            if abs(self._clock_skew) > timedelta(seconds=5):
                class_logs.warning(f"Significant clock skew detected: {self._clock_skew}")

        def _adjusted_time(self):
            """Get time adjusted for MongoDB clock skew."""
            self._check_clock_skew()
            return datetime.utcnow() - self._clock_skew

        def get_phase_instruction(self, phase, routine):
            """
            Get phase-specific instructions (days and repeats) for a given routine and phase.

            Args:
                phase (int): The current phase of the device.
                routine (str): The routine type (e.g., 'a', 'p', 's', 'l').

            Returns:
                tuple: A tuple containing (days, repeats) for the phase.
            """
            try:
                class_logs.info(f"this is pahse : {phase} ,routine: {routine}")
                if routine == 'instruction-A':
                    routine = 'a'
                elif routine == 'instruction-S':
                    routine = 's'
                elif routine == 'instruction-P':
                    routine = 'p'
                elif routine == 'instruction-L':
                    routine = 'l'
                class_logs.info(f"this is new pahse(get_phase_instruction) : {phase} ,routine: {routine}")

                # Fetch the instruction document for the routine
                instruction_doc = db['instructions'].find_one({'plan': routine})

                if instruction_doc and 'phase' in instruction_doc:
                    # Get the phase-specific instructions
                    phase_key = str(phase)
                    if phase_key in instruction_doc['phase']:
                        phase_instruction = instruction_doc['phase'][phase_key]
                        days = phase_instruction.get('days', 0)
                        repeats = phase_instruction.get('repeats', 0)
                        return days, repeats
                    else:
                        # If the phase doesn't exist, return default values
                        class_logs.warning(f"Phase {phase} not found for routine '{routine}'. Using default values.")
                        return 0, 0
                else:
                    # If the routine doesn't exist, return default values
                    class_logs.error(f"Routine '{routine}' not found in instructions collection.")
                    return 0, 0

            except Exception as e:
                # Log any errors and return default values
                class_logs.error(f"Error fetching phase instructions for routine '{routine}': {e}", exc_info=True)
                return 0, 0

        def remove_device(self, device_id):
            """
            Remove a device and its associated data from the database, heap, and filesystem.

            Args:
                device_id (str): The ID of the device to remove.

            Returns:
                bool: True if the device was successfully removed, False otherwise.
            """
            try:
                # Check if the device exists in the tasks collection
                task = self.tasks_col.find_one({'device_id': device_id})
                document = self.purgatory_col.find_one({'device_id': device_id})

                if not task:
                    class_logs.warning(f"Device {device_id} not found in the tasks collection.")

                self.tasks_col.delete_one({'device_id': device_id})
                self.device_logs_col.delete_one({'device_id': device_id})
                self.purgatory_col.delete_one({'device_id': device_id})
                self.bad_tasks_col.delete_many({'device_id': device_id})
                self.paused_process_col.delete_one({'device_id': device_id})
                self.devices_col.delete_one({'_id': ObjectId(device_id)})
                
                with self.heap_lock:
                    self.heap = [t for t in self.heap if t[2] != device_id]  # Remove by device_id
                    heapq.heapify(self.heap)  # Rebuild 

                if task and document:
                    # Delete associated image file if it exists

                    if document.get('image_path') and document.get('image_path') is not None:
                        class_logs.info(f"[_handle_expired_task]: image file found for device: {device_id}\npath:{document.get('image_path')}")
                        os.remove(document.get('image_path'))
                    else:
                        class_logs.info(f"[_handle_expired_task]: no image file found for device: {device_id}")
                        
                    if document.get('audio_path') and document.get('audio_path') is not None:
                        class_logs.info(f"[_handle_expired_task]: audio file found for device: {device_id}\npath:{document.get('audio_path')}")
                        os.remove(document.get('audio_path'))
                    else:
                        class_logs.info(f"[_handle_expired_task]: no audio file found for device: {device_id}")
                                                   

                # class_logs.info(f"Successfully removed device {device_id} and associated data.")
                return True

            except Exception as e:
                class_logs.error(f"Error removing device {device_id}: {e}", exc_info=True)
                return False
    
        def get_phase_name(self, routine, phase):
            """
            Retrieve the phase name (e.g., '010', '030') for a given routine and phase.

            Args:
                routine (str): The routine type (e.g., 'a', 'p', 's', 'l').
                phase (int): The phase number.

            Returns:
                str: The phase name (e.g., '010', '030'). Returns None if the phase or routine is not found.
            """
            try:
                print(f"this is pahse : {phase} ,routine: {routine}")
                if routine == 'instruction-A':
                    routine = 'a'
                elif routine == 'instruction-S':
                    routine = 's'
                elif routine == 'instruction-P':
                    routine == 'p'
                elif routine == 'instruction-L':
                    routine = 'l'
                print(f"this is new pahse: {phase} ,routine: {routine}")

                class_logs.debug(f"Retrieving phase name for routine: {routine}, phase: {phase}")

                # Fetch the instruction document for the routine
                instruction_doc = db['instructions'].find_one(
                    {'plan': routine}
                )

                if instruction_doc and 'phase' in instruction_doc:
                    # Get the phase-specific instructions
                    phase_key = str(phase)  # Convert phase to string to match the key in the document
                    if phase_key in instruction_doc['phase']:
                        phase_name = instruction_doc['phase'][phase_key].get('name')
                        class_logs.debug(f"Phase name for routine '{routine}', phase {phase}: {phase_name}")
                        return phase_name
                    else:
                        # If the phase doesn't exist, log a warning and return None
                        class_logs.warning(f"Phase {phase} not found for routine '{routine}'.")
                        return None
                else:
                    # If the routine doesn't exist, log an error and return None
                    class_logs.error(f"Routine '{routine}' not found in instructions collection.")
                    return None

            except Exception as e:
                # Log any errors and return None
                class_logs.error(f"Error retrieving phase name for routine '{routine}', phase {phase}: {e}", exc_info=True)
                return None
                    
        def check_max_phase(self , device_id, routine , phase):
            
            if routine == 'instruction-A':
                if phase >= 28:
                    self._pause_device(device_id)                    

            elif routine == 'instruction-S':
                if phase >= 5:
                    self._pause_device(device_id)

            elif routine == 'instruction-L':
                if phase >= 18:
                    self._pause_device(device_id)
        
        def check_device_phase(self, device_id, routine, nchange = False):
            """
            Check the current phase of a device. If no phase exists, initialize one.
            After checking, increment the phase for the next execution.

            Args:
                device_id (str): The ID of the device.
                routine (str): The routine type (e.g., 'a', 'p', 's', 'l').

            Returns:
                int: The current phase number.

            Raises:
                Exception: If an unexpected error occurs during the process.
            """
            try:
                class_logs.debug(f"Checking phase for device_id: {device_id} with routine: {routine}")

                # Fetch the device's phase from the database
                device_phase = self.tasks_col.find_one(
                    {'device_id': device_id},
                    {'phase': 1}
                )

                # self.check_max_phase(device_id, routine, (device_phase.get() - 1) )

                if device_phase and 'phase' in device_phase:
                    # Get the current phase
                    current_phase = device_phase['phase']
                    class_logs.debug(f"Current phase for device_id {device_id}: {current_phase}")
                    next_phase = current_phase

                    if not nchange:
                    # Increment the phase for the next execution
                        next_phase = current_phase + 1
                        class_logs.debug(f"Incrementing phase for device_id {device_id} to: {next_phase}")

                        self.tasks_col.update_one(
                            {'device_id': device_id},
                            {'$set': {'phase': next_phase}}
                        )
                        
                    class_logs.info(f"Updated phase for device_id {device_id} from {current_phase} to: {next_phase}")
                    return current_phase
                
                else:
                    # Initialize phase 1 if no phase exists
                    class_logs.debug(f"No phase found for device_id {device_id}. Initializing phase to 1.")

                    if not nchange:
                        self.tasks_col.update_one(
                            {'device_id': device_id},
                            {'$set': {'phase': 2}}, # set to 2 for the next time
                            upsert=True
                        )
                        class_logs.info(f"Initialized phase for device_id {device_id} to: 2")
                    
                    else:
                        self.tasks_col.update_one(
                            {'device_id': device_id},
                            {'$set': {'phase': 1}}, 
                            upsert=True
                        )
                        class_logs.info(f"Initialized phase for device_id {device_id} to: 1")
                        
                    return 1

            except Exception as e:
                # Log the error with detailed context
                class_logs.error(
                    f"An error occurred while checking/updating phase for device_id {device_id}: {str(e)}",
                    exc_info=True  # Include the full traceback in the logs
                )
                self._pause_device(device_id)    
                        
        def check_if_device_exist(self, device_id):
            """
            Check if a device exists in the tasks collection.

            Args:
                device_id (str): The ID of the device.

            Returns:
                dict: The task document if the device exists, otherwise None.
            """
            # Fetch the task document for the device
            task = self.tasks_col.find_one({'device_id': device_id})

            if task:
                return task
            else:
                return False
        
        def instruction_a(self, device_id, image_path, routine):
            phase = self.check_device_phase(device_id, routine) # get phase if not exist with this device id(and add to it phase after returning the result) or create one 
            
            if (int(phase) - 1) == 28 or phase >= 29:
                task = self.tasks_col.find_one({'device_id': device_id})
                if task:
                    task_id = str(task['_id'])
                    self._handle_expired_task(task_id, device_id, routine, image_path)
                else:
                    class_logs.error(f"Task for device {device_id} not found during expiration.")
                return True  # Exit after handling expiration


            instruction = self.get_phase_instruction(phase , routine)
            day , repeat = instruction
            
            if day == 1 and repeat == 1:                                             
                expiration = datetime.now() + timedelta(hours=30)
                next_exec = datetime.now() + timedelta(hours=24)                       
            else:    
                expiration = datetime.now() + timedelta(days=day)                       
                next_exec = datetime.now() + timedelta(hours=repeat)                    

            # Check if the document exists
            existing_doc = self.tasks_col.find_one({'device_id': device_id})

            if existing_doc:
                print(f"this is phase {phase} and this is the repeat_execution: {repeat} time: {datetime.now()}")
                # Document exists, update only the necessary parts
                update_data = {
                    'routine': routine,
                    'image_path': image_path,
                    'expiration': expiration,
                    'repeat_hours': repeat,
                    'next_execution': next_exec
                }
                print("Document exists, updating necessary parts")
                
                result = self.tasks_col.update_one(
                    {'device_id': device_id},
                    {'$set': update_data}
                )
            else:
                # Document does not exist, insert everything
                insert_data = {
                    'device_id': device_id,
                    'created_at': datetime.now(),
                    'retries': 0,
                    'errors': [],
                    'routine': routine,
                    'image_path': image_path,
                    'expiration': expiration,
                    'repeat_hours': repeat,
                    'next_execution': next_exec
                }
                
                class_logs.info("Document does not exist, inserting everything")
                result = self.tasks_col.insert_one(insert_data)


                # Get the task ID (new or existing)
            if result.upserted_id:  # New document inserted
                task_id = result.upserted_id

            else:  # Existing document updated
                task = self.tasks_col.find_one({'device_id': device_id})
                task_id = task['_id']

            task_id_str = str(task_id)
        
            # Add to heap
            heapq.heappush(self.heap, (
                next_exec.timestamp(),
                task_id_str,
                device_id,
                expiration.timestamp(),
                repeat,
                image_path,
                routine
            ))      
            
            class_logs.debug(f"Added task {task_id_str} for {device_id}")
            self._verify_heap()
            return True
        
        def instruction_p(self, device_id, image_path, routine):
            phase = self.check_device_phase(device_id, routine , nchange= True) # get phase if not exist with this device id(and add to it phase after returning the result) or create one 
                
            if (int(phase) - 1) == 2:
                task = self.tasks_col.find_one({'device_id': device_id})
                if task:
                    task_id = str(task['_id'])
                    self._handle_expired_task(task_id, device_id, routine, image_path)
                else:
                    class_logs.error(f"Task for device {device_id} not found during expiration.")
                return True  # Exit after handling expiration
            
            days , repeat = self.get_phase_instruction(phase , routine)
            class_logs.info(f"this i p taking: \ndays:{days} \nrepeat:{repeat}")
            expiration = datetime.now() + timedelta(days=days)
            next_exec = datetime.now() + timedelta(hours=repeat)
            class_logs.info(f"this is ex {expiration} and this is next {next_exec}")
            existing_doc = self.tasks_col.find_one({'device_id': device_id})

            if existing_doc:
                # Document exists, update only the necessary parts
                update_data = {
                    'routine': routine,
                    'image_path': image_path,
                    'expiration': expiration,
                    'repeat_hours': repeat,
                    'next_execution': next_exec
                }
                print("Document exists, updating necessary parts")
                
                result = self.tasks_col.update_one(
                    {'device_id': device_id},
                    {'$set': update_data}
                )
            else:
                # Document does not exist, insert everything
                insert_data = {
                    'device_id': device_id,
                    'created_at': datetime.now(),
                    'retries': 0,
                    'errors': [],
                    'routine': routine,
                    'image_path': image_path,
                    'expiration': expiration,
                    'repeat_hours': repeat,
                    'next_execution': next_exec
                }
                
                print("Document does not exist, inserting everything")
                result = self.tasks_col.insert_one(insert_data)


                # Get the task ID (new or existing)
            if result.upserted_id:  # New document inserted
                task_id = result.upserted_id

            else:  # Existing document updated
                task = self.tasks_col.find_one({'device_id': device_id})
                task_id = task['_id']

            task_id_str = str(task_id)
        
            # Add to heap
            heapq.heappush(self.heap, (
                next_exec.timestamp(),
                task_id_str,
                device_id,
                expiration.timestamp(),
                repeat,
                image_path,
                routine
            ))      
            class_logs.debug(f"Added task {task_id_str} for {device_id} with next execution at: {next_exec}")
            self._verify_heap()
            return True
        
        def instruction_s(self, device_id, image_path, routine):
            class_logs.info("this is instruction s  function")
            phase = self.check_device_phase(device_id, routine ) # get phase if not exist with this device id(and add to it phase after returning the result) or create one 
            
            if (int(phase) - 1) == 5:
                task = self.tasks_col.find_one({'device_id': device_id})
                if task:
                    task_id = str(task['_id'])
                    self._handle_expired_task(task_id, device_id, routine, image_path)
                else:
                    class_logs.error(f"Task for device {device_id} not found during expiration.")
                return True  # Exit after handling expiration
            
            instruction = self.get_phase_instruction(phase , routine)
            days , repeat = instruction

            if days == 1 and repeat == 1:                                                           #for the unique tasks
                expiration = datetime.now() + timedelta(hours=30)
                next_exec = datetime.now() + timedelta(hours=24)                                           
            else:
                expiration = datetime.now() + timedelta(days=days)
                next_exec = datetime.now() + timedelta(hours=repeat)
            
            existing_doc = self.tasks_col.find_one({'device_id': device_id})

            if existing_doc:
                class_logs.info(f"this is phase {phase} and this is the repeat_execution: {repeat} time: {datetime.now()}")
                # Document exists, update only the necessary parts
                update_data = {
                    'routine': routine,
                    'image_path': image_path,
                    'expiration': expiration,
                    'repeat_hours': repeat,
                    'next_execution': next_exec
                }
                class_logs.info("Document exists, updating necessary parts")
                
                result = self.tasks_col.update_one(
                    {'device_id': device_id},
                    {'$set': update_data}
                )
            else:
                # Document does not exist, insert everything
                insert_data = {
                    'device_id': device_id,
                    'created_at': datetime.now(),
                    'retries': 0,
                    'errors': [],
                    'routine': routine,
                    'image_path': image_path,
                    'expiration': expiration,
                    'repeat_hours': repeat,
                    'next_execution': next_exec
                }
                
                class_logs.info("Document does not exist, inserting everything")
                result = self.tasks_col.insert_one(insert_data)

            
                # Get the task ID (new or existing)
            if result.upserted_id:  # New document inserted
                task_id = result.upserted_id

            else:  # Existing document updated
                task = self.tasks_col.find_one({'device_id': device_id})
                task_id = task['_id']

            task_id_str = str(task_id)
        
        
            # Add to heap
            heapq.heappush(self.heap, (
                next_exec.timestamp(),
                task_id_str,
                device_id,
                expiration.timestamp(),
                repeat,
                image_path,
                routine
            ))      
            class_logs.debug(f"Added task {task_id_str} for {device_id}")
            self._verify_heap()
            return True
        
        def instruction_l(self, device_id, image_path, routine):
            class_logs.info("this is instruction l  function")
            phase = self.check_device_phase(device_id, routine ) # get phase if not exist with this device id(and add to it phase after returning the result) or create one 
            
            if (int(phase) - 1) == 17:
                task = self.tasks_col.find_one({'device_id': device_id})
                if task:
                    task_id = str(task['_id'])
                    self._handle_expired_task(task_id, device_id, routine, image_path)
                else:
                    class_logs.error(f"Task for device {device_id} not found during expiration.")
                return True  # Exit after handling expiration
            
            instruction = self.get_phase_instruction(phase , routine)
            days , repeat = instruction
                        
            if days == 1 and repeat == 24:                                                           #for the unique tasks
                expiration = datetime.now() + timedelta(hours=30)
                next_exec = datetime.now() + timedelta(hours=24)                                  

            else:    
                expiration = datetime.now() + timedelta(days=days)
                next_exec = datetime.now() + timedelta(hours=repeat)                                  
                
            # Check if the document exists
            existing_doc = self.tasks_col.find_one({'device_id': device_id})

            if existing_doc:
                class_logs.info(f"this is phase {phase} and this is the repeat_execution: {repeat} time: {datetime.now()}")
                # Document exists, update only the necessary parts
                update_data = {
                    'routine': routine,
                    'image_path': image_path,
                    'expiration': expiration,
                    'repeat_hours': repeat,
                    'next_execution': next_exec
                }
                class_logs.info("Document exists, updating necessary parts")
                
                result = self.tasks_col.update_one(
                    {'device_id': device_id},
                    {'$set': update_data}
                )
            else:
                # Document does not exist, insert everything
                insert_data = {
                    'device_id': device_id,
                    'created_at': datetime.now(),
                    'retries': 0,
                    'errors': [],
                    'routine': routine,
                    'image_path': image_path,
                    'expiration': expiration,
                    'repeat_hours': repeat,
                    'next_execution': next_exec
                }
                
                class_logs.info("Document does not exist, inserting everything")
                result = self.tasks_col.insert_one(insert_data)


                # Get the task ID (new or existing)
            if result.upserted_id:  # New document inserted
                task_id = result.upserted_id

            else:  # Existing document updated
                task = self.tasks_col.find_one({'device_id': device_id})
                task_id = task['_id']

            task_id_str = str(task_id)
        
            # Add to heap
            heapq.heappush(self.heap, (
                next_exec.timestamp(),
                task_id_str,
                device_id,
                expiration.timestamp(),
                repeat,
                image_path,
                routine
            ))      
            
            class_logs.debug(f"Added task {task_id_str} for {device_id}")
            self._verify_heap()
            return True
        
        def _start_device(self, device_id):
            """
            Resume a paused device's task, calculate the new next execution time, and re-add it to the heap.

            Args:
                device_id (str): The ID of the device to resume.

            Returns:
                dict: A message indicating success or failure.
            """
            task = self.check_if_device_exist(device_id)
            if task and task.get('paused'):
                now = datetime.now()

                # Retrieve the saved time difference
                time_diff = task['time_diff']

                # Convert the time difference back to a timedelta
                time_diff_timedelta = timedelta(
                    days=time_diff['days'],
                    seconds=time_diff['seconds']
                )

                # Calculate the new next execution time
                next_exec = now + time_diff_timedelta

                # Update the task document
                task['next_execution'] = next_exec
                task['paused'] = False
                del task['time_diff']  # Remove the saved time difference

                # Re-add the task to the heap
                with self.heap_lock:  # Ensure thread-safe heap operations
                    heapq.heappush(self.heap, (
                        next_exec.timestamp(),
                        str(task['_id']),  # Task ID
                        task['device_id'],
                        task['expiration'].timestamp(),
                        task['repeat_hours'],
                        task['image_path'],
                        task['routine']
                    ))

                # Update the task in the database
                self.tasks_col.update_one(
                    {'device_id': device_id},
                    {'$set': {'next_execution': next_exec, 'paused': False}, '$unset': {'time_diff': ''}}
                )
                
                stat = activate_device(device_id)
                if stat is False:
                    class_logs.error(f'could not active the device with device id: {device_id}')

                return {"success": "Device resumed", "next_execution": next_exec.isoformat()}
            
            elif task and not task.get('paused'):
                return {"warning": "Device is not paused"}
            
            else:
                return {"warning": "Device not found"}

        def _pause_device(self, device_id):
            """
            Pause a device's task, save the time difference, and remove it from the heap.

            Args:
                device_id (str): The ID of the device to pause.

            Returns:
                dict: A message indicating success or failure.
            """
            task = self.check_if_device_exist(device_id)
            if task:
                now = datetime.now()

                # Calculate the time difference between now and the next execution
                time_diff = task['next_execution'] - now

                # Save the time difference in the task document
                task['time_diff'] = {
                    'days': time_diff.days,
                    'seconds': time_diff.seconds,
                    'total_seconds': time_diff.total_seconds()
                }

                # Mark the task as paused
                task['paused'] = True

                # Remove the task from the heap
                with self.heap_lock:  # Ensure thread-safe heap operations
                    self.heap = [t for t in self.heap if t[2] != device_id]  # Remove by device_id
                    heapq.heapify(self.heap)  # Rebuild the heap

                # Update the task in the database
                self.tasks_col.update_one(
                    {'device_id': device_id},
                    {'$set': {'time_diff': task['time_diff'], 'paused': True}}
                )
                
                stat = deactivate_device(device_id)
                if stat is False:
                    class_logs.error(f'could not deactive the device with device id: {device_id}')


                

                return {"success": "Device paused", "time_diff": str(time_diff)}
            else:
                return {"warning": "Device not found"}
                
        def delete_device(self, device_id, image_path):
            """
            Delete a device from the tasks collection and its associated image file.
            
            Args:
                device_id (str): The ID of the device to delete.
                image_path (str): The path to the image file associated with the device.
            
            Returns:
                bool: True if the device and file were successfully deleted, False otherwise.
            """
            try:
                # Check if the device exists
                task = self.check_if_device_exist(device_id)
                if task:
                    # Delete the device from the tasks collection
                    result = self.tasks_col.delete_one({'device_id': device_id})
                    
                    if result.deleted_count > 0:
                        # Log successful deletion from the database
                        class_logs.info(f"Device {device_id} successfully deleted from the tasks collection.")
                        

     
                        return True  # Return True if both database and file deletion succeed
                    else:
                        # Log deletion failure
                        class_logs.warning(f"Failed to delete device {device_id}. No matching document found.")
                        return False
                else:
                    # Log device not found
                    class_logs.warning(f"Device {device_id} not found in the tasks collection.")
                    return False
            except Exception as e:
                # Log any exceptions that occur during deletion
                class_logs.error(f"Error deleting device {device_id}: {e}", exc_info=True)
                return False

        def _get_servers(self):
            """Retrieve servers from MongoDB"""
            servers = list(self.server_col.find({}, {"_id" : 0 , 'failed' : 0 , 'succeed' : 0}))
            return servers

        def _check_server_status(self , ip, port):
            """Check if server status is okay"""
            try:
                response = requests.get(f"http://{ip}:{port}/checkup", timeout=5)
                return response.status_code == 200 and response.text.lower().strip() == "okay"
            except Exception as e:
                class_logs.error(f"all servers validations failed: {e}")
                return False

        def _send_merge_request(self, device_id, image_path, routine, audio_path):
            """
            Send files (image and audio) to a server for processing and save the resulting video.
            Retries up to 10 times across different servers. If all retries fail, pauses the device.

            Args:
                device_id (str): The ID of the device.
                image_path (str): The path to the image file.
                routine (str): The routine type (e.g., 'a', 'p', 's', 'l').
                status (bool): The status of the image (True if a new image is available, False otherwise).
                audio (str): The name of the audio file (e.g., '010.mp3', '030.mp3').
                image (str, optional): The name of the image file if a new image is available. Defaults to None.

            Returns:
                str: A message indicating the success or failure of the merge request.

            Raises:
                Exception: If no servers are available or all retries fail.
            """
            try:
                class_logs.info("this is send merge request")
                # Get list of servers from MongoDB
                servers = self._get_servers()

                #if no server exist set device to pause
                if not servers:
                    self._pause_device(device_id)
                    server_log.error(f"No servers available in database")

                #if file doesnt exist set the device to pause     
                if audio_path is None or not os.path.exists(audio_path):
                    self._pause_device(device_id)
                    class_logs.error(f"Audio file not found: {audio_path}")
                    
                #if file doesnt exist set the device to pause     
                if image_path is None or not os.path.exists(image_path):
                    self._pause_device(device_id)
                
                    class_logs.error(f"Audio file not found: {audio_path}")
                
                # Initialize retry counter
                retries = 0
                

                while retries < self.MAX_SER_RETRIES:
                    # Iterate through available servers
                    for server in servers:
                        ip = server['ip']
                        endpoint = server['endpoint']
                        port = server['port']
                        class_logs.info(f"checking the first server to send the merge request with the following ip and endpoint {ip} , {endpoint}")
                     
                        # Check server status
                        if not self._check_server_status(ip, port):
                            class_logs.warning(f"Server {ip} is unavailable. Trying next server...")
                            continue  # Skip to the next server if this one is unavailable

                        try:
                            # Prepare headers for the request
                            headers = {
                                "routine": routine,
                                "device_id": device_id
                            }

                            # Prepare files for upload
                            files = {}
                            if image_path is not None and audio_path is not None:
                                files["photo"] = open(image_path, "rb")
                                files["audio"] = open(audio_path, "rb")

                            # Send files to the server's processing endpoint
                            response = requests.post(
                                f"http://{ip}:{endpoint}",
                                headers=headers,
                                files=files,
                                timeout=240
                            )

                            # Close the files after sending
                            for file in files.values():
                                file.close()

                            # Handle the server response
                            if response.status_code == 200:
                                # Determine the file type from the Content-Disposition header
                                content_disposition = response.headers.get('Content-Disposition')
                        
                                if content_disposition and 'filename=' in content_disposition:
                                    filename = content_disposition.split('filename=')[1].strip('"')
                        
                                else:
                                    # Fallback: Use timestamp and default extension
                                    filename = f"{datetime.now().strftime('%Y%m%d_%H%M%S')}.mp4"

                                # Save the file locally
                                video_dir = os.path.join("videos", device_id)
                                os.makedirs(video_dir, exist_ok=True)
                                file_path = os.path.join(video_dir, filename)

                                with open(file_path, "wb") as file:
                                    file.write(response.content)

                                # Log success
                                class_logs.info(f"File successfully saved as {file_path}")
                                server_log.info(f"File successfully saved as {file_path} at {datetime.now()}")
                                return f"Merge happened for {device_id} at {datetime.now()}"

                            else:
                                # Log server error
                                class_logs.error(f"Server {ip} returned an error: {response.status_code} - {response.text}")
                                server_log.error(f"Server {ip} returned an error: {response.status_code} - {response.text}")

                        except Exception as e:
                            # Log any errors during the request
                            class_logs.error(f"Error processing files with server {ip}: {e}", exc_info=True)
                            continue  # Try the next server if this one fails

                    # Increment retry counter
                    retries += 1
                    class_logs.warning(f"Retry {retries}/{self.MAX_SER_RETRIES} for device {device_id}")

                # If all retries fail, pause the device
                class_logs.error(f"All retries failed for device {device_id}. Pausing the device.")
                self._pause_device(device_id)

            except Exception as e:
                # Log any unexpected errors
                class_logs.error(f"Error in _send_merge_request: {e}", exc_info=True)
                
            finally:
                print("one merge request done either correct or wrong")  

        def modify_ser(self, command, ip, port, endpoint, name):
            """
            Modify the server list in MongoDB by either adding or removing a server.

            Assumes that:
            - db is a MongoDB database instance.
            - The servers collection is named "servers".

            :param command: A string, either 'add' or 'remove'
            :param ip: The IP address of the server
            :param port: The port of the server
            :param endpoint: The endpoint of the server
            :param name: The name of the server (to be prefixed to the endpoint)
            :return: For 'add', returns the inserted document's id if successful,
                    For 'remove', returns True if a document was removed, False otherwise.
            :raises ValueError: if the command is neither 'add' nor 'remove'
            """
            collection = self.server_col  # Change to your actual collection if different

            # Construct the full endpoint with the port and name prefixed
            full_endpoint = f"{port}/{endpoint}"

            if command.lower() == 'add':
                # Prevent adding duplicates by checking if server already exists
                if collection.find_one({'ip': ip, 'endpoint': full_endpoint}):
                    print(f"Server with IP {ip} and endpoint {full_endpoint} already exists.")
                    return False
                
                result = collection.insert_one({'name' : name, 'ip': ip, 'endpoint': full_endpoint, 'port' : port})
                print(f"Added server with id: {result.inserted_id}")
                return result.inserted_id

            elif command.lower() == 'remove':
                result = collection.delete_one({'ip': ip, 'endpoint': full_endpoint})
                if result.deleted_count == 0:
                    print(f"No server found with IP {ip} and endpoint {full_endpoint} to remove.")
                    return False
                print(f"Removed server with IP {ip} and endpoint {full_endpoint}.")
                return True

            else:
                raise ValueError("Invalid command. Use 'add' or 'remove'.")
        
        def delete_device_data(self, device_id):
            """
            Delete all data associated with a device from the purgatory collection, tasks collection, 
            and remove corresponding tasks from the in-memory heap queue.

            Args:
                device_id (str): The ID of the device to delete.

            Returns:
                bool: True if the deletion was successful, False otherwise.
            """
            if not device_id:
                class_logs.error("Device ID cannot be None or empty.")
                return False

            try:
                # Delete all entries for the device from the purgatory collection
                purgatory_result = self.purgatory_col.delete_many({'device_id': device_id})
                class_logs.info(f"Deleted {purgatory_result.deleted_count} entries from purgatory_col for device {device_id}")

                # Delete all tasks for the device from the tasks collection
                tasks_result = self.tasks_col.delete_many({'device_id': device_id})
                class_logs.info(f"Deleted {tasks_result.deleted_count} tasks from tasks_col for device {device_id}")

                # Remove all tasks for the device from the in-memory heap
                with self.heap_lock:
                    initial_count = len(self.heap)
                    self.heap = [task for task in self.heap if task[2] != device_id]
                    if len(self.heap) < initial_count:
                        heapq.heapify(self.heap)  # Rebuild the heap only if entries were removed
                        class_logs.info(f"Removed {initial_count - len(self.heap)} tasks from heap for device {device_id}")
                    else:
                        class_logs.info(f"No tasks found in heap for device {device_id}")

                    # Verify heap integrity
                    self._verify_heap()

                return True

            except Exception as e:
                class_logs.error(f"Error deleting data for device {device_id}: {e}", exc_info=True)
                return False
        

def save_user_info(name, lastname, username, email, phone_number, role, password):
    """
    Save user information to the 'users' collection with a securely hashed password.
    Ensures the username is unique.

    Args:
        name (str): The user's first name.
        lastname (str): The user's last name.
        username (str): The user's unique username.
        email (str): The user's email address.
        phone_number (str): The user's phone number.
        role (str): The user's role.
        photo (str): URL to the user's photo.
        password (str): The user's plain-text password.

    Returns:
        ObjectId: The ID of the newly created user document, or None if an error occurs.
    """
    try:
        # Access the 'users' collection
        collection = db['users']

        # Hash the password securely
        hashed_password = bcrypt.hashpw(password.encode('utf-8'), bcrypt.gensalt())

        # Create the user document
        user_document = {
            "name": name,
            "lastname": lastname,
            "username": username,
            "email": email,
            "phone_number": phone_number,
            "role": role,
            "photo": None,
            "password": hashed_password.decode('utf-8')  # Store as a string
        }

        # Insert the document into the collection
        result = collection.insert_one(user_document)
        logging.info(f"User saved successfully with ID: {result.inserted_id}")
        return result.inserted_id

    except DuplicateKeyError:
        logging.error(f"Username '{username}' already exists. Please choose a different username.")
        return None
    except Exception as e:
        logging.error(f"An error occurred while saving user info: {e}")
        return None
    
def admin_dash_data_recive(admin_id):
    """
    Retrieve admin information from the 'users' collection based on the provided admin ID.

    Args:
        admin_id (str or ObjectId): The unique identifier for the admin.

    Returns:
        dict or None: A dictionary containing the admin's name, role, and profile picture URL,
                      or None if the admin is not found.
    """
    try:
        # Access the 'users' collection
        collection = db['users']

        # Find the admin document by _id
        admin_info = collection.find_one({'_id': ObjectId(admin_id)})
        # database_log.info(f"this is database: {admin_info.get("photo", None)}")

        if admin_info:
            # Return only the necessary fields
            return {
                "name": admin_info.get("name", "Unknown"),
                "lastname": admin_info.get("lastname", "Unknown"),
                "role": admin_info.get("role", "Guest"),
                "photo": admin_info.get("photo", None)  # Fallback image
            }
            
        else:
            database_log.warning(f"No admin found with ID: {admin_id}")
            return None

    except Exception as e:
        database_log.error(f"An error occurred while retrieving admin info for ID {admin_id}: {e}")
        return None

def login_validation(username, password):
    """
    Retrieve user information from the 'users' collection based on the provided user ID and password.
    Passwords are securely hashed using bcrypt.

    Args:
        userid (str or ObjectId): The unique identifier for the user.
        password (str): The plain-text password provided by the user.

    Returns:
        dict or bool: The user document if the user ID and password are valid, otherwise False.
    """
    try:
        # Access the 'users' collection
        collection = db['users']
        
        # Find the user document by _id
        user_info = collection.find_one({'username': username})
        
        if user_info:
            # Check if the provided password matches the hashed password in the database
            if bcrypt.checkpw(password.encode('utf-8'), user_info['password'].encode('utf-8')):
                database_log.info(f"User info retrieved successfully for user: {username}")
                # Remove the password field from the returned document for security
                user_info.pop('password', None)
                if user_info['role'] == 'admin':
                    return 'admin'
                return 'user'
            
            else:
                database_log.warning(f"Invalid password for user : {username}")
                return False
        
        else:
            database_log.warning(f"No user found with : {username}")
            return False
    except Exception as e:
        # Log any exceptions that occur during the process
        database_log.error(f"An error occurred while retrieving user info for  {username}: {e}")
        return 
    
def load_dash(user_id):
    try:
        # Access the 'users' collection
        collection = db['users']
        
        # Find the user document by _id
        user_info = collection.find_one({'_id': ObjectId(user_id)})
        
        if user_info:
                user_info.pop('password', None)
                return True
                    
        else:
            database_log.warning(f"No user found with ID: {user_id}")
            return False
        
    except Exception as e:
        # Log any exceptions that occur during the process
        database_log.error(f"An error occurred while retrieving user info for ID {user_id}: {e}")
        return 

def get_ai(username):
    """
    Retrieve the user ID (_id) from the 'users' collection based on the provided username.

    Args:
        username (str): The username of the user.

    Returns:
        str or None: The user ID (_id) as a string if the user is found, otherwise None.
    """
    try:
        # Access the 'users' collection
        collection = db['users']

        # Find the user document by username
        user_info = collection.find_one({'username': username}, {'_id': 1})  # Projection to return only _id

        if user_info:
            database_log.info(f"User ID retrieved successfully for username: {username}")
            return str(user_info['_id'])  # Convert ObjectId to string
        else:
            database_log.warning(f"No user found with username: {username}")
            return None

    except Exception as e:
        database_log.error(f"An error occurred while retrieving user ID for username {username}: {e}")
        return None
    
def save_new_user(name, lastname, username, start_subscription, end_subscription, part_number, industry, phone_number,  province, city, additional_info,password ,  photo_base64=None):
    try:
        collection = db['users']

        hashed_password = bcrypt.hashpw(password.encode('utf-8'), bcrypt.gensalt())


        # Create the user document
        user_document = {
            "name": name,
            "lastname": lastname,
            "username": username,
            "role": "user",
            "phone_number":phone_number,
            "password": hashed_password.decode('utf-8'),  # Store as a stringm,
            "start_subscription": start_subscription,
            "end_subscription": end_subscription,
            "part_number": part_number,
            "industry": industry,
            "province": province,
            "city": city,
            "additional_info": additional_info,
            "photo": None,  # Initially set to None, will be updated later
            "devices": None
        }

        # Insert the document into the collection
        result = collection.insert_one(user_document)
        user_id = result.inserted_id  # Get the generated _id
        database_log.info(f"User saved successfully with ID: {user_id}")

        # If a photo is provided, update the photo field and save the file
        
        # Generate the filename using the user's _id
        filename = f"{user_id}.jpg"
        user_document["photo"] = filename  # Update the photo field     

        # Update the user document in the database with the new photo filename
        collection.update_one(
            {"_id": user_id},
            {"$set": {"photo": filename}}
        )
        database_log.info(f"Photo saved successfully as: {filename}")

        return user_id

    except DuplicateKeyError:
        database_log.error(f"Username '{username}' already exists. Please choose a different username.")
        return None
    except Exception as e:
        database_log.error(f"An error occurred while saving user info: {e}")
        return None
    
def update_user_photo(user_id, photo):
    """
    Update the photo field of a user in the database.

    Args:
        user_id (ObjectId): The ID of the user.
        photo (str): The photo URL or path.

    Returns:
        bool: True if the update was successful, False otherwise.
    """
    try:
        collection = db['users']
        result = collection.update_one(
            {'_id': ObjectId(user_id)},
            {'$set': {'photo': photo}}
        )
        return result.modified_count > 0
    except Exception as e:
        database_log.error(f"An error occurred while updating user photo: {e}")
        return False
    
def get_all_users():
    """
    Fetch all users with the role 'user' from the database.

    Returns:
        list: A list of user documents (excluding the password field).
              Returns an empty list if no users are found or an error occurs.
    """
    try:
        # Access the 'users' collection
        collection = db['users']

        # Fetch all users with the role 'user' and exclude the password field
        users = collection.find({'role': 'user'}, {'password': 0})

        # Convert the MongoDB cursor to a list of dictionaries
        users_list = list(users)

        return users_list

    except Exception as e:
        database_log.error(f"An error occurred while fetching users: {e}")
        return []
        
def get_user_details(user_id):
    """
    Fetch user details by user_id.

    Args:
        user_id (str): The ID of the user.

    Returns:
        dict: User details (excluding the password field).
              Returns None if the user is not found or an error occurs.
    """
    try:
        # Access the 'users' collection
        collection = db['users']

        # Find the user document by _id
        user = collection.find_one({'_id': ObjectId(user_id)}, {'password': 0})

        if user:
            # Convert ObjectId to string
            user['_id'] = str(user['_id'])
            return user
        else:
            database_log.warning(f"No user found with ID: {user_id}")
            return None

    except Exception as e:
        database_log.error(f"An error occurred while fetching user details for ID {user_id}: {e}")
        return None
    
def get_user_devices(user_id):
    """
    Fetch device details for a user by user_id.

    Args:
        user_id (str): The ID of the user.

    Returns:
        list: A list of device documents.
              Returns an empty list if no devices are found or an error occurs.
    """
    try:
        # Access the 'users' collection
        collection = db['users']

        # Find the user document by _id
        user = collection.find_one({'_id': ObjectId(user_id)}, {'devices': 1} )

        if user and 'devices' in user and user['devices'] is not None:
            return user['devices']
        else:
            database_log.warning(f"No devices found for user ID: {user_id}")
            return []

    except Exception as e:
        database_log.error(f"An error occurred while fetching devices for user ID {user_id}: {e}")
        return []
    
def add_device_and_update_user(user_id, device_data):
    """
    Adds a new device to the database and associates it with the specified user.
    
    Args:
        user_id: The ObjectId of the user to associate the device with
        device_data: Dictionary containing device details including:
                    - deviceName
                    - startDate
                    - endDate
                    - routine
                    - (optional) days
                    - (optional) repeats

    Returns:
        Dictionary with:
        - On success: {'status': 'success', 'device_id': str(device_id)}
        - On error: {'status': 'error', 'message': str(error)}
    """
    try:
        devices_collection = db['devices']
        users_collection = db['users']

        # Initialize device status and timestamps
        device_data.update({
            'status': 'غیرفعال',
            'can_run': False,
            'created_at': datetime.utcnow()
        })

        # Create new device record
        device_result = devices_collection.insert_one(device_data)
        device_id = device_result.inserted_id

        # Ensure user's devices array exists
        users_collection.update_one(
            {'_id': ObjectId(user_id), 'devices': None},
            {'$set': {'devices': []}}
        )

        # Associate device with user
        users_collection.update_one(
            {'_id': ObjectId(user_id)},
            {'$push': {
                'devices': str(device_id),
                'deviceNames': device_data['deviceName']
            }}
        )

        database_log.info(f"Added device {device_id} for user {user_id}")
        return {"status": "success", "device_id": str(device_id)}

    except Exception as e:
        database_log.error(f"Failed to add device: {str(e)}", exc_info=True)
        return {"status": "error", "message": str(e)}

def get_devices_by_user_id(user_id):
    """
    Fetch devices for a specific user from the database.

    Args:
        user_id (str): The ID of the user.

    Returns:
        list: A list of device documents.
    """
    try:

        # Fetch devices for the given userId
        devices = list(db.devices.find({"userId": user_id}))

        return devices
    except Exception as e:
        database_log.info(f"Error in get_devices_by_user_id: {str(e)}")  # Debug database_log.info
        raise
    
def get_device_details_db(device_id):
    """
    Fetch details of a specific device.

    Args:
        device_id (str): The ID of the device.

    Returns:
        dict: Device details or None if not found.
    """
    try:
        database_log.info(f"Fetching device details for device_id: {device_id}")  # Debug database_log.info

        # Access the 'devices' collection
        device = db.devices.find_one({"_id": ObjectId(device_id)})
        if device:
            return {
                "id": str(device["_id"]),  # Convert ObjectId to string
                "name": device.get("deviceName", "N/A"),
                "type": device.get("deviceType", "N/A"),
                "status": device.get("status", "N/A"),
                "endDate": device.get("endDate", "N/A"),
                "routine": device.get("routine", "N/A"),
                "created_at": device.get("created_at", "N/A"),
                "userId": device.get("userId", "N/A")
            }
        else:
            database_log.info("Device not found")  # Debug database_log.info
            return None
    except Exception as e:
        database_log.info(f"Error in get_device_details: {str(e)}")  # Debug database_log.info
        raise

def get_device_log_chart_data(device_id):
    """
    Fetch log chart data for a specific device.

    Args:
        device_id (str): The ID of the device.

    Returns:
        dict: Log chart data with "labels" and "values" keys.
    """
    try:
        # Aggregate logs by month
        log_chart_data = db.logs.aggregate([
            {"$match": {"deviceId": ObjectId(device_id)}},
            {"$addFields": {
                "time_date": {
                    "$dateFromString": {
                        "dateString": "$time",
                        "format": "%Y-%m-%d %H:%M"  # Match the format of your time field
                    }
                }
            }},
            {"$group": {
                "_id": {
                    "year": {"$year": "$time_date"},
                    "month": {"$month": "$time_date"}
                },
                "count": {"$sum": 1}
            }},
            {"$sort": {"_id.year": 1, "_id.month": 1}}  # Sort by year and month
        ])

        # Format the data for the chart
        labels = []
        values = []
        for entry in log_chart_data:
            year = entry["_id"]["year"]
            month = entry["_id"]["month"]
            labels.append(f"{year}-{month:02d}")  # Format as "YYYY-MM"
            values.append(entry["count"])

        return {"labels": labels, "values": values}
    except Exception as e:
        database_log.info(f"Error fetching log chart data: {str(e)}")
        return {"labels": [], "values": []}

def get_device_log_table_data(device_id):
    """
    Fetch log table data for a specific device.

    Args:
        device_id (str): The ID of the device.

    Returns:
        list: Log table data with "timestamp", "event", and "status" keys.
    """
    try:
        logs = db.logs.find({"deviceId": ObjectId(device_id)})
        log_table_data = []
        for log in logs:
            log_table_data.append({
                "timestamp": log.get("time", "N/A"),  # Use "time" field from the log document
                "event": log.get("event", "N/A"),
                "status": log.get("status", "N/A")
            })
        return log_table_data
    except Exception as e:
        database_log.info(f"Error fetching log table data: {str(e)}")
        return []
    
def store_log(device_id, msg_status , msg_event, time = None):
    """
    Stores a log document in the MongoDB log collection.

    Args:
        device_id (str): The ID of the device generating the log.
        msg_type (str): The type of log message (e.g., "error", "warning", "info").
        time (str): The timestamp of the log in the format "YYYY-MM-DD HH:MM".

    Returns:
        bool: True if the log was inserted successfully, False otherwise.
    """
    if time == None:
        time = datetime.now()
    log_collection = db["logs"]
    
    # Validate device_id
    if not ObjectId.is_valid(device_id):
        database_log.info(f"Invalid device_id: {device_id}")
        return False

    # Validate time format
      # If time is a datetime object, convert it to a string in the desired format
    if isinstance(time, datetime):
        time_str = time.strftime("%Y-%m-%d %H:%M")
    # If time is already a string, assume it's in the correct format
    elif isinstance(time, str):
        time_str = time
    else:
        raise ValueError("Invalid time format. Expected datetime object or string.")

    # Create the log document
    log_document = {
        "deviceId": ObjectId(device_id),  # Convert to ObjectId
        "event": msg_event,
        "status": msg_status,
        "time": time_str
    }

    # Insert the log document into the collection
    try:
        result = log_collection.insert_one(log_document)
        database_log.info(f"Log inserted with ID: {result.inserted_id}")
        return True
    
    except Exception as e:
        database_log.info(f"Error inserting log: {e}")
        return False
    

def update_device_details(device_id, updated_data):
    """
    Update device details in the database.

    Args:
        device_id (str): The ID of the device.
        updated_data (dict): A dictionary containing the updated fields.

    Returns:
        bool: True if the update was successful, False otherwise.
    """
    try:
        # Access the 'devices' collection
        collection = db['devices']
        database_log.info(f"removing device {device_id} collections for changing the device details")
        scheduler.delete_device_data(device_id=device_id)
        # Update the device document
        result = collection.update_one(
            {"_id": ObjectId(device_id)},
            {"$set": updated_data}
        )

        if result.modified_count > 0:
            database_log.info(f"Device details updated successfully: {device_id}")
            return True
        
        else:
            database_log.warning(f"No changes made to device: {device_id}")
            return False
    except Exception as e:
        database_log.error(f"An error occurred while updating device details: {e}")
        return False
        
def get_device_details_for_edit(device_id):
    """
    Fetch details of a specific device.

    Args:
        device_id (str): The ID of the device.

    Returns:
        dict: Device details or None if not found.
    """
    try:
        # Access the 'devices' collection
        device = db.devices.find_one({"_id": ObjectId(device_id)})
        if device:
            return {
                "id": str(device["_id"]),  # Convert ObjectId to string
                "name": device.get("deviceName", "N/A"),
                "type": device.get("deviceType", "N/A"),
                "startDate": device.get("startDate", "N/A"),
                "endDate": device.get("endDate", "N/A"),
                "routine": device.get("routine", "N/A"),
                "days": device.get("days", "N/A"),  # Add days field
                "repeats": device.get("repeats", "N/A"),  # Add repeats field
                "status": device.get("status", "N/A"),
                "userId": device.get("userId", "N/A")
            }
        else:
            database_log.warning(f"No device found with ID: {device_id}")
            return None
    except Exception as e:
        database_log.error(f"An error occurred while fetching device details: {e}")
        return None
    
def update_user_without_new_id(user_id, updated_data):
    """
    Update user information without changing the _id field.

    Args:
        user_id (str): The current _id of the user.
        updated_data (dict): The updated user data.

    Returns:
        bool: True if the update was successful, False otherwise.
    """
    try:
        database_log.info(f"Updating user {user_id} without changing the ID")  # Debug database_log.info
        database_log.info(f"Updated data: {updated_data}")  # Debug database_log.info

        # Access the 'users' collection
        collection = db['users']

        # Remove the _id field from the updated data (to avoid modifying it)
        updated_data.pop('_id', None)

        # Hash the password if it is provided
        if 'password' in updated_data:
            database_log.info("Hashing the password")  # Debug database_log.info
            updated_data['password'] = bcrypt.hashpw(updated_data['password'].encode('utf-8'), bcrypt.gensalt()).decode('utf-8')

        # Update the user document
        result = collection.update_one(
            {'_id': ObjectId(user_id)},
            {'$set': updated_data}
        )

        if result.modified_count > 0:
            database_log.info(f"User {user_id} updated successfully")  # Debug database_log.info
            database_log.info(f"User {user_id} updated successfully")
            return True
        else:
            database_log.info(f"No changes made to user {user_id}")  # Debug database_log.info
            database_log.warning(f"No changes made to user {user_id}")
            return False

    except KeyError as e:
        database_log.info(f"Missing required field in updated data: {e}")  # Debug database_log.info
        database_log.error(f"Missing required field in updated data: {e}")
        return False
    except Exception as e:
        database_log.info(f"An error occurred while updating user {user_id}: {e}")  # Debug database_log.info
        database_log.error(f"An error occurred while updating user {user_id}: {e}")
        return False
    
def update_user_with_new_id(old_id, new_id, updated_data):
    """
    Update user information by changing the _id field.

    Args:
        old_id (str): The current _id of the user.
        new_id (str): The new _id for the user.
        updated_data (dict): The updated user data.

    Returns:
        bool: True if the update was successful, False otherwise.
    """
    try:
        # Access the 'users' collection
        collection = db['users']

        # Fetch the existing user document
        user = collection.find_one({'_id': ObjectId(old_id)})
        if not user:
            database_log.warning(f"No user found with ID: {old_id}")
            return False

        # Update the _id field in the user document
        updated_data['_id'] = ObjectId(new_id)

        # Insert the new document with the updated _id
        collection.insert_one(updated_data)

        # Delete the old document
        collection.delete_one({'_id': ObjectId(old_id)})

        database_log.info(f"User _id updated from {old_id} to {new_id}")
        return True

    except Exception as e:
        database_log.error(f"An error occurred while updating user _id: {e}")
        return False
    
def get_user_by_username(username):
    """
    Fetch user information by username.

    Args:
        username (str): The username of the user.

    Returns:
        dict: User details (excluding the password field).
              Returns None if the user is not found.
    """
    try:
        # Access the 'users' collection
        collection = db['users']

        # Find the user document by username
        user = collection.find_one({'username': username}, {'password': 0})  # Exclude password

        if user['role'] == 'admin':
            return None
        
        if user:
            # Convert ObjectId to string
            user['_id'] = str(user['_id'])
            return user
        else:
            database_log.warning(f"No user found with username: {username}")
            return None

    except Exception as e:
        database_log.error(f"An error occurred while fetching user info for username {username}: {e}")
        return None

def create_indexes():
    try:
        db['process'].create_index([('device_id', 1)], unique=True, background=True)
        database_log.info("Index created on device_id field")
    except Exception as e:
        database_log.warning(f"Index creation warning: {e}")

def deactivate_device(device_id):
    """
    Deactivate a device by updating its status and can_run fields in the devices collection.

    Args:
        device_id (str): The ID of the device to deactivate.

    Returns:
        bool: True if the update was successful, False otherwise.
    """
    try:
        devices_collection = db['devices']

        # Define the update data
        update_data = {
            'status': 'غیرفعال',  # Persian for "inactive"
            'can_run': False
        }

        # Update the document
        result = devices_collection.update_one(
            {'_id': ObjectId(device_id)},  # Filter by device_id
            {'$set': update_data}          # Use $set to update specific fields
        )

        # Check if the update was successful
        if result.matched_count > 0:
            database_log.info(f"Device {device_id} deactivated successfully")
            return True
        else:
            database_log.warning(f"No device found with ID: {device_id}")
            return False

    except Exception as e:
        database_log.error(f"Error deactivating device {device_id}: {str(e)}")
        return False

def activate_device(device_id):
    """
    Activate a device by updating its status and can_run fields in the devices collection.

    Args:
        device_id (str): The ID of the device to activate.

    Returns:
        bool: True if the update was successful, False otherwise.
    """
    try:
        devices_collection = db['devices']

        # Define the update data
        update_data = {
            'status': 'فعال',  # Persian for "active"
            'can_run': True
        }

        # Update the document
        result = devices_collection.update_one(
            {'_id': ObjectId(device_id)},  # Filter by device_id
            {'$set': update_data}          # Use $set to update specific fields
        )

        # Check if the update was successful
        if result.matched_count > 0:
            database_log.info(f"Device {device_id} activated successfully")
            return True
        else:
            database_log.warning(f"No device found with ID: {device_id}")
            return False

    except Exception as e:
        database_log.error(f"Error activating device {device_id}: {str(e)}")
        return False

def get_mod_status(device_id):
        """
        Check and reset the modification status for a device in the purgatory collection.
        Args:
            device_id (str): The unique identifier for the device.
        Returns:
            false if the mod is not false return none if the collection is not found for the device id.
        """
        try:
            purgatory_col = db['purgatory_col']

            data = purgatory_col.find_one(
                {'device_id': device_id},
                {'mod': 1, 'audio_path': 1, 'image_path': 1 , 'kind' : 1}
            )
            if data and data.get('mod', False):
                # Update mod to False
                result = purgatory_col.update_one(
                    {'device_id': device_id},
                    {'$set': {'mod': False}}
                )
                
                if result.modified_count > 0:
                    class_logs.info(f"Reset mod status to False for device_id: {device_id}")
                    data['status'] = 'yes'
                    return data
                
                else:
                    class_logs.warning(f"Failed to reset mod status for device_id: {device_id}")
                    return None
            
            else:
                class_logs.info(f"No mod status or mod is False for device_id: {device_id}")
                data['status'] = 'no'
                return data
        except Exception as e:
            class_logs.error(f"Error in _get_mod_status for device_id {device_id}: {str(e)}")
            return None

def save_to_mongo(instruction, device_id, audio_path=None, image_path=None):
    """
    Save or update a document in the purgatory collection based on device_id.
    Enhanced with detailed logging to track two specific bugs.
    
    Args:
        instruction (str): The instruction type
        device_id (str): The unique identifier for the device
        audio_path (str, optional): Path to the audio file
        image_path (str, optional): Path to the image file
    
    Returns:
        bool: True if successful, False otherwise
    """
    # Initialize tracking variables for our two bugs
    bug1_triggered = False  # For tracking document existence issues
    bug2_triggered = False  # For tracking scheduling issues
    
    database_log.info(f"STARTING save_to_mongo for device {device_id} with instruction {instruction} \naudio_path: {audio_path}\nimage_path: {image_path}")

    try:
        purgatory_col = db['purgatory_col']
        database_log.debug(f"Accessed purgatory_col for device {device_id}")

        # Determine the 'kind' with logging
        if instruction in ['instruction-A', 'instruction-S'] or (instruction == 'custom' and image_path):
            kind = 'with_image'
            database_log.debug(f"Determined kind=with_image for device {device_id}")
        else:
            kind = 'audio_only'
            database_log.debug(f"Determined kind=audio_only for device {device_id}")

        # Prepare the document with null checks
        document = {
            'device_id': device_id,
            'kind': kind,
            'mod': True,
            'audio_path': audio_path,
            'image_path': image_path,
            'instruction': instruction,
            'updated_at': datetime.now()
        }
        database_log.debug(f"Prepared document for device {device_id}: {document}")

        # Check for existing document with detailed logging
        existing_doc = purgatory_col.find_one({'device_id': device_id})
        document_exists = existing_doc is not None
        
        database_log.info(f"Document existence check for {device_id}: {'EXISTS' if document_exists else 'NEW'}")

        # Track Bug #1: Document existence flag mismatch
        if document_exists:
            database_log.info("Existing document found, will update")
        else:
            database_log.info("No existing document, will create new")
            bug1_triggered = True  # Potential bug tracking point

        # Upsert operation with result logging
        result = purgatory_col.update_one(
            {'device_id': device_id},
            {'$set': document},
            upsert=True
        )
        database_log.debug(f"Upsert result for {device_id}: {result.raw_result}")

        if result.matched_count > 0:
            database_log.info(f"Updated existing document for {device_id}")
        elif result.upserted_id:
            database_log.info(f"Created new document for {device_id}")
        else:
            database_log.warning(f"Unexpected upsert result for {device_id}")
            bug1_triggered = True  # Potential bug tracking point

        task_col = db['tasks']
        task_col.update_one({'device_id' : device_id} , {'$set' : {'image_path' : image_path}})

        # Device activation with error handling
        try:
            activation_result = activate_device(device_id)
            database_log.info(f"Device activation for {device_id}: {'SUCCESS' if activation_result else 'FAILED'}")
        except Exception as e:
            database_log.error(f"Device activation failed for {device_id}: {str(e)}")
            bug2_triggered = True  # Potential bug tracking point

    except Exception as e:
        database_log.error(f"MAIN TRY BLOCK FAILED for {device_id}: {str(e)}", exc_info=True)
        return False

    finally:
        database_log.info(f"ENTERING FINALLY BLOCK for {device_id}")
        
        # Only execute scheduling if document didn't exist before
        if not document_exists:
            database_log.info(f"PROCEEDING TO SCHEDULING for {device_id}")
            
            try:
                device_details = get_device_details_for_edit(device_id)
                database_log.debug(f"Retrieved device details for {device_id}: {device_details}")

                if not device_details:
                    database_log.warning(f"No device details found for {device_id}")
                    bug2_triggered = True
                    return True

                routine = device_details.get('routine', instruction)
                expiration = device_details.get('endDate')
                repeat_hours = device_details.get('repeats')

                # Log parameter determination
                database_log.info(
                    f"Scheduling parameters for {device_id}:\n"
                    f"Routine: {routine}\n"
                    f"Expiration: {expiration}\n"
                    f"Repeat hours: {repeat_hours}\n"
                    f"Image path: {image_path}"
                )

                # Handle expiration conversion
                if isinstance(expiration, str):
                    try:
                        expiration = datetime.fromisoformat(expiration)
                        database_log.debug(f"Converted expiration to datetime: {expiration}")
                    except ValueError as e:
                        database_log.warning(f"Failed to parse expiration {expiration}: {str(e)}")
                        expiration = None
                        bug2_triggered = True

                # Handle repeat_hours conversion
                if isinstance(repeat_hours, str) and repeat_hours.isdigit():
                    repeat_hours = int(repeat_hours)
                    database_log.debug(f"Converted repeat_hours to int: {repeat_hours}")
                else:
                    database_log.warning(f"Invalid repeat_hours format: {repeat_hours}")
                    repeat_hours = None
                    bug2_triggered = True

                # Schedule the task with parameter validation
                if not all([device_id, routine]):
                    database_log.error(f"Missing required parameters for scheduling {device_id}")
                    bug2_triggered = True
                else:
                    try:
                        scheduler.add_task(
                            device_id=device_id,
                            image_path=image_path,
                            routine=routine,
                            expiration=None,
                            repeat_hours=repeat_hours,
                            next_execution=None
                        )
                        database_log.info(f"Successfully scheduled task for {device_id}")
                    except Exception as e:
                        database_log.error(f"Scheduling failed for {device_id}: {str(e)}")
                        bug2_triggered = True

                # Store log with error handling
                try:
                    data = get_mod_status(device_id)  
                    
                    # Send merge request based on image status
                    if data['kind'] == 'with_image':
                        _send_merge_request_offline(device_id , image_path   , audio_path=data['audio_path'])
                                        
                    elif data['kind'] == 'audio_only':
                        _send_merge_request_offline(device_id , image_path=None   , audio_path=data['audio_path'])

                    database_log.debug(f"Successfully stored log for {device_id}")
                except Exception as e:
                    database_log.error(f"Failed to store log for {device_id}: {str(e)}")
                    bug2_triggered = True

            except Exception as e:
                database_log.error(f"FINALLY BLOCK FAILED for {device_id}: {str(e)}")
                bug2_triggered = True
        else:
            database_log.info(f"SKIPPED SCHEDULING for {device_id} (document existed)")

        # Final bug status reporting
        if bug1_triggered or bug2_triggered:
            database_log.warning(
                f"POTENTIAL BUGS DETECTED for {device_id}:\n"
                f"Bug #1 (Document existence): {'TRIGGERED' if bug1_triggered else 'OK'}\n"
                f"Bug #2 (Scheduling): {'TRIGGERED' if bug2_triggered else 'OK'}"
            )
        else:
            database_log.info(f"Operation completed successfully for {device_id} with no bugs detected")

    return True

def update_output_path(device_id: str, path: str) -> bool:
    """
    Update the output path for a device in the database.
    
    Args:
        device_id: String representation of device ObjectId
        path: Output path to be stored
        
    Returns:
        boolen
    """
    try:
        # Validate input format
        if not isinstance(device_id, str):
            raise ValueError("Invalid device ID format")
            
        if not isinstance(path, str):
            raise ValueError("Output path must be a string")

        devices_collection = db['devices']
        obj_id = ObjectId(device_id)
        
        result = devices_collection.update_one(
            {'_id': obj_id},
            {'$set': {'output': path}}
        )
        
        if result.matched_count == 0:
            database_log.warning(f"Device {device_id} not found")
            return False
            
        if result.modified_count > 0:
            database_log.info(f"Successfully updated output path for device {device_id}")
            return True
            
        database_log.info(f"No changes made to device {device_id}")
        return True

    except pymongo_errors.PyMongoError as e:
        database_log.error(f"Database error updating device {device_id}: {str(e)}")
        return False
        
    except ValueError as e:
        database_log.error(f"Validation error: {str(e)}")
        return False
        
    except Exception as e:
        database_log.critical(f"Unexpected error updating device {device_id}: {str(e)}")
        return False

def read_output_path(device_id: str) -> bool | str:
    """
    Retrieve the output path for a device from the database.
    
    Args:
        device_id: String representation of device ObjectId
        
    Returns:
        boolen
    """
    try:
        if not isinstance(device_id, str):
            raise ValueError("Invalid device ID format")

        devices_collection = db['devices']
        obj_id = ObjectId(device_id)
        
        document = devices_collection.find_one({'_id': obj_id})
        
        if not document:
            database_log.warning(f"Device {device_id} not found")
            return False
            
        output_path = document.get('output')
        
        if output_path:
            database_log.debug(f"Retrieved output path for device {device_id}")
            return output_path
            
        database_log.info(f"No output path set for device {device_id}")
        return False

    except pymongo_errors.PyMongoError as e:
        database_log.error(f"Database error reading device {device_id}: {str(e)}")
        return False
        
    except ValueError as e:
        database_log.error(f"Validation error: {str(e)}")
        return False
        
    except Exception as e:
        database_log.critical(f"Unexpected error reading device {device_id}: {str(e)}")
        return False
        
def _send_merge_request_offline(device_id, image_path, audio_path):
    try:
        path = merger(audio_path , device_id , image_path)
        
        if os.path.exists(path):
            if update_output_path(device_id , path):
                store_log(device_id , "مرج با موفقیت انجام شد" , "اطلاع" )
                threading.Thread(target=play, args=(device_id,), daemon=True).start()
          
            else:
                store_log(device_id, "خطا در ذخیره‌سازی داده‌ها. لطفاً با واحد فنی تماس بگیرید.", "خطا")      

        else:
            store_log(device_id, "خطا در ذخیره‌سازی داده‌ها. لطفاً با واحد فنی تماس بگیرید.", "خطا")      

    except Exception as e:
            store_log(device_id, "خطا در ذخیره‌سازی داده‌ها. لطفاً با واحد فنی تماس بگیرید.", "خطا")      
            database_log.error(e)              

    
    
def retrieve_from_mongo(device_id):
    """
    Retrieve information from the purgatory collection for a given device_id.
    
    Args:
        device_id (str): The unique identifier for the device (primary key).
    
    Returns:
        dict: The document containing device information, or None if not found or an error occurs.
    """
    try:
        purgatory_col = db['purgatory_col']
        document = purgatory_col.find_one({'device_id': device_id})
        
        if document:
            document.pop('_id', None)
            database_log.info(f"Retrieved document for device_id: {device_id}: {document}")
            return document
        else:
            database_log.info(f"No document found for device_id: {device_id}")
            return None

    except Exception as e:
        database_log.info(f"Error retrieving from purgatory collection for device_id {device_id}: {str(e)}")
        return None

def save_merge_info(device_id, image_address, data):
    try:
        collection = db['process']
        update_data = {
            '$set': {'image': image_address},
            '$setOnInsert': {
                'device_id': device_id,
                'status': None,
                'phase': None,
                'mod': True,  # True for new documents
                'routine': None,
                'days': None,
                'repeats': None
            },

            '$currentDate': {
                'last_modified': True  # Uses MongoDB server time
            }
        }

        # Existing document updates
        if 'routine' in data:
            update_data['$set']['routine'] = data['routine']
            update_data['$set']['mod'] = True  # Only set mod=True if routine changes

        if 'days' in data:
            update_data['$set']['days'] = data['days']
        if 'repeats' in data:
            update_data['$set']['repeats'] = data['repeats']

        result = collection.update_one(
            {'device_id': device_id},
            update_data,
            upsert=True
        )
        return result.acknowledged

    except Exception as e:
        database_log.error(f"Database error: {e}")
        return False

def get_device_info(device_id):
    """
    Fetch device information from the database.

    Args:
        device_id (str): The device ID to search for.

    Returns:
        dict: A dictionary containing device information if found.
        None: If the device is not found or an error occurs.
    """
    try:
        collection = db['process']
        document = collection.find_one(
            {'device_id': device_id},
            {
                '_id': 0,  # Exclude MongoDB ID
                'days': 1,
                'repeats': 1,
                'routine': 1,
                'mod': 1,
                'phase': 1,
                'status': 1,
                'image': 1,
                'last_modified': 1
            }
        )

        if not document:
            database_log.warning(f"Device not found: {device_id}")
            return None

        # Convert MongoDB document to Python dict
        result = {
            'days': document.get('days'),
            'repeats': document.get('repeats'),
            'routine': document.get('routine'),
            'mod': document.get('mod', False),
            'phase': document.get('phase'),
            'status': document.get('status'),
            'image': document.get('image'),
            'last_modified': document.get('last_modified').isoformat() if document.get('last_modified') else None
        }

        return result

    except Exception as e:
        database_log.error(f"Error fetching device info for {device_id}: {e}")
        return None

def user_exists(username):
    """Check if a user exists in the database by username."""
    users_collection = db['users']
    return users_collection.count_documents({'username': username}, limit=1) > 0

def create_admin():
    try:
        user_data = {
            "name": "امیر",
            "lastname": "امینی",
            "role": "admin",
            "photo": "1.jpg",
            "password": "$2b$12$asEUmAoh4lIyQX8cZr40R.LYOXW0.P/zPZawchldJqr9XeQZhhZqi",
            "username": "admin",
            "devices": [],
            "deviceNames": [],
            "created_at": datetime.utcnow()
        }

        if user_exists(user_data['username']):
            return {'status': 'exists', 'message': 'User already exists'}

        users_collection = db['users']
        result = users_collection.insert_one(user_data)
        user_id = result.inserted_id

        database_log.info(f"Created new user {user_id} (admin)")
        return {'status': 'success', 'user_id': str(user_id)}

    except Exception as e:
        database_log.error(f"Failed to create user: {str(e)}", exc_info=True)
        return {'status': 'error', 'message': str(e)}

def delete_user(user_id):
    """
    Completely delete a user and all associated data including:
    - User profile and image
    - All user devices
    - Device media files (audio/images)
    - Device logs
    - Purgatory entries
    - Scheduled tasks
    """
    try:
        users_collection = db['users']
        # Verify user exists
        user = users_collection.find_one({'_id': ObjectId(user_id)})
        if not user:

            return False

        # 1. Delete user profile image
        if user.get('photo'):
            profile_image_path = os.path.join('static', 'profile_images', user['photo'])
            if os.path.exists(profile_image_path):
                try:
                    os.remove(profile_image_path)

                except Exception as e:
                    print(f"Error deleting profile image: {e}")

        # 2. Delete all user devices and their data
        device_ids = user.get('devices', [])
        for device_id in device_ids:
            try:
                # Get device info to find media paths
                device = db.devices.find_one({'_id': ObjectId(device_id)})
                if device:
                    # Delete device media files
                    for media_type in ['audio_path', 'image_path']:
                        if device.get(media_type):
                            media_path = device[media_type]
                            if os.path.exists(media_path):
                                try:
                                    os.remove(media_path)

                                except Exception as e:
                                    print(f"Error deleting {media_type}: {e}")

                    # Delete device directory if exists
                    device_dir = os.path.join('static', 'devices', device_id)
                    if os.path.exists(device_dir):
                        try:
                            for filename in os.listdir(device_dir):
                                file_path = os.path.join(device_dir, filename)
                                try:
                                    if os.path.isfile(file_path):
                                        os.remove(file_path)
                                except Exception as e:
                                    print(f"Error deleting device file {file_path}: {e}")
                            os.rmdir(device_dir)

                        except Exception as e:
                            print(f"Error removing device directory: {e}")

                # Delete database entries
                db.logs.delete_many({'deviceId': ObjectId(device_id)})
                db.purgatory_col.delete_many({'device_id': device_id})
                db.tasks.delete_many({'device_id': device_id})
                db.devices.delete_one({'_id': ObjectId(device_id)})


            except Exception as e:
                print(f"Error cleaning up device {device_id}: {e}")
                continue

        # 3. Finally delete the user
        result = users_collection.delete_one({'_id': ObjectId(user_id)})
        if result.deleted_count > 0:

            return True
        return False

    except Exception as e:
        print(f"Fatal error deleting user {user_id}: {e}")
        return False


def play(device_id: str) -> bool:
    """
    Play the video file from the configured output path of a device.
    
    Args:
        device_id: String representation of device ObjectId
        
    Returns:
        bool: True if playback succeeded, False otherwise
    """
    try:
        if not isinstance(device_id, str):
            raise ValueError("Invalid device ID format")

        path_result = read_output_path(device_id)
        
        if isinstance(path_result, bool) and not path_result:
            database_log.error(f"No output path configured for device {device_id}")
            return False
            
        path = str(path_result)  
        
        # Verify file existence
        if not os.path.exists(path):
            database_log.warning(f"Path not found: {path}")
            return False

        try:
            open_video_file(path)
        except OSError as e:
            database_log.error(f"File access error: {str(e)}")
            return False
            
        database_log.info(f"Successfully played file for device {device_id}")
        return True

    except ValueError as e:
        database_log.error(f"Validation error: {str(e)}")
        return False
        
    except Exception as e:
        database_log.critical(f"Unexpected playback error for device {device_id}: {str(e)}")
        return False

create_indexes()
create_admin()
scheduler = PersistentTaskScheduler()

