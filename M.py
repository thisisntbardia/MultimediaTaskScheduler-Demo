import sched
import time
from datetime import datetime, timedelta
import os
import sys
import heapq
import psutil
import threading
from shutil import copyfile
import platform
import ffmpeg 
from PIL import Image, UnidentifiedImageError
import logging
from moviepy.editor import ImageClip, AudioFileClip, concatenate_audioclips
import multiprocessing
from pathlib import Path
import tempfile
from pydub import AudioSegment
from mutagen.mp3 import MP3
from mutagen.id3 import ID3
from mutagen.flac import FLAC
from mutagen.aac import AAC
from mutagen.m4a import M4A

current_dir = os.path.dirname(sys.executable) if getattr(sys, 'frozen', False) else os.path.dirname(os.path.abspath(__file__))
scheduler = sched.scheduler(time.time, time.sleep)
logging.basicConfig(
    filename='logs.txt',
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

ALLOWED_AUDIO_EXTENSIONS = [
    '.mp3', '.wav', '.flac', '.aac', 
    '.m4a', '.wma', '.alac'
]
ALLOWED_IMAGE_EXTENSIONS = [
    '.png', '.jpg', '.jpeg', '.bmp', '.gif', 
    '.tiff', '.webp', '.heif', '.svg'
]
AUDIO_DIR = os.path.join(current_dir, 'Audio')
IMAGE_DIR = os.path.join(current_dir, 'Image')
OUTPUT_DIR = os.path.join(current_dir, 'Merge')
TEMP_DIR = os.path.join(current_dir, 'Temp')

task_queue = []
task_lock = threading.Lock()

def init():
    logging.info(f"Current directory determined dynamically: {current_dir}")

    logging.getLogger("moviepy").setLevel(logging.ERROR)
    logging.info("MoviePy logging level set to ERROR.")

    logging.info("Logging configuration set successfully.")

    logging.info("Allowed file formats for audio and images defined.")

    os.makedirs(AUDIO_DIR, exist_ok=True)
    os.makedirs(IMAGE_DIR, exist_ok=True)
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    os.makedirs(TEMP_DIR, exist_ok=True)
    logging.info("Necessary directories ensured: audio, image, and merge.")

def is_valid_format(file_name, allowed_extensions):
    """
    Checks whether the file format is valid based on the allowed extensions.
    
    This function compares the file's extension (case-insensitive) against a list 
    of allowed extensions. It returns `True` if the file's format is valid, 
    and `False` otherwise. It logs the result of the validation process.
    
    Args:
        file_name (str): The name of the file to check.
        allowed_extensions (list): A list of allowed file extensions (e.g., ['.mp3', '.jpg']).
    
    Returns:
        bool: `True` if the file format is valid, otherwise `False`.
    """
    try:
        # Check if the file has a valid extension by comparing it to allowed extensions
        valid = any(file_name.lower().endswith(ext) for ext in allowed_extensions)
        
        # Log the validation result
        logging.debug(f"File validation for '{file_name}': {'valid' if valid else 'invalid'}")
        
        return valid
    except Exception as e:
        # Log any errors that occur during validation
        logging.error(f"Error during file format validation for '{file_name}': {e}", exc_info=True)
        return False

def open_video_file(video_path):
    """
    Opens a video file using the appropriate media player based on the operating system.
    
    The function attempts to open the video file using the default media player for 
    the detected operating system (Windows, macOS, or Linux). After opening, it waits 
    for 1 minute, then closes the media player. It logs the entire process, including 
    any errors encountered.

    Args:
        video_path (str): The path to the video file to open.
    
    Returns:
        None
    """
    try:
        logging.info(f"Attempting to open video file: {video_path}")

        if platform.system() == "Windows":
            os.system(f'start wmplayer "{video_path}"')
        elif platform.system() == "Darwin":  # macOS
            os.system(f"open -a 'QuickTime Player' '{video_path}'")
        elif platform.system() == "Linux":
            os.system(f"xdg-open '{video_path}'")
        else:
            raise OSError("Unsupported operating system.")
        
        logging.info(f"Successfully opened video file: {video_path}")
        time.sleep(62)

        if platform.system() == "Windows":
            os.system("taskkill /F /IM wmplayer.exe")
        elif platform.system() == "Darwin":  # macOS
            os.system("osascript -e 'tell application \"QuickTime Player\" to quit'")
        elif platform.system() == "Linux":
            os.system("pkill -f 'xdg-open'")  

        logging.info("Media player closed after 1 minute.")
    
    except Exception as e:
        logging.error(f"Error handling video playback for video '{video_path}': {e}", exc_info=True)

def find_file(directory, file_name):
    """
    Searches for a specified file within a given directory and its subdirectories.

    This function traverses the directory recursively to locate the specified file. 
    If the file is found, its full path is returned; otherwise, it returns `None`. 
    The function logs both the search process and the outcome (whether the file was found or not).

    Args:
        directory (str): The directory to search within.
        file_name (str): The name of the file to search for.
    
    Returns:
        str or None: The full path to the file if found, otherwise `None`.
    """
    try:
        # Log the start of the file search
        logging.info(f"Searching for file '{file_name}' in directory: {directory}")

        # Iterate over the files in the directory (and subdirectories)
        for root, _, files in os.walk(directory):
            if file_name in files:
                # Log the successful file search
                logging.info(f"File '{file_name}' found in directory: {root}")
                return os.path.join(root, file_name)
        
        # Log if the file was not found
        logging.warning(f"File '{file_name}' not found in directory: {directory}")
        return None
    
    except Exception as e:
        # Log the error if there was an issue with the file search
        logging.error(f"Error searching for file '{file_name}' in directory '{directory}': {e}", exc_info=True)
        return None

def schedule_task(func, args, execute_at):
    """
    Schedules a task to be executed at a specific time by adding it to the priority queue.
    The task will be executed when the current time matches or exceeds the specified 
    execution time. The priority queue ensures tasks are executed in the correct order 
    based on their scheduled execution times.

    Args:
        func (function): The function to be executed when the scheduled time arrives.
        args (tuple): A tuple of arguments to pass to the function when it is executed.
        execute_at (datetime): The time at which the task should be executed.

    Returns:
        None
    """
    try:
        # Acquire lock to safely add the task to the priority queue
        with task_lock:
            # Add the task to the queue with its execution time, function, and arguments
            heapq.heappush(task_queue, (execute_at, func, args))
        
        # Log the task scheduling event
        logging.info(f"Task {func.__name__} scheduled for execution at {execute_at}.")
    
    except Exception as e:
        # Log any errors that occur during task scheduling
        logging.error(f"Error scheduling task {func.__name__} for execution at {execute_at}: {e}", exc_info=True)

def run_scheduler():
    """
    Continuously runs the scheduler to execute tasks at their scheduled times. 
    The scheduler checks the task queue, and when the time arrives for a task, 
    it executes the corresponding function. If an error occurs while executing 
    a task, it logs the error and moves on to the next task.
    
    The scheduler runs indefinitely, checking for tasks every second.

    Returns:
        None
    """
    try:
        logging.info("Scheduler has been started and is running continuously.")
        
        # Continuously check and process scheduled tasks
        while True:
            now = datetime.now()

            while task_queue and task_queue[0][0] <= now:
                with task_lock:
                    execute_at, func, args = heapq.heappop(task_queue)

                try:
                    func(*args)
                    logging.info(f"Task {func.__name__} executed successfully at {now}.\n\n")
                except Exception as e:
                    logging.error(f"Error occurred while executing task '{func.__name__}': {e}", exc_info=True)
            
            time.sleep(1)

    except Exception as e:
        # Log any errors that occur within the scheduler itself
        logging.error(f"An error occurred while running the scheduler: {e}", exc_info=True)

def remove_metadata(file_path):
    """Remove metadata from an audio file."""
    try:
        if file_path.endswith('.mp3'):
            audio = MP3(file_path, ID3=ID3)
            audio.delete()
            audio.save(file_path)

        elif file_path.endswith('.flac'):
            audio = FLAC(file_path)
            audio.clear()  
            audio.save(file_path)

        elif file_path.endswith('.aac'):
            audio = AAC(file_path)
            audio.delete()  
            audio.save(file_path)
 
        elif file_path.endswith('.m4a'):
            audio = M4A(file_path)
            audio.delete()  
            audio.save(file_path)
        else:
            pass
  
    except Exception as e:
        print(f"Error removing metadata from {file_path}: {e}")

def convert_to_mp3(file_path):
    """Convert audio file to MP3 format."""
    try:
        audio = AudioSegment.from_file(file_path)
        # Create a temporary file to store the converted MP3
        temp_file = tempfile.NamedTemporaryFile(delete=False, suffix='.mp3')
        audio.export(temp_file.name, format='mp3')
        return temp_file.name
    except Exception as e:
        print(f"Error converting file {file_path} to MP3: {e}")
        return None

def process_audio(file_path):
    """Process the audio file, remove metadata, and return a temporary file."""    
    temp_file_to_edit = tempfile.NamedTemporaryFile(delete=False, suffix='.mp3', dir=TEMP_DIR)

    try:
        copyfile(file_path, temp_file_to_edit.name)
        remove_metadata(temp_file_to_edit.name)

        file_extension = os.path.splitext(file_path)[1].lower()
        
        if file_extension not in ['.mp3']:
            temp_file = convert_to_mp3(temp_file_to_edit.name)

            if temp_file:
                temp_file_to_edit.close()
                return temp_file

            else:
                temp_file_to_edit.close()
                return None

        else:
            temp_file_to_edit.close()
            return temp_file_to_edit.name

    except Exception as e:
        temp_file_to_edit.close()
        os.remove(temp_file_to_edit.name)
        raise e  # Re-raise the error to propagate it

def process_image(file_path: str, target_resolution=(854, 480)) -> str:
    """
    Process an image by converting to .jpg, removing metadata, ensuring a standard resolution,
    and saving it to a temporary file.

    Args:
        file_path (str): Path to the input image.
        target_resolution (tuple): Desired resolution (width, height).

    Returns:
        str: Path to the processed image in a temporary location.
    """
    ext = Path(file_path).suffix.lower()
    if ext not in ALLOWED_IMAGE_EXTENSIONS:
        raise ValueError(f"Unsupported file format: {ext}")

    base_name = Path(file_path).stem

    try:
        with Image.open(file_path) as img:
            img = remove_transparency(img)
            img = img.resize(target_resolution, Image.Resampling.LANCZOS)
            temp_file_path = os.path.join(TEMP_DIR, f"{base_name}_processed.jpg")
            img.save(temp_file_path, format="JPEG", quality=75, optimize=True)
                    
        return temp_file_path
    except UnidentifiedImageError:
        raise ValueError(f"Unable to process the image file: {file_path}")

def remove_transparency(img, bg_color=(255, 255, 255)):
    if img.mode in ("RGBA", "LA") or (img.mode == "P" and "transparency" in img.info):
        alpha = img.convert("RGBA").getchannel("A")
        background = Image.new("RGB", img.size, bg_color)
        background.paste(img, mask=alpha)
        return background
    return img

def terminate_ffmpeg():
    """Find and terminate any running ffmpeg processes."""
    for proc in psutil.process_iter(['pid', 'name']):
        if 'ffmpeg' in proc.info['name']:
            try:
                proc.terminate()  # Forcefully terminate ffmpeg process
                proc.wait()  # Ensure the process is fully terminated
                logging(f"Terminated ffmpeg process (PID: {proc.info['pid']})")
            except Exception as e:
                continue

def merge_media(photo_path, audio_path, output_dir , file_name):
    """Merges an image with an audio file to create a video."""
    try:
        logging.info("Starting merge_media process...")

        # Step 1: Process the photo (image)
        photo_path = process_image(photo_path)
        logging.info(f"Image processed and saved at: {photo_path}")

        # Step 2: Process the audio
        audio_path = process_audio(audio_path)
        logging.info(f"Audio processed and saved at: {audio_path}")

        # Step 3: Create subdirectory for output files based on current date
        date_subdir = datetime.now().strftime('%Y-%m-%d')
        output_subdir = os.path.join(output_dir, date_subdir)
        os.makedirs(output_subdir, exist_ok=True)

        # Step 4: Set up the video with the processed image
        logging.info("Setting up the video with the image.")
        image = ImageClip(photo_path).set_duration(60)

        # Step 5: Generate a unique timestamp for the temporary audio file
        timestamp = datetime.now().strftime('%Y-%m-%d_%H-%M-%S')
        temp_audio_path = f"temp_audio_{timestamp}.mp3"  # Dynamic temp audio file name
        logging.info(f"Generated temporary audio file path: {temp_audio_path}")

        # Step 6: Extend the audio
        extend_audio(audio_path, temp_audio_path)
        logging.info(f"Audio extended and saved as: {temp_audio_path}")

        # Step 7: Load the extended audio
        logging.info(f"Loading extended audio from: {temp_audio_path}")
        audio = AudioFileClip(temp_audio_path)

        # Step 8: Create the video with the image and audio
        logging.info("Creating the video with the image and audio.")
        video = image.set_audio(audio)

        # Step 9: Define the output path for the video
        output_path = os.path.join(output_subdir, f"{file_name}.mp4")
        logging.info(f"Defining output path for the video: {output_path}")

        if os.path.exists(output_path):
            try:
                os.remove(output_path)
                logging.info(f"Removed existing file: {output_path}")
            except Exception as e:
                logging.error(f"Failed to remove existing file: {output_path} — {e}")
                
        # Step 10: Write the video file to the output path
        logging.info(f"Writing the video file to: {output_path}")
        video.write_videofile(output_path, codec='libx264', fps=24, verbose=False, logger=None)

        # Step 11: Clean up the temporary audio and image files
        logging.info("Starting cleanup of temporary files.")
        
        # Cleanup each temporary file step by step
        os.remove(temp_audio_path)

        os.remove(photo_path)
        terminate_ffmpeg()
        os.remove(audio_path)

        logging.info("Cleanup completed successfully.")

        return output_path
    except Exception as e:
        logging.error(f"Error in merge_media: {e}")
        return False

def extend_audio(audio_path, temp_audio_path):
    """Extends an audio file to a minimum duration of 1 minute."""
    try:
        audio = AudioFileClip(audio_path)
        duration = audio.duration

        if duration < 60:
            loops = int(60 // duration) + 1
            audio = concatenate_audioclips([audio] * loops).subclip(0, 60)
        elif duration > 60:
            audio = audio.subclip(0, 60)
        
        # Save the extended audio to the dynamic temp_audio_path
        audio.write_audiofile(temp_audio_path, verbose=False, logger=None)
    except Exception as e:
        logging.error(f"Error extending audio: {e}")
        raise

def merge_audio(audio_path, save_audio_path, device_id):
    try:
        # Load audio
        audio = AudioFileClip(audio_path)
        duration = audio.duration

        # Adjust audio length to exactly 60 seconds
        if duration < 60:
            loops = int(60 // duration) + 1
            audio = concatenate_audioclips([audio] * loops).subclip(0, 60)
        elif duration > 60:
            audio = audio.subclip(0, 60)

        # Create dated folder
        date_folder = datetime.now().strftime("%Y-%m-%d")
        full_save_path = os.path.join(save_audio_path, date_folder)
        os.makedirs(full_save_path, exist_ok=True)

        # Save audio file
        output_file = os.path.join(full_save_path, f"{device_id}.mp3")


        if os.path.exists(output_file):
            os.remove(output_file)

        audio.write_audiofile(output_file, verbose=False, logger=None)
        return output_file

    except Exception as e:
        logging.error(f"Error processing audio: {e}")
        return False


def execute_task(audio_name, image_name, period_hours, end_time, path_merge):
    """
    Executes the task to play the merged media file and schedules its next execution if
    the end time has not been reached. This task is intended to run periodically based 
    on the specified period and continues until the end time is reached.

    Args:
        audio_name (str): The name of the audio file used in the media merge.
        image_name (str): The name of the image file used in the media merge.
        period_hours (int): The interval (in hours) between each task execution.
        end_time (datetime): The time when the periodic task should stop executing.
        path_merge (str): The file path of the merged media (audio + image) that will be played.

    Returns:
        None
    """
    try:
        logging.info(f"Executing task at {datetime.now()} for audio '{audio_name}' and image '{image_name}'.")
        open_video_file(path_merge)
        if datetime.now() < end_time:
            next_execution_time = datetime.now() + timedelta(hours=period_hours)
            
            schedule_task(
                execute_task, 
                (audio_name, image_name, period_hours, end_time, path_merge), 
                next_execution_time
            )
            logging.info(f"Next task for '{path_merge}' scheduled at: {next_execution_time}")

    except Exception as e:
        logging.error(f"Error during execution of task for audio '{audio_name}' and image '{image_name}': {e}", exc_info=True)

def process_result(path, audio_name, image_name, period_hours, duration_days):
    """
    Processes the result of the media merge operation and schedules the next execution 
    of the periodic task. If the merge was successful, the task will be scheduled to 
    run at regular intervals for the specified duration.

    Args:
        path (str): The file path of the successfully merged media file.
        audio_name (str): The name of the audio file used in the merge.
        image_name (str): The name of the image file used in the merge.
        period_hours (int): The interval (in hours) at which the task should recur.
        duration_days (int): The total duration (in days) for which the task should be scheduled.
    
    Returns:
        None
    """
    try:
        logging.info(f"Processing result for merged media: {path}")

        # Check if the result file indicates a successful merge
        if "Merge" in path:
            logging.info("Media merge was successful. Proceeding with scheduling the task.")

            # Calculate the end time based on the specified duration (in days)
            end_time = datetime.now() + timedelta(days=duration_days)

            # Schedule the first execution of the periodic task
            first_execution_time = datetime.now()
            schedule_task(
                execute_task, 
                (audio_name, image_name, period_hours, end_time, path), 
                first_execution_time
            )
            logging.info(f"Task successfully scheduled to run every {period_hours} hour(s) for {duration_days} day(s).")
            logging.info(f"Scheduled task Thread Ended")
            logging.info("-" * 60)
        else:
            logging.error("Media merge failed. No further actions will be taken.")

    except Exception as e:
        logging.error(f"Error processing result for merged media '{path}': {e}", exc_info=True)

def schedule_task_initial(audio_name, image_name, period_hours, duration_days, pool):
    """
    Schedules a task to merge an audio file with an image file and process the result.
    The task is scheduled to run periodically based on the provided interval (in hours) 
    and the total duration (in days) for which the task should continue.
    
    Args:
        audio_name (str): The name of the audio file to merge with the image.
        image_name (str): The name of the image file to merge with the audio.
        period_hours (int): The interval in hours between each task execution.
        duration_days (int): The total duration in days for which the task should run.
        pool (multiprocessing.Pool): The pool of worker processes used to run the task asynchronously.
        
    Returns:
        None
    """
    try:
        audio_path = find_file(AUDIO_DIR, audio_name)
        if not audio_path:
            logging.error(f"Audio file '{audio_name}' not found in directory '{AUDIO_DIR}'. Task cannot proceed.")
            return

        image_path = find_file(IMAGE_DIR, image_name)
        if not image_path:
            logging.error(f"Image file '{image_name}' not found in directory '{IMAGE_DIR}'. Task cannot proceed.")
            return

        logging.info(f"Scheduling task to merge audio '{audio_name}' with image '{image_name}'.")

        # Asynchronously merge the audio and image files using a worker from the pool
        pool.apply_async(
            merge_media, 
            args=(image_path, audio_path, OUTPUT_DIR), 
            callback=lambda path: process_result(path, audio_name, image_name, period_hours, duration_days)
        )
        
    except Exception as e:
        logging.error(f"Exception occurred in schedule_task_initial: {e}", exc_info=True)

def main():
    """
    Main function to control the automated media merge process.
    
    This function serves as the entry point for the program. It handles the entire 
    process of prompting the user for input (audio and image files), validating the 
    input, and scheduling a task for periodic media merges. It also manages the 
    multi-threaded and multi-processing tasks for handling media files.
    """
    try:
        with multiprocessing.Pool(processes=4) as pool:
            while True:
                try:
                    
                    # User input for audio file
                    audio_name = input("Enter the audio file name (e.g., 'example.wav'):\n")
                    while not find_file(AUDIO_DIR, audio_name) or not is_valid_format(audio_name, ALLOWED_AUDIO_EXTENSIONS):
                        if not find_file(AUDIO_DIR, audio_name):
                            os.system('cls' if os.name == 'nt' else 'clear')
                            logging.warning(f"Invalid audio file '{audio_name}'. Please try again.")
                            audio_name = input("Enter the audio file name (e.g., 'example.wav'):\n")
                        else:
                            os.system('cls' if os.name == 'nt' else 'clear')
                            logging.warning(f"Invalid audio format for '{audio_name}'. Supported formats: {', '.join(ALLOWED_AUDIO_EXTENSIONS)}")
                            audio_name = input("Enter the audio file name (e.g., 'example.wav'):\n")

                    # User input for image file
                    image_name = input("Enter the image file name (e.g., 'example.png'):\n")
                    while not find_file(IMAGE_DIR, image_name) or not is_valid_format(image_name, ALLOWED_IMAGE_EXTENSIONS):
                        if not find_file(IMAGE_DIR, image_name):
                            logging.warning(f"Image file '{image_name}' not found. Please try again.")
                            image_name = input("Enter the image file name (e.g., 'example.png'):\n")
                        else:
                            logging.warning(f"Invalid image format for '{image_name}'. Supported formats: {', '.join(ALLOWED_IMAGE_EXTENSIONS)}")
                            image_name = input("Enter the image file name (e.g., 'example.png'):\n")

                    # Get the duration and period for scheduling
                    try:
                        duration_in_days = int(input("Enter the duration for the periodic task in days:\n"))
                        period = int(input("Enter the period in hours between media merges (e.g., 2):\n"))
                    except ValueError as e:
                        logging.error(f"Invalid input for duration or period. Please enter valid numbers. Error: {e}")
                        continue  # Skip this iteration and prompt again
                    logging.info(f"Scheduled task Thread started")
                    logging.info("-" * 60)
                    # Schedule the task in a separate thread
                    threading.Thread(target=schedule_task_initial, args=(audio_name, image_name, period, duration_in_days, pool), daemon=True).start()

                    # Clear screen and log the scheduling information
                    os.system('cls' if os.name == 'nt' else 'clear')
                    logging.info(f"Scheduled task for {duration_in_days} day(s) with {period} hour(s) intervals.")

                except Exception as e:
                    logging.error(f"Unexpected error occurred in the main loop: {e}", exc_info=True)
                    logging.info("Retrying the process...")

    except Exception as e:
        logging.critical(f"Critical error in main function: {e}", exc_info=True)
        raise  # Re-raise the error after logging for higher level handling

def merger( audio_path , device_id ,image_path  ):
    try:
        print(f'this is merger talking, status are: \naudiopath {audio_path}\nimagepath {image_path}\n')
        if image_path == None:
            status = merge_audio(audio_path, OUTPUT_DIR , device_id)                
        else:
            status = merge_media(image_path, audio_path , OUTPUT_DIR , device_id)

        return status
                   
    except Exception as e:
        logging.critical(f"Critical error in merger: {e}")

try:
    init()
    
except Exception as e:
    logging.error(f"init Error: {e}", exc_info=True)
