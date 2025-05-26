MultimediaTaskScheduler-Demo

A demo version of a multimedia task scheduling system built with Python, Flask, and MongoDB. This academic project automates task scheduling, media processing (audio/image to video), and user management via a web dashboard. Features include concurrent task execution, media merging with FFmpeg, and rate-limited APIs. Ideal for learning about distributed systems, media processing, and web development.
Note: The production version is proprietary and not included, as per company policies (سیاست‌های شرکت).
Table of Contents

Project Overview
This repository contains the demo version of a multimedia task scheduling system, designed as a proof of concept (اثبات مفهوم) for academic purposes. The system automates the scheduling and execution of multimedia tasks, processes audio and image files into videos, and provides a web-based interface for user and device management. Key features include:

Task Scheduling (زمان‌بندی وظایف): Persistent task queuing and execution using a heap-based scheduler in MongoDB.
Media Processing (پردازش رسانه): Conversion of audio (e.g., .wma to MP3) and images (e.g., PNG to 854x480 JPG) with merging into MP4 videos.
Web Dashboard (داشبورد وب): Flask-based interface for user authentication (احراز هویت), task submission (ارسال رسانه), and log review (بررسی لاگ‌ها).
Concurrency (هم‌زمانی): Parallel task execution with ThreadPoolExecutor.
Rate Limiting (محدودسازی نرخ): API protection using Flask-Limiter and Redis.
Logging (لاگ‌گیری): Detailed event and error tracking in MongoDB and log files.

This project is ideal for exploring distributed systems (سیستم‌های توزیع‌شده), media processing frameworks, and web development with Python.
Prerequisites
Before setting up the project, ensure the following are installed:

Python 3.8+: Required for running the scripts (نصب پایتون). Download from python.org.
MongoDB Community Edition: NoSQL database for task and user data (پایگاه داده MongoDB). Install from mongodb.com.
FFmpeg: Essential for video processing (چارچوب ویدئویی). Download from ffmpeg.org and add it to your system PATH.
Git: For cloning the repository. Install from git-scm.com.
pip: Python package manager (included with Python).

Installation
Follow these steps to set up the project locally:

Clone the Repository:
git clone https://github.com/thisisntbardia/MultimediaTaskScheduler-Demo.git
cd MultimediaTaskScheduler-Demo


Install Python Dependencies (کتابخانه‌های موردنیاز):
pip install -r requirements.txt

The requirements.txt includes Flask, Pymongo, MoviePy, Pydub, Mutagen, Waitress, and Pytest.

Install and Start MongoDB:

On Windows: Run the MongoDB installer and start the service via MongoDB Compass or mongod.
On Linux:sudo apt-get install -y mongodb-org
sudo systemctl start mongod



Verify MongoDB is running:
mongo --version


Install FFmpeg:

On Windows: Download the binary, extract it, and add it to your system PATH.
On Linux:sudo apt-get install ffmpeg



Verify installation:
ffmpeg -version


Run the Server (سرور ویترس):
python server.py

The Flask app will start on http://localhost:5000 using Waitress.


Usage
Once the server is running, access the web interface at http://localhost:5000. Follow these steps to use the system:

Login (ورود):

Navigate to the root page (/) and enter your username and password.
Admins access the admin dashboard; regular users access the user dashboard.


Register New User (ثبت کاربر جدید):

Admins can add new users via the admin dashboard (/api/admin/new_user), providing details like name, email, and subscription dates.


Add Device (افزودن دستگاه):

Admins define new devices with routines (e.g., instruction-A) via /api/admin/new_device.


Submit Media (ارسال رسانه):

Users upload audio (e.g., MP3) and image (e.g., JPEG) files for registered devices via /api/user/upload/<device_id>.
Files are processed and merged into MP4 videos.


Check Logs (بررسی لاگ‌ها):

Admins view system and device logs in the admin dashboard or check log files (e.g., SERVER.log).
Users have limited access to their device logs.



For detailed instructions, refer to the User Manual (if included).
Project Structure
The repository is organized as follows:

M.py: Handles media processing (پردازش رسانه), including image resizing, audio conversion, and video merging.
server.py: Runs the Flask web server (رابط کاربری وب) and API endpoints for authentication, task submission, and device management.
db.py: Manages task scheduling (زمان‌بندی وظایف) and MongoDB interactions, including persistent queuing and logging.
test_project.py: Contains Pytest scripts for unit and integration testing (آزمایش واحد و یکپارچگی).
requirements.txt: Lists required Python libraries.
templates/: Stores Flask HTML templates (e.g., dashboard.html, index.html).
.gitignore: Excludes temporary files, logs, and virtual environments.
LICENSE: MIT License for the demo code.
README.md: This file, providing project overview and instructions.

Testing
The project includes automated tests in test_project.py to validate core functionalities. Tests cover:

Task Scheduling: Adding tasks, executing tasks on time, and retrying failed tasks (تلاش مجدد).
Media Processing: Converting unsupported audio formats and resizing images.
API and Authentication: Admin login, invalid login attempts, and rate limiting (محدودسازی نرخ).

To run the tests:
pip install pytest
pytest test_project.py -v

The tests use mocking (شبیه‌سازی) for MongoDB and Redis, ensuring no external dependencies are needed. For details, see the Testing Documentation (if included).
Limitations
As a demo version with a monolithic architecture (معماری مونولیتیک), this project has limitations:

Frontend: Basic HTML templates with limited interactivity (no WebSocket notifications / اعلان‌های WebSocket).
Scalability (مقیاس‌پذیری): Not optimized for high loads; production version uses advanced scaling.
Security: Basic authentication; production version includes enhanced measures.
Features: Lacks advanced media processing (ویژگی‌های پیشرفته رسانه) like video trimming or subtitles.
Multi-Tenancy (چندمستاجری): Not supported in the demo.

Future work could include microservices (میکروسرویس) and CI/CD pipelines (خط لوله CI/CD), as outlined in the Conclusion section.
License
This project is licensed under the MIT License. See the LICENSE file for details. Note that the production version is proprietary and not covered by this license.
Acknowledgments
This project leverages open-source tools (نرم‌افزار متن‌باز) and resources:

Flask: Web framework (Flask Documentation).
MongoDB: NoSQL database (MongoDB Documentation).
FFmpeg: Media processing (FFmpeg Documentation).
MoviePy: Video editing (MoviePy Documentation).
Pydub & Mutagen: Audio processing (Pydub, Mutagen).
Redis: Rate limiting (Redis Documentation).
Waitress: WSGI server (Waitress Documentation).
Pytest: Testing framework (Pytest Documentation).

For a full list of references, see the Bibliography.

