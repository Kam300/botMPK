import os
import logging
import asyncio
from datetime import datetime, timedelta
from concurrent.futures import ThreadPoolExecutor
from functools import partial
import openpyxl
import threading
import time
import json
from cache_utils import get_cached_teacher_schedule, cache_teacher_schedule
from excel_cache import use_excel_cache, get_cached_workbook_async

# Import necessary functions from было.py without modifying it
# We'll use these imported functions to maintain compatibility
from было import (
    parse_teacher_schedule,
    format_teacher_schedule,
    run_blocking
)

logger = logging.getLogger(__name__)

# Create a dedicated thread pool for Excel file processing
# This is separate from the main thread_pool in было.py
excel_thread_pool = ThreadPoolExecutor(max_workers=25)  # Increased from 20

# Create a semaphore to limit concurrent file operations
# This prevents overwhelming the system with too many file operations
file_semaphore = asyncio.Semaphore(20)  # Increased from 15

# Lock for thread-safe operations
excel_lock = threading.Lock()

# Track files being processed to avoid duplicate processing
files_being_processed = {}
files_lock = threading.Lock()

# Store file applicability results to avoid recalculating
file_applicability_cache = {}
file_applicability_lock = threading.Lock()

# Popular teachers list for prioritized processing
POPULAR_TEACHERS = []
popular_teachers_lock = threading.Lock()

# Dictionary to track teacher access frequency
teacher_access_counts = {}
access_counts_lock = threading.Lock()

# Background task control
background_processor_running = False
background_processor_lock = threading.Lock()

def update_teacher_access(teacher_name):
    """Track teacher access to identify popular teachers"""
    with access_counts_lock:
        if teacher_name not in teacher_access_counts:
            teacher_access_counts[teacher_name] = 0
        teacher_access_counts[teacher_name] += 1
        
        # Update popular teachers list if this teacher is frequently accessed
        if teacher_access_counts[teacher_name] >= 3 and teacher_name not in POPULAR_TEACHERS:
            with popular_teachers_lock:
                if teacher_name not in POPULAR_TEACHERS:
                    POPULAR_TEACHERS.append(teacher_name)
                    logger.info(f"Added {teacher_name} to popular teachers list")
                    
                    # Save popular teachers to disk for persistence across restarts
                    try:
                        with open("popular_teachers.json", "w") as f:
                            json.dump(POPULAR_TEACHERS, f)
                    except Exception as e:
                        logger.error(f"Error saving popular teachers: {e}")

def load_popular_teachers():
    """Load popular teachers from disk to persist across restarts"""
    global POPULAR_TEACHERS
    try:
        if os.path.exists("popular_teachers.json"):
            with open("popular_teachers.json", "r") as f:
                teachers = json.load(f)
                with popular_teachers_lock:
                    POPULAR_TEACHERS = teachers
                logger.info(f"Loaded {len(POPULAR_TEACHERS)} popular teachers from disk")
    except Exception as e:
        logger.error(f"Error loading popular teachers: {e}")

async def run_excel_task(func, *args, **kwargs):
    """Run Excel processing tasks in a dedicated thread pool with semaphore control."""
    loop = asyncio.get_running_loop()
    async with file_semaphore:
        return await loop.run_in_executor(excel_thread_pool, partial(func, *args, **kwargs))

# Apply the Excel cache decorator to our processing function
@use_excel_cache
def cached_parse_teacher_schedule(file_path, date_str, teacher_name):
    """Cached version of parse_teacher_schedule that uses the Excel cache."""
    try:
        return parse_teacher_schedule(file_path, date_str, teacher_name)
    except Exception as e:
        logger.error(f"Error in cached_parse_teacher_schedule for {file_path}, {date_str}, {teacher_name}: {e}")
        return {}

async def process_excel_file_for_teacher(file_path, date_str, teacher_name):
    """Process a single Excel file for a teacher on a specific date with error handling."""
    # Create a unique key for this file processing task
    task_key = f"{file_path}_{date_str}_{teacher_name}"
    
    # Check if this file is already being processed
    with files_lock:
        if task_key in files_being_processed:
            # Wait for the existing task to complete
            try:
                logger.debug(f"Waiting for existing processing of {file_path} for {date_str}")
                return await files_being_processed[task_key]
            except Exception:
                # If the existing task failed, we'll try again
                if task_key in files_being_processed:
                    del files_being_processed[task_key]
    
    # Create a new task for processing this file
    task = asyncio.create_task(_process_excel_file(file_path, date_str, teacher_name))
    
    # Register the task
    with files_lock:
        files_being_processed[task_key] = task
    
    try:
        # Wait for the task to complete
        return await task
    finally:
        # Remove the task from the registry
        with files_lock:
            if task_key in files_being_processed and files_being_processed[task_key] == task:
                del files_being_processed[task_key]

async def _process_excel_file(file_path, date_str, teacher_name):
    """Internal function to process an Excel file."""
    try:
        # Preload the workbook asynchronously
        await get_cached_workbook_async(file_path)
        # Then process it
        return await run_excel_task(cached_parse_teacher_schedule, file_path, date_str, teacher_name)
    except Exception as e:
        logger.error(f"Error processing {file_path} for {date_str}: {e}")
        return {}

# Function to check if a file is applicable for a specific date with caching
def is_file_applicable_for_date(file_name, date_str):
    """Check if an Excel file is applicable for a specific date with caching."""
    # Create a cache key for this file and date
    cache_key = f"{file_name}_{date_str}"
    
    # Check if we have a cached result
    with file_applicability_lock:
        if cache_key in file_applicability_cache:
            return file_applicability_cache[cache_key]
    
    try:
        result = False
        # For files with date ranges (like 20.02.25-22.02.25.xlsx or 20.02.2025-22.02.2025.xlsx)
        if '-' in file_name:
            dates = file_name.replace('.xlsx', '').split('-')
            if len(dates) == 2:
                try:
                    target_date = datetime.strptime(date_str, '%d.%m.%Y')
                    
                    # Try different date formats
                    start_str = dates[0]
                    end_str = dates[1]
                    
                    # Try to parse with short year format first (DD.MM.YY)
                    try:
                        start_date = datetime.strptime(start_str, '%d.%m.%y')
                        end_date = datetime.strptime(end_str, '%d.%m.%y')
                    except ValueError:
                        # Try with full year format (DD.MM.YYYY)
                        try:
                            start_date = datetime.strptime(start_str, '%d.%m.%Y')
                            end_date = datetime.strptime(end_str, '%d.%m.%Y')
                        except ValueError:
                            # If we can't parse the date, assume it's not applicable
                            result = False
                    
                    # Check if target date is in range
                    result = start_date.date() <= target_date.date() <= end_date.date()
                except ValueError:
                    # If we can't parse the date, assume it's not applicable
                    result = False
        else:
            # For regular schedule files (like ИСпВ-24-1.xlsx)
            # These are always applicable
            result = True
    except Exception as e:
        logger.error(f"Error checking if file {file_name} is applicable for date {date_str}: {e}")
        # In case of any error, assume the file is applicable to be safe
        result = True
    
    # Cache the result
    with file_applicability_lock:
        file_applicability_cache[cache_key] = result
    
    return result

async def get_teacher_schedule_optimized(teacher_name: str, start_date: str, end_date: str) -> str:
    """Optimized version that only processes relevant files for each date."""
    try:
        # Track teacher access to identify popular teachers
        update_teacher_access(teacher_name)
        
        # Check cache first
        cached_schedule = get_cached_teacher_schedule(teacher_name, start_date, end_date)
        if cached_schedule:
            logger.info(f"Using cached schedule for {teacher_name} from {start_date} to {end_date}")
            return cached_schedule

        all_schedules = {}
        start_date_obj = datetime.strptime(start_date, '%d.%m.%Y').date()
        end_date_obj = datetime.strptime(end_date, '%d.%m.%Y').date()

        # Get all files once
        all_files = await run_blocking(lambda: os.listdir("downloaded_files"))
        excel_files = [f for f in all_files if f.endswith('.xlsx')]
        
        # Organize files by priority
        regular_files = [f for f in excel_files if '-' not in f]  # Regular schedule files
        replacement_files = [f for f in excel_files if '-' in f]  # Replacement files
        
        # Sort replacement files by date (newest first) to prioritize recent replacements
        replacement_files.sort(reverse=True)
        
        # Combine files with prioritization
        prioritized_files = regular_files + replacement_files
        
        # Preload Excel files in parallel
        preload_tasks = []
        for file in prioritized_files[:25]:  # Limit to 25 files to avoid overloading
            file_path = os.path.join("downloaded_files", file)
            preload_tasks.append(get_cached_workbook_async(file_path))
        
        # Start preloading but don't wait for completion
        preload_task = asyncio.create_task(asyncio.gather(*preload_tasks))
        
        # Process each date
        tasks = []
        file_date_pairs = []
        
        current_date = start_date_obj
        while current_date <= end_date_obj:
            if current_date.weekday() != 6:  # Skip Sundays
                date_str = current_date.strftime('%d.%m.%Y')
                
                # Filter files applicable for this date - do this more efficiently
                applicable_files = []
                
                # Regular schedule files are always applicable
                for file in regular_files:
                    applicable_files.append(file)
                
                # Process applicable replacement files
                for file in replacement_files:
                    if await run_blocking(is_file_applicable_for_date, file, date_str):
                        applicable_files.append(file)
                
                # Process each applicable file
                for file in applicable_files:
                    file_path = os.path.join("downloaded_files", file)
                    tasks.append(process_excel_file_for_teacher(file_path, date_str, teacher_name))
                    file_date_pairs.append((file, date_str))
            
            current_date += timedelta(days=1)

        # Wait for all tasks to complete
        results = await asyncio.gather(*tasks)

        # Process results
        for i, result in enumerate(results):
            if result:
                file, date_str = file_date_pairs[i]
                if date_str not in all_schedules:
                    all_schedules[date_str] = {}
                all_schedules[date_str].update(result)

        # Format the final schedule
        formatted_schedule = await run_blocking(
            format_teacher_schedule,
            all_schedules,
            teacher_name,
            start_date,
            end_date
        )

        # Cache the result with a longer expiration for popular teachers
        if teacher_name in POPULAR_TEACHERS:
            cache_teacher_schedule(teacher_name, start_date, end_date, formatted_schedule, expiration=3600)  # 1 hour
        else:
            cache_teacher_schedule(teacher_name, start_date, end_date, formatted_schedule)  # Default expiration
        
        # Make sure preloading is complete before returning
        try:
            await asyncio.wait_for(preload_task, timeout=0.1)
        except asyncio.TimeoutError:
            # It's okay if preloading is still ongoing
            pass
        
        return formatted_schedule

    except Exception as e:
        logger.error(f"Error getting optimized teacher schedule: {e}")
        return f"Ошибка при получении расписания преподавателя: {str(e)}"

# Function to preload schedules for popular teachers in the background
async def preload_teacher_schedules():
    """Preload schedules for popular teachers in the background."""
    try:
        # Get today's date and date a week from now
        today = datetime.now().date()
        next_week = today + timedelta(days=7)
        
        start_date = today.strftime('%d.%m.%Y')
        end_date = next_week.strftime('%d.%m.%Y')
        
        # Make a copy of the popular teachers list to avoid lock contention
        with popular_teachers_lock:
            teachers_to_preload = POPULAR_TEACHERS.copy()
        
        if not teachers_to_preload:
            logger.info("No popular teachers to preload schedules for")
            return
        
        logger.info(f"Preloading schedules for {len(teachers_to_preload)} popular teachers")
        
        # Preload schedules for each popular teacher
        for teacher_name in teachers_to_preload:
            try:
                # Check if we already have a cached schedule
                if get_cached_teacher_schedule(teacher_name, start_date, end_date):
                    continue
                
                # Generate and cache the schedule
                logger.info(f"Preloading schedule for popular teacher: {teacher_name}")
                await get_teacher_schedule_optimized(teacher_name, start_date, end_date)
                # Small delay to avoid overwhelming the system
                await asyncio.sleep(0.5)
            except Exception as e:
                logger.error(f"Error preloading schedule for {teacher_name}: {e}")
    except Exception as e:
        logger.error(f"Error in preload_teacher_schedules: {e}")

def start_background_processor():
    """Start a background processor to preload and maintain schedules."""
    global background_processor_running
    
    # Load popular teachers from disk
    load_popular_teachers()
    
    with background_processor_lock:
        if background_processor_running:
            logger.info("Background processor already running")
            return
        background_processor_running = True
    
    # Create a dedicated thread for the background processor
    thread = threading.Thread(target=_run_background_processor, daemon=True)
    thread.start()
    logger.info("Started background teacher schedule processor")

def _run_background_processor():
    """Run the background processor in a dedicated thread."""
    try:
        # Create a new event loop for this thread
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        
        # Run the background processor loop
        loop.run_until_complete(_background_processor_loop())
    except Exception as e:
        logger.error(f"Error in background processor: {e}")
        with background_processor_lock:
            global background_processor_running
            background_processor_running = False

async def _background_processor_loop():
    """Background processor loop to preload and maintain schedules."""
    try:
        # Wait for initial startup to complete
        await asyncio.sleep(30)
        
        while True:
            try:
                # Preload schedules for popular teachers
                await preload_teacher_schedules()
                
                # Wait before the next preload cycle
                await asyncio.sleep(3600)  # Run once per hour
            except Exception as e:
                logger.error(f"Error in background processor cycle: {e}")
                await asyncio.sleep(300)  # Wait 5 minutes before retrying
    except asyncio.CancelledError:
        logger.info("Background processor cancelled")
    except Exception as e:
        logger.error(f"Fatal error in background processor loop: {e}") 