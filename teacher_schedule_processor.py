import os
import logging
import asyncio
from datetime import datetime, timedelta
from concurrent.futures import ThreadPoolExecutor
from functools import partial
import openpyxl
import threading
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
excel_thread_pool = ThreadPoolExecutor(max_workers=20)  # Increased from 15

# Create a semaphore to limit concurrent file operations
# This prevents overwhelming the system with too many file operations
file_semaphore = asyncio.Semaphore(15)  # Increased from 10

# Lock for thread-safe operations
excel_lock = threading.Lock()

# Track files being processed to avoid duplicate processing
files_being_processed = {}
files_lock = threading.Lock()

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

async def get_teacher_schedule_enhanced(teacher_name: str, start_date: str, end_date: str) -> str:
    """Enhanced version of get_teacher_schedule with better parallelism and caching."""
    try:
        # Check cache first
        cached_schedule = get_cached_teacher_schedule(teacher_name, start_date, end_date)
        if cached_schedule:
            logger.info(f"Using cached schedule for {teacher_name} from {start_date} to {end_date}")
            return cached_schedule

        # Get all schedule files
        schedule_files = await run_blocking(lambda: [
            f for f in os.listdir("downloaded_files")
            if f.endswith('.xlsx') and not '-' in f
        ])

        all_schedules = {}
        start_date_obj = datetime.strptime(start_date, '%d.%m.%Y').date()
        end_date_obj = datetime.strptime(end_date, '%d.%m.%Y').date()

        # Create a list of dates to process
        dates_to_process = []
        current_date = start_date_obj
        while current_date <= end_date_obj:
            if current_date.weekday() != 6:  # Skip Sundays
                dates_to_process.append(current_date.strftime('%d.%m.%Y'))
            current_date += timedelta(days=1)

        # Process files in parallel with controlled concurrency
        tasks = []
        for date_str in dates_to_process:
            for file in schedule_files:
                file_path = os.path.join("downloaded_files", file)
                tasks.append(process_excel_file_for_teacher(file_path, date_str, teacher_name))

        # Wait for all tasks to complete
        results = await asyncio.gather(*tasks)

        # Organize results by date
        for i, result in enumerate(results):
            if result:
                date_str = dates_to_process[i // len(schedule_files)]
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

        # Cache the result
        cache_teacher_schedule(teacher_name, start_date, end_date, formatted_schedule)
        
        return formatted_schedule

    except Exception as e:
        logger.error(f"Error getting enhanced teacher schedule: {e}")
        return f"Ошибка при получении расписания преподавателя: {str(e)}"

# Function to check if a file is applicable for a specific date
def is_file_applicable_for_date(file_name, date_str):
    """Check if an Excel file is applicable for a specific date."""
    try:
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
                            logger.warning(f"Could not parse dates from filename {file_name} with any known format")
                            # If we can't parse the date, assume it's not applicable
                            return False
                    
                    # Check if target date is in range
                    return start_date.date() <= target_date.date() <= end_date.date()
                except ValueError as e:
                    logger.warning(f"Could not parse date from filename {file_name}: {e}")
                    # If we can't parse the date, assume it's not applicable
                    return False
        
        # For regular schedule files (like ИСпВ-24-1.xlsx)
        # These are always applicable
        return True
    except Exception as e:
        logger.error(f"Error checking if file {file_name} is applicable for date {date_str}: {e}")
        # In case of any error, assume the file is applicable to be safe
        return True

async def get_teacher_schedule_optimized(teacher_name: str, start_date: str, end_date: str) -> str:
    """Optimized version that only processes relevant files for each date."""
    try:
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
        
        # Preload all Excel files in parallel
        preload_tasks = []
        for file in excel_files[:40]:  # Increased from 30 files to avoid overloading
            file_path = os.path.join("downloaded_files", file)
            preload_tasks.append(get_cached_workbook_async(file_path))
        
        # Start preloading but don't wait for completion
        preload_task = asyncio.create_task(asyncio.gather(*preload_tasks))
        
        # Process each date
        tasks = []
        dates_to_process = []
        file_date_pairs = []
        
        current_date = start_date_obj
        while current_date <= end_date_obj:
            if current_date.weekday() != 6:  # Skip Sundays
                date_str = current_date.strftime('%d.%m.%Y')
                dates_to_process.append(date_str)
                
                # Filter files applicable for this date
                for file in excel_files:
                    try:
                        if await run_blocking(is_file_applicable_for_date, file, date_str):
                            file_path = os.path.join("downloaded_files", file)
                            tasks.append(process_excel_file_for_teacher(file_path, date_str, teacher_name))
                            file_date_pairs.append((file, date_str))
                    except Exception as e:
                        logger.error(f"Error checking file applicability for {file}: {e}")
                        # Include the file anyway to be safe
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

        # Cache the result
        cache_teacher_schedule(teacher_name, start_date, end_date, formatted_schedule)
        
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