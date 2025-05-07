import logging
import asyncio
from functools import wraps
from datetime import datetime, timedelta
import threading
import os
import re

from teacher_schedule_processor import (
    get_teacher_schedule_optimized, 
    preload_teacher_schedules,
    get_teacher_schedule_with_index,
    build_schedule_index,
    precache_popular_teachers
)
from было import get_teacher_schedule as original_get_teacher_schedule
from было import parse_teacher_schedule, format_teacher_schedule, run_blocking
from cache_utils import get_cached_teacher_schedule, cache_teacher_schedule

logger = logging.getLogger(__name__)

# Function to check if a file is a replacement file based on its name
def is_replacement_file(filename):
    """Проверяет, является ли файл файлом замен (по формату имени)"""
    if not filename.endswith('.xlsx'):
        return False
        
    # Проверка формата с диапазоном дат (DD.MM.YY-DD.MM.YY.xlsx)
    date_range_pattern = re.compile(r'^(\d{2}\.\d{2}\.\d{2,4})-(\d{2}\.\d{2}\.\d{2,4})\.xlsx$')
    if date_range_pattern.match(filename):
        return True
    
    # Проверка формата с одной датой (DD.MM.YY.xlsx)
    single_date_pattern = re.compile(r'^(\d{2}\.\d{2}\.\d{2,4})\.xlsx$')
    if single_date_pattern.match(filename):
        return True
    
    return False

# Dictionary to track ongoing teacher schedule requests
ongoing_requests = {}
ongoing_requests_lock = threading.Lock()
REQUEST_TIMEOUT = 45  # Seconds to wait before considering a request as hung

# Flag to track if we've done initial setup
initial_setup_done = False
initial_setup_lock = threading.Lock()

def timeout_checker(key, start_time):
    """Check if a request has been running too long and remove it from ongoing_requests"""
    with ongoing_requests_lock:
        if key in ongoing_requests and ongoing_requests[key]['start_time'] == start_time:
            if (datetime.now() - start_time).total_seconds() > REQUEST_TIMEOUT:
                logger.warning(f"Request timed out for {key}. Removing from ongoing requests.")
                # Resolve the future with an error message to unblock any waiting clients
                if not ongoing_requests[key]['future'].done():
                    teacher_name = key.split('_')[0]  # Extract teacher name from the key
                    ongoing_requests[key]['future'].set_result(
                        f"Не удалось получить расписание для {teacher_name} из-за таймаута. Пожалуйста, попробуйте позже."
                    )
                del ongoing_requests[key]

async def do_initial_setup():
    """Initialize the index and cache popular teachers"""
    global initial_setup_done
    
    # Use a lock to prevent multiple threads from doing setup simultaneously
    with initial_setup_lock:
        if initial_setup_done:
            return
            
        logger.info("Building schedule index...")
        await build_schedule_index()
        
        # Start precaching popular teachers in the background
        asyncio.create_task(precache_popular_teachers())
        
        initial_setup_done = True
        logger.info("Initial setup completed - schedule index built and started preloading popular teachers")

async def get_teacher_schedule(teacher_name: str, start_date: str, end_date: str) -> str:
    """Patched version of the teacher schedule retrieval function that uses optimized implementation"""
    try:
        # Ensure index is built before proceeding
        await do_initial_setup()

        # Check for single-date replacement files (10.05.25.xlsx format) that may extend the date range
        files_dir = "downloaded_files"
        replacement_files = [f for f in os.listdir(files_dir) 
                             if f.endswith('.xlsx') and f[0].isdigit()]
                             
        # Parse the input date range
        start_date_obj = datetime.strptime(start_date, '%d.%m.%Y').date()
        end_date_obj = datetime.strptime(end_date, '%d.%m.%Y').date()
        
        # Expand the start/end date range if we find single-date files outside the specified range
        for file in replacement_files:
            if "-" not in file:  # Single date file
                try:
                    single_date_str = file.replace('.xlsx', '')
                    try:
                        # Try DD.MM.YY format
                        single_date = datetime.strptime(single_date_str, '%d.%m.%y').date()
                    except ValueError:
                        try:
                            # Try DD.MM.YYYY format
                            single_date = datetime.strptime(single_date_str, '%d.%m.%Y').date()
                        except ValueError:
                            # Not a date format we recognize
                            continue
                    
                    # If this single date is outside our current range but should be included,
                    # expand the range
                    if single_date < start_date_obj:
                        start_date_obj = single_date
                        start_date = single_date.strftime('%d.%m.%Y')
                        logger.info(f"Extended start date to {start_date} due to single date file {file}")
                    elif single_date > end_date_obj:
                        end_date_obj = single_date
                        end_date = single_date.strftime('%d.%m.%Y')
                        logger.info(f"Extended end date to {end_date} due to single date file {file}")
                    except Exception as e:
                    logger.error(f"Error processing single date file {file}: {e}")
                    continue
        
        # Create a unique key for this request
        request_key = f"{teacher_name}_{start_date}_{end_date}"
        start_time = datetime.now()
        
        # Check if this request is already ongoing
        with ongoing_requests_lock:
            if request_key in ongoing_requests:
                # Get the existing future
                existing_request = ongoing_requests[request_key]
                # Check if the request has been running for too long
                if (datetime.now() - existing_request['start_time']).total_seconds() > REQUEST_TIMEOUT:
                    logger.warning(f"Request for {request_key} appears to be hanging. Creating a new request.")
                    # Create a new future
                    future = asyncio.Future()
                    ongoing_requests[request_key] = {
                        'future': future,
                        'start_time': start_time
                    }
                    # Schedule a timeout check
                    asyncio.get_event_loop().call_later(
                        REQUEST_TIMEOUT, 
                        lambda: timeout_checker(request_key, start_time)
                    )
                else:
                    logger.info(f"Joining existing request for {teacher_name} from {start_date} to {end_date}")
                    try:
                        # Add a timeout to waiting for the existing request
                        return await asyncio.wait_for(existing_request['future'], timeout=30)
                    except asyncio.TimeoutError:
                        logger.warning(f"Timeout waiting for existing request for {teacher_name}")
                        # Clear the ongoing request
                        with ongoing_requests_lock:
                            ongoing_requests.pop(request_key, None)
            else:
                # Create a new future for this request
                future = asyncio.Future()
                ongoing_requests[request_key] = {
                    'future': future,
                    'start_time': start_time
                }
                # Schedule a timeout check
                asyncio.get_event_loop().call_later(
                    REQUEST_TIMEOUT, 
                    lambda: timeout_checker(request_key, start_time)
                )
        
        # Use the indexed version with a timeout
        try:
            logger.info(f"Using indexed teacher schedule for {teacher_name}")
            
            # Add a timeout to the indexed method
            result = await asyncio.wait_for(
                get_teacher_schedule_with_index(teacher_name, start_date, end_date),
                timeout=40  # 40 seconds max wait
            )
            
            # Complete the future with the result
            with ongoing_requests_lock:
                if request_key in ongoing_requests and not ongoing_requests[request_key]['future'].done():
                    ongoing_requests[request_key]['future'].set_result(result)
                    # Remove the request after a short delay to allow other waiters to get the result
                    asyncio.get_event_loop().call_later(
                        5, 
                        lambda: ongoing_requests.pop(request_key, None)
                    )
            
            return result
        except asyncio.TimeoutError:
            logger.error(f"Timeout using indexed method for {teacher_name}")
            
            # Try a simplified fallback method that directly searches only replacement files
            # This is much faster but less comprehensive
            simple_result = await get_simple_teacher_schedule(teacher_name, start_date, end_date)
            
            # Complete the future with the result
            with ongoing_requests_lock:
                if request_key in ongoing_requests and not ongoing_requests[request_key]['future'].done():
                    ongoing_requests[request_key]['future'].set_result(simple_result)
                    # Remove the request
                    ongoing_requests.pop(request_key, None)
            
            return simple_result
        except Exception as e:
            logger.error(f"Error using indexed method for {teacher_name}: {e}")
            logger.info(f"Falling back to optimized method")
            
            try:
                # Use the optimized method with a timeout
                result = await asyncio.wait_for(
                    get_teacher_schedule_optimized(teacher_name, start_date, end_date),
                    timeout=30  # 30 seconds timeout
                )
                
                # Complete the future with the result
                with ongoing_requests_lock:
                    if request_key in ongoing_requests and not ongoing_requests[request_key]['future'].done():
                        ongoing_requests[request_key]['future'].set_result(result)
                        # Remove the request after a short delay to allow other waiters to get the result
                        asyncio.get_event_loop().call_later(
                            5, 
                            lambda: ongoing_requests.pop(request_key, None)
                        )
                
                return result
            except (asyncio.TimeoutError, Exception) as e:
                # Try a simplified fallback method that directly searches only replacement files
                logger.error(f"Error or timeout in optimized method: {e}")
                simple_result = await get_simple_teacher_schedule(teacher_name, start_date, end_date)
                
                # Complete the future with the result
                with ongoing_requests_lock:
                    if request_key in ongoing_requests and not ongoing_requests[request_key]['future'].done():
                        ongoing_requests[request_key]['future'].set_result(simple_result)
                        # Remove the request
                        ongoing_requests.pop(request_key, None)
                
                return simple_result
            
    except Exception as e:
        logger.error(f"Error in patched get_teacher_schedule: {e}")
        # Mark the future as failed (if it exists)
            with ongoing_requests_lock:
            if request_key in ongoing_requests and not ongoing_requests[request_key]['future'].done():
                ongoing_requests[request_key]['future'].set_exception(e)
                # Remove the request
                ongoing_requests.pop(request_key, None)
        
        # As a last resort, try a simplified method
        try:
            return await get_simple_teacher_schedule(teacher_name, start_date, end_date)
        except Exception:
            # If all else fails, return an error message
            return f"Не удалось получить расписание для {teacher_name}. Пожалуйста, попробуйте позже."

async def get_simple_teacher_schedule(teacher_name: str, start_date: str, end_date: str) -> str:
    """Simplified fallback method that only checks replacement files - very fast but may miss some lessons"""
    try:
        logger.info(f"Using simplified schedule method for {teacher_name}")
        
        # First check cache
        cached_schedule = get_cached_teacher_schedule(teacher_name, start_date, end_date)
        if cached_schedule:
            logger.info(f"Using cached schedule for {teacher_name}")
            return cached_schedule
            
        # Get all files in the downloads directory
        files_dir = "downloaded_files"
        file_list = os.listdir(files_dir)
        
        # Find replacement files using our helper function
        replacement_files = [f for f in file_list if is_replacement_file(f)]
        
        # Check for single-date files that may extend our date range
        start_date_obj = datetime.strptime(start_date, '%d.%m.%Y').date()
        end_date_obj = datetime.strptime(end_date, '%d.%m.%Y').date()
        
        # Expand the date range if needed based on single-date files
        for file in replacement_files:
            if "-" not in file:  # Single date file
                try:
                    single_date_str = file.replace('.xlsx', '')
                    try:
                        # Try DD.MM.YY format
                        single_date = datetime.strptime(single_date_str, '%d.%m.%y').date()
                    except ValueError:
                        try:
                            # Try DD.MM.YYYY format
                            single_date = datetime.strptime(single_date_str, '%d.%m.%Y').date()
                        except ValueError:
                            # Not a date format we recognize
                            continue
                    
                    # If this single date is outside our current range but should be included,
                    # expand the range
                    if single_date < start_date_obj:
                        start_date_obj = single_date
                        start_date = single_date.strftime('%d.%m.%Y')
                        logger.info(f"Extended start date to {start_date} due to single date file {file}")
                    elif single_date > end_date_obj:
                        end_date_obj = single_date
                        end_date = single_date.strftime('%d.%m.%Y')
                        logger.info(f"Extended end date to {end_date} due to single date file {file}")
                except Exception as e:
                    logger.error(f"Error processing single date file {file}: {e}")
                    continue
        
        # Process each date in the (possibly expanded) range
        all_schedules = {}
        
        current_date = start_date_obj
        while current_date <= end_date_obj:
            if current_date.weekday() != 6:  # Skip Sundays
                date_str = current_date.strftime('%d.%m.%Y')
                
                date_schedule = {}
                for file in replacement_files:
                    file_path = os.path.join(files_dir, file)
                    
                    # We're directly checking each replacement file
                    try:
                        # Use a simplified parser with a short timeout
                        result = await asyncio.wait_for(
                            run_blocking(parse_teacher_schedule, file_path, date_str, teacher_name),
                            timeout=2  # Very short timeout per file
                        )
                        if result:
                            date_schedule.update(result)
                    except (asyncio.TimeoutError, Exception):
                        # Skip this file on any error
                        continue
                
                if date_schedule:
                    all_schedules[date_str] = date_schedule
                
            current_date += timedelta(days=1)
        
        # Format the minimal schedule we found
        if all_schedules:
            formatted_schedule = format_teacher_schedule(all_schedules, teacher_name, start_date, end_date)
            # Cache it with a short expiration since it's potentially incomplete
            cache_teacher_schedule(teacher_name, start_date, end_date, formatted_schedule, expiration=600)  # 10 minutes
            return formatted_schedule
        else:
            return f"Расписание для {teacher_name} на указанный период не найдено"
            
    except Exception as e:
        logger.error(f"Error in simplified schedule method: {e}")
        return f"Не удалось получить расписание для {teacher_name}. Пожалуйста, попробуйте позже."

def patch_get_teacher_schedule():
    """
    Monkey patch the original get_teacher_schedule function with our wrapper.
    This should be called at the start of the application.
    """
    import было
    было.get_teacher_schedule = get_teacher_schedule
    logger.info("Teacher schedule function has been patched with optimized indexed version")

# Function to measure and log execution time
def log_execution_time(func):
    @wraps(func)
    async def wrapper(*args, **kwargs):
        start_time = datetime.now()
        result = await func(*args, **kwargs)
        end_time = datetime.now()
        execution_time = (end_time - start_time).total_seconds()
        logger.info(f"{func.__name__} executed in {execution_time:.2f} seconds")
        return result
    return wrapper

# Function to cancel all ongoing teacher schedule requests
def cancel_all_schedule_requests():
    """Cancel all ongoing teacher schedule requests."""
    with ongoing_requests_lock:
        for key, task in list(ongoing_requests.items()):
            task.cancel()
        ongoing_requests.clear()
    logger.info("Cancelled all ongoing teacher schedule requests") 