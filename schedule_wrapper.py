import logging
import asyncio
from functools import wraps
from datetime import datetime
import threading

from teacher_schedule_processor import get_teacher_schedule_optimized, preload_teacher_schedules
from было import get_teacher_schedule as original_get_teacher_schedule

logger = logging.getLogger(__name__)

# Dictionary to track ongoing teacher schedule requests
ongoing_requests = {}
ongoing_requests_lock = threading.Lock()

# Semaphore to limit concurrent teacher schedule requests
# This prevents overwhelming the system with too many requests
MAX_CONCURRENT_SCHEDULE_REQUESTS = 20  # Increased from 15
schedule_semaphore = asyncio.Semaphore(MAX_CONCURRENT_SCHEDULE_REQUESTS)

# Flag to track if initial preloading has completed
initial_preload_done = False
initial_preload_lock = threading.Lock()

async def get_teacher_schedule_wrapper(teacher_name: str, start_date: str, end_date: str) -> str:
    """
    Wrapper for get_teacher_schedule that uses the optimized version
    but falls back to the original if needed.
    """
    request_key = f"{teacher_name}_{start_date}_{end_date}"
    
    # Use a lock to safely check and update the ongoing_requests dictionary
    task = None
    with ongoing_requests_lock:
        # Check if there's already an ongoing request for this teacher and date range
        if request_key in ongoing_requests:
            task = ongoing_requests[request_key]
            logger.info(f"Joining existing request for {teacher_name} from {start_date} to {end_date}")
    
    if task:
        try:
            # Wait for the existing request to complete
            return await task
        except Exception as e:
            logger.error(f"Error waiting for existing request: {e}")
            # If the existing request failed, we'll try again with a new request
            # Remove the failed task from ongoing requests
            with ongoing_requests_lock:
                if request_key in ongoing_requests and ongoing_requests[request_key] == task:
                    del ongoing_requests[request_key]
    
    # Use a semaphore to limit concurrent requests
    async with schedule_semaphore:
        # Trigger initial preload if not done yet
        global initial_preload_done
        if not initial_preload_done:
            with initial_preload_lock:
                if not initial_preload_done:
                    try:
                        # Start preloading popular teacher schedules in the background
                        asyncio.create_task(preload_teacher_schedules())
                        initial_preload_done = True
                        logger.info("Started initial preloading of popular teacher schedules")
                    except Exception as e:
                        logger.error(f"Error starting initial preload: {e}")
        
        # Create a new task for this request
        new_task = asyncio.create_task(get_teacher_schedule_optimized(teacher_name, start_date, end_date))
        
        # Register the task in ongoing_requests
        with ongoing_requests_lock:
            ongoing_requests[request_key] = new_task
        
        try:
            # Wait for the task to complete
            start_time = datetime.now()
            result = await new_task
            end_time = datetime.now()
            execution_time = (end_time - start_time).total_seconds()
            logger.info(f"Teacher schedule for {teacher_name} generated in {execution_time:.2f} seconds")
            return result
        except asyncio.CancelledError:
            logger.warning(f"Teacher schedule request for {teacher_name} was cancelled")
            # Fall back to the original implementation
            return await original_get_teacher_schedule(teacher_name, start_date, end_date)
        except Exception as e:
            logger.error(f"Error in optimized teacher schedule: {e}")
            # Fall back to the original implementation if the optimized version fails
            logger.info(f"Falling back to original implementation for {teacher_name}")
            try:
                return await original_get_teacher_schedule(teacher_name, start_date, end_date)
            except Exception as fallback_error:
                logger.error(f"Error in fallback teacher schedule: {fallback_error}")
                return f"Ошибка при получении расписания преподавателя. Пожалуйста, попробуйте позже."
        finally:
            # Remove the task from ongoing requests
            with ongoing_requests_lock:
                if request_key in ongoing_requests and ongoing_requests[request_key] == new_task:
                    del ongoing_requests[request_key]

def patch_get_teacher_schedule():
    """
    Monkey patch the original get_teacher_schedule function with our wrapper.
    This should be called at the start of the application.
    """
    import было
    было.get_teacher_schedule = get_teacher_schedule_wrapper
    logger.info("Teacher schedule function has been patched with optimized version")

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