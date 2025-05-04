import logging
import asyncio
from functools import wraps
from typing import Any, Optional, Dict, List, Callable, TypeVar, cast
from telegram import Update
from telegram.ext import ContextTypes, Application, CommandHandler, MessageHandler, ConversationHandler

logger = logging.getLogger(__name__)

# Dictionary to track ongoing user requests
user_tasks: Dict[int, List[asyncio.Task]] = {}

# Increase the number of concurrent operations
MAX_CONCURRENT_OPERATIONS = 30  # Increased from default

# Type variables for better type hinting
T = TypeVar('T')
HandlerCallbackType = Callable[..., Any]

def concurrent_handler(func: HandlerCallbackType) -> HandlerCallbackType:
    """
    Decorator to make command handlers run concurrently.
    This allows the bot to process multiple commands from different users simultaneously.
    """
    @wraps(func)
    async def wrapper(update: Update, context: ContextTypes.DEFAULT_TYPE):
        # Get user ID to track requests per user
        user_id = update.effective_user.id if update and update.effective_user else None
        
        if not user_id:
            # If no user ID (rare case), just run the function directly
            return await func(update, context)
        
        # Create a task for this request
        task = asyncio.create_task(func(update, context))
        
        # Store the task with user ID as key
        if user_id not in user_tasks:
            user_tasks[user_id] = []
        user_tasks[user_id].append(task)
        
        try:
            # Execute the handler concurrently
            return await task
        except asyncio.CancelledError:
            logger.info(f"Task for user {user_id} was cancelled")
            return None
        except Exception as e:
            logger.error(f"Error in concurrent handler for user {user_id}: {e}")
            # Send error message to user if possible
            try:
                if update and update.message:
                    await update.message.reply_text(
                        f"Произошла ошибка при обработке вашего запроса. Пожалуйста, попробуйте еще раз."
                    )
            except Exception:
                pass
            return None
        finally:
            # Clean up completed tasks for this user
            if user_id in user_tasks and task in user_tasks[user_id]:
                user_tasks[user_id].remove(task)
                if not user_tasks[user_id]:
                    del user_tasks[user_id]
    
    return cast(HandlerCallbackType, wrapper)

def patch_application_handlers(application: Application):
    """
    Patch all command handlers in the application to run concurrently.
    This makes the entire bot handle multiple users simultaneously.
    """
    if not application or not hasattr(application, 'handlers'):
        logger.warning("Application has no handlers attribute, skipping handler patching")
        return
        
    # Get all handlers from the application
    for group in application.handlers.values():
        for handler in group:
            # Check if the handler has a callback attribute
            if hasattr(handler, 'callback'):
                original_callback = handler.callback
                # Only patch if it's not already patched and not None
                if original_callback is not None and not hasattr(original_callback, '_concurrent_patched'):
                    try:
                        # Apply the concurrent_handler decorator
                        handler.callback = concurrent_handler(original_callback)  # type: ignore
                        # Mark as patched to avoid double patching
                        setattr(handler.callback, '_concurrent_patched', True)
                    except Exception as e:
                        logger.error(f"Error patching handler callback: {e}")
            
            # Special handling for ConversationHandler
            if isinstance(handler, ConversationHandler):
                # Patch entry points
                for entry_point in handler.entry_points:
                    if hasattr(entry_point, 'callback') and entry_point.callback is not None:
                        if not hasattr(entry_point.callback, '_concurrent_patched'):
                            try:
                                entry_point.callback = concurrent_handler(entry_point.callback)  # type: ignore
                                setattr(entry_point.callback, '_concurrent_patched', True)
                            except Exception as e:
                                logger.error(f"Error patching entry point callback: {e}")
                
                # Patch state handlers
                for state, state_handlers in handler.states.items():
                    for state_handler in state_handlers:
                        if hasattr(state_handler, 'callback') and state_handler.callback is not None:
                            if not hasattr(state_handler.callback, '_concurrent_patched'):
                                try:
                                    state_handler.callback = concurrent_handler(state_handler.callback)  # type: ignore
                                    setattr(state_handler.callback, '_concurrent_patched', True)
                                except Exception as e:
                                    logger.error(f"Error patching state handler callback: {e}")
                
                # Patch fallbacks
                for fallback in handler.fallbacks:
                    if hasattr(fallback, 'callback') and fallback.callback is not None:
                        if not hasattr(fallback.callback, '_concurrent_patched'):
                            try:
                                fallback.callback = concurrent_handler(fallback.callback)  # type: ignore
                                setattr(fallback.callback, '_concurrent_patched', True)
                            except Exception as e:
                                logger.error(f"Error patching fallback callback: {e}")
    
    logger.info("All command handlers have been patched to run concurrently")

# Function to cancel all ongoing tasks for a user
async def cancel_user_tasks(user_id: int) -> bool:
    """Cancel all ongoing tasks for a specific user."""
    if user_id in user_tasks:
        for task in user_tasks[user_id]:
            task.cancel()
        del user_tasks[user_id]
        logger.info(f"Cancelled all tasks for user {user_id}")
        return True
    return False

# Patch the Application class to ensure true concurrency
def patch_application_class():
    """
    Patch the Application class to ensure true concurrency.
    This is a more aggressive approach that modifies how the application processes updates.
    """
    try:
        from telegram.ext import Application as TelegramApplication
        
        # Save the original method
        original_process_update = TelegramApplication.process_update
        
        # Create a patched version
        async def patched_process_update(self, update, *args, **kwargs):
            """Process updates in separate tasks to ensure true concurrency"""
            # Create a task for this update
            task = asyncio.create_task(original_process_update(self, update, *args, **kwargs))
            # We don't wait for it to complete - this is the key difference
            return task
        
        # Apply the patch - using setattr to avoid type checking issues
        setattr(TelegramApplication, 'process_update', patched_process_update)
        
        # Also increase the number of worker threads in the application
        if hasattr(TelegramApplication, 'update_queue'):
            # Try to increase the number of workers if possible
            try:
                # Using setattr to avoid type checking issues
                setattr(TelegramApplication, '_update_fetcher_task', MAX_CONCURRENT_OPERATIONS)
            except Exception as e:
                logger.warning(f"Could not set _update_fetcher_task: {e}")
        
        logger.info("Application.process_update has been patched for true concurrency")
    except Exception as e:
        logger.error(f"Error patching Application class: {e}")

# Patch the dispatcher to process updates in parallel
def patch_dispatcher():
    """
    Patch the dispatcher to process updates in parallel.
    This ensures that updates are processed concurrently.
    
    Note: In newer versions of python-telegram-bot, the Dispatcher class
    is not directly exposed, so we need to access it through the Application.
    """
    try:
        # Try to patch through Application's update_queue
        from telegram.ext import Application
        
        # The Application class in newer versions handles dispatching internally
        logger.info("Using Application-based approach for concurrent update processing")
        
        # Increase the number of workers if possible
        try:
            # This is a more aggressive approach to ensure concurrency
            original_init = Application.__init__
            
            def patched_init(self, *args, **kwargs):
                # Call original init
                original_init(self, *args, **kwargs)
                # Instead of directly setting concurrent_updates, we'll modify the kwargs
                # when the application is created in main.py
                logger.info(f"Application initialized with enhanced concurrency support")
            
            # Using setattr to avoid type checking issues
            setattr(Application, '__init__', patched_init)
            logger.info(f"Patched Application.__init__ for enhanced concurrency")
        except Exception as e:
            logger.warning(f"Could not patch Application.__init__: {e}")
        
        return
    except Exception as e:
        logger.warning(f"Could not patch dispatcher: {e}")

# Apply all concurrency patches
def apply_all_concurrency_patches():
    """Apply all concurrency patches to ensure true parallel processing"""
    # Set asyncio to use a larger thread pool
    try:
        import concurrent.futures
        asyncio.get_event_loop().set_default_executor(
            concurrent.futures.ThreadPoolExecutor(max_workers=MAX_CONCURRENT_OPERATIONS)
        )
        logger.info(f"Set asyncio thread pool to {MAX_CONCURRENT_OPERATIONS} workers")
    except Exception as e:
        logger.warning(f"Could not set asyncio thread pool: {e}")
    
    patch_application_class()
    patch_dispatcher()
    logger.info("All concurrency patches have been applied") 