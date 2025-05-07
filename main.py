import logging
import os
import sys
import signal
import asyncio
import nest_asyncio
import traceback
import json  # Add this import
import time
import platform
import subprocess
from было import main as original_main
from schedule_wrapper import patch_get_teacher_schedule
from cache_utils import init_cache, selective_cache_clear
from telegram import Bot  # Add this import
from teacher_schedule_processor import start_background_processor  # Add this import

# Configure logging
logging.basicConfig(
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    level=logging.INFO
)
logger = logging.getLogger(__name__)

# Flag to track if we've already patched the application
application_patched = False

# Path to the lock file
LOCK_FILE = "bot_instance.lock"

# Check if another instance is running
def is_another_instance_running():
    # Check for lock file
    if os.path.exists(LOCK_FILE):
        try:
            # Read PID from lock file
            with open(LOCK_FILE, 'r') as f:
                pid = int(f.read().strip())
            
            # Check if process with this PID is running
            if platform.system() == "Windows":
                # On Windows, use tasklist
                try:
                    output = subprocess.check_output(f'tasklist /FI "PID eq {pid}"', shell=True).decode()
                    if str(pid) in output:
                        return True
                except:
                    pass  # If command fails, assume no process is running
            else:
                # On Linux/Unix, check /proc directory
                if os.path.exists(f"/proc/{pid}"):
                    return True
            
            # If we get here, the process is not running, so remove stale lock file
            os.remove(LOCK_FILE)
            return False
        except:
            # If there's any error, remove lock file and continue
            try:
                os.remove(LOCK_FILE)
            except:
                pass
            return False
    
    return False

# Create lock file
def create_lock_file():
    try:
        with open(LOCK_FILE, 'w') as f:
            f.write(str(os.getpid()))
        logger.info(f"Created lock file with PID {os.getpid()}")
    except Exception as e:
        logger.error(f"Error creating lock file: {e}")

# Remove lock file when bot stops
def remove_lock_file():
    try:
        if os.path.exists(LOCK_FILE):
            os.remove(LOCK_FILE)
            logger.info("Removed lock file")
    except Exception as e:
        logger.error(f"Error removing lock file: {e}")

# Register cleanup handler
def cleanup_on_exit():
    remove_lock_file()

# Monkey patch original signal handler to ensure lock file cleanup
original_signal_handler = signal.signal
def patched_signal_handler(sig, frame):
    remove_lock_file()
    # Call original handler if it exists
    if hasattr(signal, 'original_handler') and signal.original_handler:
        signal.original_handler(sig, frame)
    sys.exit(0)

# Initialize subscribers file if it doesn't exist
def init_subscribers_file():
    subscribers_file = os.path.join(os.path.dirname(__file__), "subscribers.json")
    if not os.path.exists(subscribers_file):
        try:
            with open(subscribers_file, 'w') as f:
                json.dump({}, f)
            logger.info("Created empty subscribers.json file")
        except Exception as e:
            logger.error(f"Error creating subscribers file: {e}")
            logger.error(traceback.format_exc())

# Patch the было module to ensure json is imported
def patch_было_module():
    try:
        import было
        # Check if json is already imported in было
        if 'json' not in было.__dict__:
            import json as json_module
            было.json = json_module
            logger.info("Successfully patched было module with json import")
    except Exception as e:
        logger.error(f"Error patching было module: {e}")
        logger.error(traceback.format_exc())

# Patch the было module to ensure notification checking is properly set up
def patch_notification_system():
    try:
        import было
        
        async def send_notifications():
            try:
                # Only check new_replacements_notify.json
                notification_file = "new_replacements_notify.json"
                
                if os.path.exists(notification_file):
                    logger.info(f"Found notifications file: {notification_file}")
                    with open(notification_file, "r", encoding='utf-8') as f:
                        notification_data = json.load(f)
                    у
                    message = notification_data.get("message", "")
                    chat_ids = notification_data.get("chat_ids", [])
                    
                    if message and chat_ids:
                        bot = Bot(token="5849256613:AAH34MtjRPyBhrtQouFseQzVw5G9KJsX1WQ")
                        
                        for chat_id in chat_ids:
                            try:
                                await bot.send_message(
                                    chat_id=int(chat_id), 
                                    text=message,
                                    parse_mode='Markdown'
                                )
                                logger.info(f"Notification sent to chat ID {chat_id}")
                            except Exception as e:
                                logger.error(f"Error sending notification to chat ID {chat_id}: {e}")
                        
                        try:
                            os.remove(notification_file)
                            logger.info(f"Notification file {notification_file} deleted after sending")
                        except Exception as e:
                            logger.error(f"Error removing notification file {notification_file}: {e}")
                            
            except Exception as e:
                logger.error(f"Error processing notifications: {e}")
                logger.error(traceback.format_exc())

        # Add notification checker thread
        def start_notification_checker():
            import threading
            import time
            
            def notification_checker():
                logger.info("Starting notification checker thread")
                while True:
                    try:
                        # Create event loop for async operation
                        loop = asyncio.new_event_loop()
                        asyncio.set_event_loop(loop)
                        loop.run_until_complete(send_notifications())
                        loop.close()
                        time.sleep(30)  # Check every 30 seconds
                    except Exception as e:
                        logger.error(f"Error in notification checker thread: {e}")
                        time.sleep(60)  # Wait longer if there's an error
            
            # Start the thread
            thread = threading.Thread(target=notification_checker, daemon=True)
            thread.start()
            logger.info("Notification checker thread started")
        
        # Start the notification checker
        start_notification_checker()
        logger.info("Notification system initialized")
        
    except Exception as e:
        logger.error(f"Error patching notification system: {e}")
        logger.error(traceback.format_exc())
        
def main():
    """
    Enhanced main function that patches the teacher schedule function
    and enables concurrent command handling before starting the bot.
    """
    global application_patched
    
    try:
        # Check if another instance is already running
        if is_another_instance_running():
            logger.error("Another instance of the bot is already running. Exiting.")
            sys.exit(1)
        
        # Create lock file to prevent multiple instances
        create_lock_file()
        
        # Register cleanup on exit
        import atexit
        atexit.register(cleanup_on_exit)
        
        # Patch signal handlers to ensure cleanup
        for sig in [signal.SIGINT, signal.SIGTERM]:
            signal.original_handler = signal.getsignal(sig)
            signal.signal(sig, patched_signal_handler)
        
        init_subscribers_file()
        patch_было_module()
        patch_notification_system()
        
        # Очистка кэша при запуске с указанием причины
        selective_cache_clear(reason="startup")
        # Специальная очистка кэша для расписаний преподавателей
        selective_cache_clear(pattern="teacher_*", reason="teacher schedule date fix")
        
        # Инициализация кэша
        init_cache()
        
        # Import and apply patches with better error handling
        try:
            from bot_concurrency import patch_application_handlers, apply_all_concurrency_patches
            # Apply all concurrency patches for true parallel processing
            apply_all_concurrency_patches()
            logger.info("Successfully applied concurrency patches")
        except Exception as e:
            logger.error(f"Error applying concurrency patches: {e}")
            logger.error(traceback.format_exc())
            logger.warning("Continuing without concurrency enhancements")
        
        # Patch the get_teacher_schedule function with our optimized version
        try:
            patch_get_teacher_schedule()
            logger.info("Successfully patched teacher schedule function")
            
            # Start the background processor for teacher schedules
            start_background_processor()
            logger.info("Started background teacher schedule processor")
        except Exception as e:
            logger.error(f"Error patching teacher schedule function: {e}")
            logger.error(traceback.format_exc())
            logger.warning("Continuing with original teacher schedule implementation")
        
        # Patch Excel functions to use caching
        try:
            from excel_cache import patch_excel_functions, start_file_monitor, preload_excel_files
            patch_excel_functions()
            logger.info("Successfully patched Excel functions")
            
            # Start file monitor thread
            start_file_monitor()
            logger.info("Started file monitor thread")
            
            # Preload Excel files to improve first-time performance
            preload_excel_files()
            logger.info("Started Excel file preloading")
        except Exception as e:
            logger.error(f"Error setting up Excel caching: {e}")
            logger.error(traceback.format_exc())
            logger.warning("Continuing without Excel caching enhancements")
        
        logger.info("Starting bot with available enhancements")
        
        # Monkey patch the Application.run_polling method if available
        try:
            from telegram.ext import Application
            original_run_polling = Application.run_polling
            
            def patched_run_polling(self, *args, **kwargs):
                """Patched version of run_polling that applies concurrency enhancements"""
                global application_patched
                
                try:
                    # Apply concurrency patches to all handlers if available
                    if not application_patched and hasattr(self, 'handlers') and self.handlers:
                        from bot_concurrency import patch_application_handlers
                        patch_application_handlers(self)
                        application_patched = True
                        logger.info("Applied concurrency patches to application handlers")
                except Exception as e:
                    logger.error(f"Error patching application handlers: {e}")
                    logger.error(traceback.format_exc())
                
                # Ensure drop_pending_updates is True to avoid processing old updates
                kwargs['drop_pending_updates'] = True
                
                # Call the original method
                return original_run_polling(self, *args, **kwargs)
            
            # Apply the patch
            Application.run_polling = patched_run_polling
            logger.info("Successfully patched Application.run_polling")
        except Exception as e:
            logger.error(f"Error patching Application.run_polling: {e}")
            logger.error(traceback.format_exc())
        
        # Patch the original main function to ensure we can intercept the application creation
        try:
            import было
            original_было_main = было.main
            
            def patched_было_main():
                # Set the asyncio policy to allow more concurrent operations
                try:
                    import nest_asyncio
                    nest_asyncio.apply()
                    logger.info("Applied nest_asyncio to allow nested event loops")
                except ImportError:
                    logger.warning("nest_asyncio not available, skipping")
                
                # Patch the Application.builder method to set concurrent_updates
                try:
                    from telegram.ext import Application
                    original_builder = Application.builder
                    
                    def patched_builder():
                        builder = original_builder()
                        # Store the original build method
                        original_build = builder.build
                        
                        # Create a patched build method that sets concurrent_updates
                        def patched_build(*args, **kwargs):
                            # Set concurrent_updates in kwargs
                            kwargs['concurrent_updates'] = 30
                            logger.info("Setting concurrent_updates to 30 in Application builder")
                            # Call the original build method
                            return original_build(*args, **kwargs)
                        
                        # Replace the build method
                        builder.build = patched_build
                        return builder
                    
                    # Apply the patch
                    Application.builder = patched_builder
                    logger.info("Successfully patched Application.builder to set concurrent_updates")
                except Exception as e:
                    logger.error(f"Error patching Application.builder: {e}")
                
                # IMPORTANT MODIFICATION: Create a modified version of Application.run_polling 
                # that doesn't actually start polling
                try:
                    from telegram.ext import Application
                    original_run_polling = Application.run_polling
                    
                    def no_op_run_polling(self, *args, **kwargs):
                        """This version doesn't actually run polling, just returns self"""
                        logger.info("Prevented double polling by intercepting run_polling call")
                        return self
                    
                    # Temporarily replace Application.run_polling with our no-op version
                    Application.run_polling = no_op_run_polling
                    
                    # Call the original main function to set up the application
                    app = original_было_main()
                    
                    # Restore the original run_polling method
                    Application.run_polling = original_run_polling
                    
                    logger.info("Bot application created successfully, prevented double polling")
                    return app
                except Exception as e:
                    logger.error(f"Error intercepting run_polling: {e}")
                    # If our interception fails, just call the original function
                    return original_было_main()
            
            # Apply the patch
            было.main = patched_было_main
            logger.info("Successfully patched было.main to prevent double polling")
        except Exception as e:
            logger.error(f"Error patching было.main: {e}")
            logger.error(traceback.format_exc())
        
        # Call the original main function to get the application instance
        app = original_main()
        if app:
            logger.info("Bot application obtained, starting single polling instance")
            # Now we can safely start polling - the original call was intercepted
            if hasattr(app, 'running') and not app.running:
                app.run_polling(drop_pending_updates=True)
                logger.info("Bot polling started successfully")
        else:
            logger.error("Failed to get application instance from original_main")
        
    except Exception as e:
        logger.error(f"Error in enhanced main: {e}")
        logger.error(traceback.format_exc())
        sys.exit(1)

def main_direct():
    """
    A completely standalone version of main() that bypasses
    the было.main() function entirely and just starts a clean bot instance.
    This function is used only as a fallback.
    """
    try:
        # Check if another instance is already running
        if is_another_instance_running():
            logger.error("Another instance of the bot is already running. Exiting.")
            sys.exit(1)
        
        # Create lock file to prevent multiple instances
        create_lock_file()
        
        # Register cleanup on exit
        import atexit
        atexit.register(cleanup_on_exit)
        
        # Patch signal handlers to ensure cleanup
        for sig in [signal.SIGINT, signal.SIGTERM]:
            signal.original_handler = signal.getsignal(sig)
            signal.signal(sig, patched_signal_handler)
        
        # Initialize basic systems
        init_subscribers_file()
        patch_было_module()
        patch_notification_system()
        
        # Clear and initialize cache
        selective_cache_clear(reason="startup")
        init_cache()
        
        # Apply concurrency patches
        try:
            from bot_concurrency import patch_application_handlers, apply_all_concurrency_patches
            apply_all_concurrency_patches()
            logger.info("Successfully applied concurrency patches")
        except Exception as e:
            logger.error(f"Error applying concurrency patches: {e}")
            logger.error(traceback.format_exc())
        
        # Optimize teacher schedule
        try:
            patch_get_teacher_schedule()
            logger.info("Successfully patched teacher schedule function")
            
            # Start the background processor for teacher schedules
            start_background_processor()
            logger.info("Started background teacher schedule processor")
        except Exception as e:
            logger.error(f"Error patching teacher schedule function: {e}")
        
        # Setup Excel caching
        try:
            from excel_cache import patch_excel_functions, start_file_monitor, preload_excel_files
            patch_excel_functions()
            logger.info("Successfully patched Excel functions")
            
            # Start file monitor thread
            start_file_monitor()
            logger.info("Started file monitor thread")
            
            # Preload Excel files to improve first-time performance
            preload_excel_files()
            logger.info("Started Excel file preloading")
        except Exception as e:
            logger.error(f"Error setting up Excel caching: {e}")
        
        # Create a direct application instance
        from telegram.ext import Application
        from было import TELEGRAM_TOKEN
        
        # Create application instance directly
        app = Application.builder().token(TELEGRAM_TOKEN).build()
        
        # Import all the necessary handlers
        from было import (
            start, subscribe_command, unsubscribe_command, manual_clear_cache,
            classroom_schedule_command, get_my_id, error_handler, cancel,
            CHOOSE_ACTION, ENTER_CLASSROOM, CHOOSE_DATE_FOR_CLASSROOM,
            ENTER_TEACHER, CHOOSE_DATE_FOR_TEACHER, ENTER_GROUP, CHOOSE_SUBGROUP,
            choose_action, enter_classroom, choose_date_for_classroom,
            enter_teacher, choose_date_for_teacher, group_input, subgroup_choice,
            handle_all_messages, set_commands
        )
        from telegram.ext import CommandHandler, MessageHandler, filters, ConversationHandler
        
        # Apply concurrency to handlers if available
        try:
            from bot_concurrency import patch_application_handlers
            patch_application_handlers(app)
            logger.info("Applied concurrency patches to application handlers")
        except Exception as e:
            logger.error(f"Error patching application handlers: {e}")
        
        # Add handlers
        app.add_handler(CommandHandler("start", start))
        app.add_handler(CommandHandler("subscribe", subscribe_command))
        app.add_handler(CommandHandler("unsubscribe", unsubscribe_command))
        app.add_handler(CommandHandler("clear_cache", manual_clear_cache))
        app.add_handler(CommandHandler("classroom", classroom_schedule_command))
        app.add_handler(CommandHandler("myid", get_my_id))
        
        # Add conversation handler
        conv_handler = ConversationHandler(
            entry_points=[
                CommandHandler('start', start),
                MessageHandler(filters.TEXT & ~filters.COMMAND, handle_all_messages),
            ],
            states={
                CHOOSE_ACTION: [
                    MessageHandler(filters.TEXT & ~filters.COMMAND, choose_action),
                    MessageHandler(filters.Regex('^Отмена$'), cancel)
                ],
                ENTER_CLASSROOM: [
                    MessageHandler(filters.TEXT & ~filters.COMMAND, enter_classroom),
                    MessageHandler(filters.Regex('^Отмена$'), cancel)
                ],
                CHOOSE_DATE_FOR_CLASSROOM: [
                    MessageHandler(filters.TEXT & ~filters.COMMAND, choose_date_for_classroom),
                    MessageHandler(filters.Regex('^Отмена$'), cancel)
                ],
                ENTER_TEACHER: [
                    MessageHandler(filters.TEXT & ~filters.COMMAND, enter_teacher),
                    MessageHandler(filters.Regex('^Отмена$'), cancel)
                ],
                CHOOSE_DATE_FOR_TEACHER: [
                    MessageHandler(filters.TEXT & ~filters.COMMAND, choose_date_for_teacher),
                    MessageHandler(filters.Regex('^Отмена$'), cancel)
                ],
                ENTER_GROUP: [
                    MessageHandler(filters.TEXT & ~filters.COMMAND, group_input),
                    MessageHandler(filters.Regex('^Отмена$'), cancel)
                ],             
                CHOOSE_SUBGROUP: [
                    MessageHandler(filters.TEXT & ~filters.COMMAND, subgroup_choice),
                    MessageHandler(filters.Regex('^Отмена$'), cancel)
                ],
            },
            fallbacks=[CommandHandler('cancel', cancel)],
        )
        app.add_handler(conv_handler)
        
        # Add error handler
        app.add_error_handler(error_handler)
        
        # Start command setting
        loop = asyncio.get_event_loop()
        loop.run_until_complete(set_commands(app))
        
        # Start Dropbox sync thread if needed
        try:
            from было import schedule_sync
            import threading
            sync_thread = threading.Thread(target=schedule_sync)
            sync_thread.daemon = True
            sync_thread.start()
            logger.info("Started Dropbox sync thread")
        except Exception as e:
            logger.error(f"Error starting Dropbox sync: {e}")
        
        # Start the bot
        logger.info("Starting bot in direct mode with all optimizations")
        app.run_polling(drop_pending_updates=True)
        
    except Exception as e:
        logger.error(f"Error in direct main: {e}")
        logger.error(traceback.format_exc())
        sys.exit(1)

if __name__ == "__main__":
    try:
        # Run the direct mode by default instead of trying to patch the original
        logger.info("Starting in direct mode to avoid conflicts")
        main_direct()
    except Exception as e:
        logger.error(f"Critical error in direct mode: {e}")
        logger.error(traceback.format_exc())
        sys.exit(1)