import logging
import os
import sys
import signal
import asyncio
import nest_asyncio
import traceback
import json  # Add this import
from было import main as original_main
from schedule_wrapper import patch_get_teacher_schedule
from cache_utils import init_cache, selective_cache_clear
from telegram import Bot  # Add this import
# Configure logging
logging.basicConfig(
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    level=logging.INFO
)
logger = logging.getLogger(__name__)

# Flag to track if we've already patched the application
application_patched = False

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
        init_subscribers_file()
        patch_было_module()
        patch_notification_system()
        
        # Очистка кэша при запуске с указанием причины
        selective_cache_clear(reason="startup")
        
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
                
                # Call the original main function and ensure it returns the application
                app = original_было_main()
                logger.info("Bot application created successfully")
                return app
            
            # Apply the patch
            было.main = patched_было_main
            logger.info("Successfully patched было.main")
        except Exception as e:
            logger.error(f"Error patching было.main: {e}")
            logger.error(traceback.format_exc())
        
        # Call the original main function and ensure it starts polling
        app = original_main()
        if app:
            logger.info("Starting bot polling...")
            # Make sure the bot starts polling if it hasn't already
            if hasattr(app, 'running') and not app.running:
                app.run_polling(drop_pending_updates=True)
                logger.info("Bot polling started")
        else:
            logger.error("Failed to get application instance from original_main")
        
    except Exception as e:
        logger.error(f"Error in enhanced main: {e}")
        logger.error(traceback.format_exc())
        sys.exit(1)

if __name__ == "__main__":
    main()