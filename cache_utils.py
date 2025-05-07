import json
import os
import logging
from datetime import datetime
import threading
import glob
import time
import fnmatch
from typing import Optional, Dict, Any

logger = logging.getLogger(__name__)

CACHE_DIR = "cache"
STUDENT_CACHE_FILE = os.path.join(CACHE_DIR, "student_schedule_cache.json")
TEACHER_CACHE_FILE = os.path.join(CACHE_DIR, "teacher_schedule_cache.json")
CLASSROOM_CACHE_FILE = os.path.join(CACHE_DIR, "classroom_schedule_cache.json")  # Added new cache file
cache_lock = threading.Lock()

# Флаг для отслеживания, была ли уже выполнена очистка кэша при запуске
cache_cleared_on_startup = False

def init_cache():
    """Инициализация кэша при запуске"""
    try:
        if not os.path.exists(CACHE_DIR):
            os.makedirs(CACHE_DIR)

        # Создаем пустые файлы кэша, если они не существуют
        for cache_file in [STUDENT_CACHE_FILE, TEACHER_CACHE_FILE, CLASSROOM_CACHE_FILE]:  # Added CLASSROOM_CACHE_FILE
            if not os.path.exists(cache_file):
                with open(cache_file, 'w', encoding='utf-8') as f:
                    json.dump({}, f)

        logger.info("Кэш успешно инициализирован")
    except Exception as e:
        logger.error(f"Ошибка при инициализации кэша: {e}")


def clear_cache():
    """Очистка кэша"""
    try:
        with cache_lock:
            for cache_file in [STUDENT_CACHE_FILE, TEACHER_CACHE_FILE, CLASSROOM_CACHE_FILE]:  # Added CLASSROOM_CACHE_FILE
                with open(cache_file, 'w', encoding='utf-8') as f:
                    json.dump({}, f)
        logger.info("Кэш успешно очищен")
    except Exception as e:
        logger.error(f"Ошибка при очистки кэша: {e}")


def selective_cache_clear(pattern: str = "*", reason: str = "manual"):
    """
    Очистка кэша только при определенных условиях:
    1. При запуске бота (reason="startup")
    2. При нахождении новых замен (reason="new_replacements")
    
    Args:
        pattern: Glob pattern to match against cache filenames (default "*" for all files)
        reason: Reason for clearing the cache (for logging)
    """
    global cache_cleared_on_startup
    
    # If selective clearing by pattern is requested (non-default pattern)
    if pattern != "*":
        try:
            if os.path.exists(CACHE_DIR):
                files = glob.glob(os.path.join(CACHE_DIR, "*"))
                matching_files = [f for f in files if fnmatch.fnmatch(os.path.basename(f), pattern)]
                
                for file in matching_files:
                    try:
                        os.remove(file)
                        logger.info(f"Removed cache file {os.path.basename(file)}")
                    except Exception as e:
                        logger.error(f"Error removing cache file {file}: {e}")
                
                logger.info(f"Cleared {len(matching_files)} files matching '{pattern}' from cache. Reason: {reason}")
                
                # If we're clearing specific patterns, we don't need to do the full cache clearing logic below
                return
        except Exception as e:
            logger.error(f"Error during selective cache clearing: {e}")
    
    # Original behavior for standard reasons
    if reason == "startup":
        # Очищаем кэш только один раз при запуске
        if not cache_cleared_on_startup:
            logger.info("Очистка кэша при запуске бота")
            clear_cache()
            # Очищаем также кэш Excel файлов
            try:
                from excel_cache import clear_excel_cache
                clear_excel_cache()
                logger.info("Excel кэш очищен при запуске бота")
            except Exception as e:
                logger.error(f"Ошибка при очистке Excel кэша при запуске: {e}")
            
            cache_cleared_on_startup = True
    elif reason == "new_replacements":
        logger.info("Очистка кэша из-за новых замен")
        clear_cache()
    elif reason == "manual":
        logger.info("Ручная очистка кэша")
        clear_cache()
        # Очищаем также кэш Excel файлов
        try:
            from excel_cache import clear_excel_cache
            clear_excel_cache()
            logger.info("Excel кэш очищен вручную")
        except Exception as e:
            logger.error(f"Ошибка при ручной очистке Excel кэша: {e}")
    
    else:
        logger.warning(f"Неизвестная причина очистки кэша: {reason}, пропускаем очистку")


def cache_student_schedule(group: str, subgroup: int, schedule_data: str):
    """Кэширование расписания студентов"""
    try:
        with cache_lock:
            cache = {}
            if os.path.exists(STUDENT_CACHE_FILE):
                with open(STUDENT_CACHE_FILE, 'r', encoding='utf-8') as f:
                    try:
                        cache = json.load(f)
                    except json.JSONDecodeError:
                        logger.error("Ошибка при чтении кэша")
                        cache = {}

            cache_key = f"{group}_{subgroup}"
            cache[cache_key] = {
                'data': schedule_data,
                'timestamp': datetime.now().isoformat()
            }

            with open(STUDENT_CACHE_FILE, 'w', encoding='utf-8') as f:
                json.dump(cache, f, ensure_ascii=False, indent=2)
            logger.info(f"Кэш успешно сохранен для группы {group}, ключ: {cache_key}")
    except Exception as e:
        logger.error(f"Ошибка при кэшировании расписания студентов: {e}")


def get_cached_student_schedule(group: str, subgroup: int) -> str:
    """Получение кэшированного расписания студентов"""
    try:
        if not os.path.exists(STUDENT_CACHE_FILE):
            logger.info(f"Файл кэша не существует: {STUDENT_CACHE_FILE}")
            return None

        with cache_lock:
            with open(STUDENT_CACHE_FILE, 'r', encoding='utf-8') as f:
                try:
                    cache = json.load(f)
                    logger.info(f"Загружен кэш: {cache}")
                except json.JSONDecodeError:
                    logger.error("Ошибка декодирования JSON в кэше")
                    return None

            cache_key = f"{group}_{subgroup}"
            logger.info(f"Поиск в кэше по ключу: {cache_key}")
            if cache_key in cache:
                cached_data = cache[cache_key]
                cached_time = datetime.fromisoformat(cached_data['timestamp'])

                # Проверяем актуальность кэша (30 минут)
                if (datetime.now() - cached_time).total_seconds() < 1800:
                    logger.info(f"Найден актуальный кэш для группы {group}")
                    return cached_data['data']
                else:
                    logger.info(f"Кэш устарел для группы {group}")
        return None
    except Exception as e:
        logger.error(f"Ошибка при получении кэшированного расписания студентов: {e}")
        return None


def cache_teacher_schedule(teacher_name: str, start_date: str, end_date: str, schedule_data: str, expiration=1800):
    """Кэширование расписания преподавателя"""
    try:
        with cache_lock:
            cache = {}
            if os.path.exists(TEACHER_CACHE_FILE):
                with open(TEACHER_CACHE_FILE, 'r', encoding='utf-8') as f:
                    try:
                        cache = json.load(f)
                    except json.JSONDecodeError:
                        logger.error("Ошибка при чтении кэша преподавателей")
                        cache = {}

            cache_key = f"{teacher_name}_{start_date}_{end_date}"
            cache[cache_key] = {
                'data': schedule_data,
                'timestamp': datetime.now().isoformat(),
                'expiration': expiration  # Store expiration time in seconds
            }

            with open(TEACHER_CACHE_FILE, 'w', encoding='utf-8') as f:
                json.dump(cache, f, ensure_ascii=False, indent=2)
            logger.info(f"Кэш успешно сохранен для преподавателя {teacher_name} с периодом действия {expiration} секунд")
    except Exception as e:
        logger.error(f"Ошибка при кэшировании расписания преподавателя: {e}")


def get_cached_teacher_schedule(teacher_name: str, start_date: str, end_date: str) -> str:
    """Получение кэшированного расписания преподавателя"""
    try:
        if not os.path.exists(TEACHER_CACHE_FILE):
            logger.info(f"Файл кэша преподавателей не существует: {TEACHER_CACHE_FILE}")
            return None

        with cache_lock:
            with open(TEACHER_CACHE_FILE, 'r', encoding='utf-8') as f:
                try:
                    cache = json.load(f)
                except json.JSONDecodeError:
                    logger.error("Ошибка декодирования JSON в кэше преподавателей")
                    return None

            cache_key = f"{teacher_name}_{start_date}_{end_date}"
            if cache_key in cache:
                cached_data = cache[cache_key]
                cached_time = datetime.fromisoformat(cached_data['timestamp'])
                
                # Get expiration time (default to 30 minutes if not specified)
                expiration = cached_data.get('expiration', 1800)

                # Проверяем актуальность кэша с учетом периода действия
                if (datetime.now() - cached_time).total_seconds() < expiration:
                    logger.info(f"Найден актуальный кэш для преподавателя {teacher_name} (осталось {expiration - (datetime.now() - cached_time).total_seconds():.0f} секунд)")
                    return cached_data['data']
                else:
                    logger.info(f"Кэш устарел для преподавателя {teacher_name}")
            return None
    except Exception as e:
        logger.error(f"Ошибка при получении кэшированного расписания преподавателя: {e}")
        return None

def get_cached_classroom_schedule(classroom, date_str):
    """
    Получает кэшированное расписание кабинета
    
    Args:
        classroom (str): Номер кабинета
        date_str (str): Дата в формате DD.MM.YYYY
        
    Returns:
        str or None: Кэшированное расписание или None, если кэша нет или он устарел
    """
    try:
        cache_key = f"{classroom}:{date_str}"
        
        if not os.path.exists(CLASSROOM_CACHE_FILE):
            return None
            
        with cache_lock:
            with open(CLASSROOM_CACHE_FILE, 'r', encoding='utf-8') as f:
                cache = json.load(f)
                
            if cache_key in cache:
                cached_data = cache[cache_key]
                # Проверяем, не устарел ли кэш (30 минут)
                cache_time = datetime.fromisoformat(cached_data['timestamp'])
                if (datetime.now() - cache_time).total_seconds() < 1800:
                    logger.info(f"Использую кэшированное расписание для кабинета {classroom} на {date_str}")
                    return cached_data['schedule']
                else:
                    logger.info(f"Кэш для кабинета {classroom} на {date_str} устарел")
        
        return None
    except Exception as e:
        logger.error(f"Ошибка при получении кэша расписания кабинета: {e}")
        return None


def cache_classroom_schedule(classroom, date_str, schedule):
    """
    Кэширует расписание кабинета
    
    Args:
        classroom (str): Номер кабинета
        date_str (str): Дата в формате DD.MM.YYYY
        schedule (str): Расписание для кэширования (строка)
    """
    try:
        cache_key = f"{classroom}:{date_str}"
        
        # Make sure cache directory exists
        if not os.path.exists(CACHE_DIR):
            os.makedirs(CACHE_DIR)
        
        with cache_lock:
            # Загружаем текущий кэш
            if os.path.exists(CLASSROOM_CACHE_FILE):
                with open(CLASSROOM_CACHE_FILE, 'r', encoding='utf-8') as f:
                    cache = json.load(f)
            else:
                cache = {}
            
            # Добавляем или обновляем запись в кэше
            cache[cache_key] = {
                'schedule': schedule,
                'timestamp': datetime.now().isoformat()
            }
            
            # Сохраняем обновленный кэш
            with open(CLASSROOM_CACHE_FILE, 'w', encoding='utf-8') as f:
                json.dump(cache, f, ensure_ascii=False, indent=2)
                
        logger.info(f"Расписание для кабинета {classroom} на {date_str} успешно кэшировано")
    except Exception as e:
        logger.error(f"Ошибка при кэшировании расписания кабинета: {e}")

def cache_data(key: str, data: Any, expiration: int = 1800):
    """
    Cache data with the given key and expiration time.
    
    Args:
        key: Cache key
        data: Data to cache (must be JSON-serializable)
        expiration: Expiration time in seconds (default 30 minutes)
    """
    try:
        init_cache()  # Ensure cache directory exists
        
        cache_file = os.path.join(CACHE_DIR, key)
        cache_entry = {
            "data": data,
            "expires_at": time.time() + expiration
        }
        
        with open(cache_file, 'w', encoding='utf-8') as f:
            json.dump(cache_entry, f, ensure_ascii=False)
        
        logger.debug(f"Cached data with key '{key}', expires in {expiration} seconds")
    except Exception as e:
        logger.error(f"Error caching data for key '{key}': {e}")

def get_cached_data(key: str) -> Optional[Any]:
    """
    Get cached data for the given key if it exists and hasn't expired.
    
    Args:
        key: Cache key
        
    Returns:
        Cached data or None if not found or expired
    """
    try:
        cache_file = os.path.join(CACHE_DIR, key)
        
        if not os.path.exists(cache_file):
            return None
        
        with open(cache_file, 'r', encoding='utf-8') as f:
            cache_entry = json.load(f)
        
        # Check if expired
        if time.time() > cache_entry.get("expires_at", 0):
            # Remove expired cache file
            try:
                os.remove(cache_file)
                logger.debug(f"Removed expired cache file for key '{key}'")
            except Exception as e:
                logger.error(f"Error removing expired cache file for key '{key}': {e}")
            return None
        
        logger.debug(f"Retrieved cached data for key '{key}'")
        return cache_entry.get("data")
    except Exception as e:
        logger.error(f"Error retrieving cached data for key '{key}': {e}")
        return None

def cache_exists(key: str) -> bool:
    """
    Check if a valid (non-expired) cache entry exists for the given key.
    
    Args:
        key: Cache key
        
    Returns:
        True if a valid cache entry exists, False otherwise
    """
    return get_cached_data(key) is not None

def delete_cache_item(key: str) -> bool:
    """
    Delete a specific cache item.
    
    Args:
        key: Cache key to delete
        
    Returns:
        True if item was deleted, False otherwise
    """
    try:
        cache_file = os.path.join(CACHE_DIR, key)
        
        if not os.path.exists(cache_file):
            return False
        
        os.remove(cache_file)
        logger.debug(f"Deleted cache item with key '{key}'")
        return True
    except Exception as e:
        logger.error(f"Error deleting cache item with key '{key}': {e}")
        return False