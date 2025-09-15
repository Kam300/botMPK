import os
import logging
import dropbox
from dropbox import DropboxOAuth2FlowNoRedirect
import schedule
import time
from datetime import datetime, timedelta
import requests
from bs4 import BeautifulSoup
import json
import threading
import traceback
import re

# Настройка логирования
logging.basicConfig(format='%(asctime)s - %(name)s - %(levelname)s - %(message)s', level=logging.INFO)
logger = logging.getLogger(__name__)

# API токены
APP_KEY = "x9ts10os3wo4pfi"
APP_SECRET = "pfy9sz6333yo4f5"
REFRESH_TOKEN_FILE = "refresh_token.txt"
ACCESS_TOKEN_FILE = "access_token.txt"

# Папка для хранения загруженных файлов
DOWNLOADS_DIR = "downloaded_files"
SUBSCRIBERS_FILE = "subscribers.json"

# Файл для хранения времени последнего обновления расписаний групп
LAST_SCHEDULE_UPDATE_FILE = "last_schedule_update.txt"

# Файл-маркер, указывающий, что первый запуск уже был выполнен
FIRST_RUN_MARKER_FILE = "schedule_first_run_completed.txt"

# URLs для расписаний групп
GROUP_SCHEDULE_URLS = [
    "https://newlms.magtu.ru/mod/folder/view.php?id=1584679",
    "https://newlms.magtu.ru/mod/folder/view.php?id=1584691",
    "https://newlms.magtu.ru/mod/folder/view.php?id=1584686",
    "https://newlms.magtu.ru/mod/folder/view.php?id=1584687"
]

# URL для замен
REPLACEMENTS_URL = "https://newlms.magtu.ru/mod/folder/view.php?id=219250"

# Минимальный интервал для обновления расписаний групп в часах
SCHEDULE_UPDATE_INTERVAL_HOURS = 96

# Файл-флаг, указывающий, что идет процесс обновления
UPDATE_IN_PROGRESS_FILE = "update_in_progress.flag"

# Максимальное время выполнения операции синхронизации в секундах
SYNC_TIMEOUT_SECONDS = 300  # 5 минут

# Текст сообщения для пользователей во время обновления
UPDATE_IN_PROGRESS_MESSAGE = """⚠️ *Внимание!* 

В данный момент идет обновление расписаний и замен. 
Подождите несколько минут и повторите запрос.

Обновление обычно занимает 1-2 минуты."""

# Глобальный таймер для автоматического сброса флага обновления
update_timer = None
update_lock = threading.Lock()


class DropboxTokenManager:
    def __init__(self, app_key, app_secret):
        self.app_key = app_key
        self.app_secret = app_secret
        self.access_token = None
        self.refresh_token = None
        self.load_tokens()

    def load_tokens(self):
        """Загружает токены из файлов"""
        try:
            if os.path.exists(ACCESS_TOKEN_FILE):
                with open(ACCESS_TOKEN_FILE, 'r') as f:
                    self.access_token = f.read().strip()
            if os.path.exists(REFRESH_TOKEN_FILE):
                with open(REFRESH_TOKEN_FILE, 'r') as f:
                    self.refresh_token = f.read().strip()
        except Exception as e:
            logger.error(f"Ошибка при загрузке токенов: {e}")

    def save_tokens(self):
        """Сохраняет токены в файлы"""
        try:
            if self.access_token:
                with open(ACCESS_TOKEN_FILE, 'w') as f:
                    f.write(self.access_token)
            if self.refresh_token:
                with open(REFRESH_TOKEN_FILE, 'w') as f:
                    f.write(self.refresh_token)
        except Exception as e:
            logger.error(f"Ошибка при сохранении токенов: {e}")

    def refresh_access_token(self):
        """Обновляет access token используя refresh token"""
        try:
            if not self.refresh_token:
                raise Exception("Refresh token не найден")

            url = "https://api.dropboxapi.com/oauth2/token"
            data = {
                "grant_type": "refresh_token",
                "refresh_token": self.refresh_token,
                "client_id": self.app_key,
                "client_secret": self.app_secret
            }

            response = requests.post(url, data=data)
            if response.status_code == 200:
                result = response.json()
                self.access_token = result.get("access_token")
                self.save_tokens()
                logger.info("Access token успешно обновлен")
                return True
            else:
                logger.error(f"Ошибка при обновлении токена: {response.text}")
                return False
        except Exception as e:
            logger.error(f"Ошибка при обновлении токена: {e}")
            return False

    def get_valid_access_token(self):
        """Возвращает действительный access token"""
        try:
            # Пробуем использовать текущий токен
            if self.access_token:
                dbx = dropbox.Dropbox(self.access_token)
                try:
                    dbx.users_get_current_account()
                    return self.access_token
                except dropbox.exceptions.AuthError:
                    logger.info("Текущий access token истек")

            # Если токен истек или отсутствует, пробуем обновить
            if self.refresh_access_token():
                return self.access_token

            # Если не удалось обновить, инициируем новую авторизацию
            auth_flow = DropboxOAuth2FlowNoRedirect(
                self.app_key,
                self.app_secret,
                token_access_type='offline'
            )

            authorize_url = auth_flow.start()
            print(f"1. Перейдите по ссылке: {authorize_url}")
            print("2. Нажмите 'Allow' (разрешить)")
            print("3. Скопируйте код авторизации")

            auth_code = input("Введите код авторизации: ").strip()
            oauth_result = auth_flow.finish(auth_code)

            self.access_token = oauth_result.access_token58
            self.refresh_token = oauth_result.refresh_token
            self.save_tokens()

            return self.access_token

        except Exception as e:
            logger.error(f"Ошибка при получении валидного токена: {e}")
            return None


def get_dropbox_client():
    """Создает и возвращает клиент Dropbox с автоматическим обновлением токена"""
    try:
        token_manager = DropboxTokenManager(APP_KEY, APP_SECRET)
        access_token = token_manager.get_valid_access_token()

        if not access_token:
            logger.error("Не удалось получить валидный access token")
            return None

        dbx = dropbox.Dropbox(
            access_token,
            app_key=APP_KEY,
            app_secret=APP_SECRET,
            oauth2_refresh_token=token_manager.refresh_token
        )

        logger.info("Dropbox клиент успешно инициализирован")
        return dbx

    except Exception as e:
        logger.error(f"Ошибка при инициализации Dropbox клиента: {e}")
        return None


def notify_subscribers(new_files):
    """Отправляет уведомления подписчикам о новых файлах замен"""
    try:
        subscribers = {}
        
        # Загружаем список подписчиков
        if os.path.exists(SUBSCRIBERS_FILE):
            with open(SUBSCRIBERS_FILE, 'r', encoding='utf-8') as f:
                subscribers = json.load(f)
                
        if not subscribers:
            logger.info("Нет подписчиков для уведомления")
            return
            
        # Формируем сообщение о новых заменах
        message = "🔔 *Обнаружены новые замены в расписании!*\n\n"
        for filename in new_files:
            # Проверяем, является ли это файл диапазона дат или одной даты
            if '-' in filename and filename.endswith('.xlsx'):
                dates = filename.replace('.xlsx', '').split('-')
                if len(dates) == 2:
                    try:
                        start_date = datetime.strptime(dates[0], '%d.%m.%y').strftime('%d.%m.%Y')
                        end_date = datetime.strptime(dates[1], '%d.%m.%y').strftime('%d.%m.%Y')
                        message += f"• Замены на период: {start_date} - {end_date}\n"
                    except ValueError:
                        try:
                            start_date = datetime.strptime(dates[0], '%d.%m.%Y').strftime('%d.%m.%Y')
                            end_date = datetime.strptime(dates[1], '%d.%m.%Y').strftime('%d.%m.%Y')
                            message += f"• Замены на период: {start_date} - {end_date}\n"
                        except ValueError:
                            message += f"• {filename}\n"
            elif filename.endswith('.xlsx'):
                # Проверяем, является ли это файл с одной датой
                date_pattern = re.compile(r'^(\d{2}\.\d{2}\.\d{2,4})\.xlsx$')
                match = date_pattern.match(filename)
                if match:
                    date_str = match.group(1)
                    try:
                        formatted_date = datetime.strptime(date_str, '%d.%m.%y').strftime('%d.%m.%Y')
                        message += f"• Замены на дату: {formatted_date}\n"
                    except ValueError:
                        try:
                            formatted_date = datetime.strptime(date_str, '%d.%m.%Y').strftime('%d.%m.%Y')
                            message += f"• Замены на дату: {formatted_date}\n"
                        except ValueError:
                            message += f"• {filename}\n"
                else:
                    # Если это расписание группы, а не замены
                    if not is_replacement_file(filename):
                        message += f"• Обновлено расписание группы: {filename}\n"
                    else:
                        message += f"• {filename}\n"
        
        message += "\nБудут доступны через 2 минуты"
        
        # Save notification to both files to ensure delivery
        notification_data = {
            "message": message,
            "chat_ids": list(subscribers.keys()),
            "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        }
        
        # Write to both notification files
        with open("new_replacements_notify.json", "w", encoding='utf-8') as f:
            json.dump(notification_data, f, ensure_ascii=False)
        
        with open("pending_notifications.json", "w", encoding='utf-8') as f:
            json.dump(notification_data, f, ensure_ascii=False)
            
        logger.info(f"Подготовлено уведомление для {len(subscribers)} подписчиков")
        
    except Exception as e:
        logger.error(f"Ошибка при подготовке уведомлений: {str(e)}")
        logger.error(traceback.format_exc())


def get_last_schedule_update():
    """Получает время последнего обновления расписаний групп"""
    try:
        if os.path.exists(LAST_SCHEDULE_UPDATE_FILE):
            with open(LAST_SCHEDULE_UPDATE_FILE, 'r') as f:
                timestamp_str = f.read().strip()
                return datetime.fromisoformat(timestamp_str)
        return None
    except Exception as e:
        logger.error(f"Ошибка при получении времени последнего обновления: {e}")
        return None

def set_last_schedule_update():
    """Сохраняет текущее время как время последнего обновления расписаний групп"""
    try:
        with open(LAST_SCHEDULE_UPDATE_FILE, 'w') as f:
            now = datetime.now()
            f.write(now.isoformat())
        logger.info(f"Обновлено время последнего обновления расписаний: {datetime.now()}")
    except Exception as e:
        logger.error(f"Ошибка при сохранении времени обновления: {e}")

def is_first_run():
    """Проверяет, является ли текущий запуск первым"""
    return not os.path.exists(FIRST_RUN_MARKER_FILE)

def mark_first_run_completed():
    """Отмечает, что первый запуск завершен"""
    try:
        with open(FIRST_RUN_MARKER_FILE, 'w') as f:
            f.write(datetime.now().isoformat())
        logger.info("Первый запуск обновления расписаний завершен")
    except Exception as e:
        logger.error(f"Ошибка при создании маркера первого запуска: {e}")

def should_update_schedules(new_replacement_files):
    """Проверяет, нужно ли обновлять расписания групп"""
    # Если найдены новые файлы замен, обновляем расписания
    if new_replacement_files:
        logger.info("Обнаружены новые замены - будет выполнено обновление расписаний групп")
        return True
    
    # Если это первый запуск - обязательно обновляем
    first_run = is_first_run()
    if first_run:
        logger.info("Это первый запуск - будет выполнено полное обновление расписаний групп")
        return True
    
    # Проверяем, когда было последнее обновление
    last_update = get_last_schedule_update()
    
    # Если файл журнала не найден, но маркер первого запуска есть - 
    # это странная ситуация, лучше обновить на всякий случай
    if last_update is None:
        logger.info("Не найдено время последнего обновления - будет выполнено обновление расписаний групп")
        return True
    
    # Для последующих запусков можно использовать другие правила:
    # Обновляем только если прошло определенное время (только для первого запуска 24 часа)
    # Для последующих запусков - обновляем только при появлении новых замен
    if not first_run:
        logger.info("Не первый запуск - обновление только при новых заменах")
        return False
    
    # Для первого запуска проверяем интервал времени
    hours_since_update = (datetime.now() - last_update).total_seconds() / 3600
    if hours_since_update >= SCHEDULE_UPDATE_INTERVAL_HOURS:
        logger.info(f"Прошло {hours_since_update:.1f} часов с последнего обновления - будет выполнено обновление расписаний групп")
        return True
    
    logger.info(f"Обновление расписаний пропущено. Прошло {hours_since_update:.1f} часов с последнего обновления (минимальный интервал: {SCHEDULE_UPDATE_INTERVAL_HOURS} часов)")
    return False

def set_update_in_progress(in_progress=True):
    """Устанавливает или снимает флаг процесса обновления"""
    global update_timer
    
    try:
        with update_lock:
            if in_progress:
                # Создаем файл-флаг
                with open(UPDATE_IN_PROGRESS_FILE, 'w') as f:
                    f.write(datetime.now().isoformat())
                logger.info("Установлен флаг обновления в процессе")
                
                # Устанавливаем таймер для автоматического сброса флага
                if update_timer is not None:
                    update_timer.cancel()
                    
                update_timer = threading.Timer(SYNC_TIMEOUT_SECONDS, lambda: set_update_in_progress(False))
                update_timer.daemon = True
                update_timer.start()
                logger.info(f"Установлен таймер на {SYNC_TIMEOUT_SECONDS} секунд для автоматического сброса флага обновления")
            else:
                # Удаляем файл-флаг, если он существует
                if os.path.exists(UPDATE_IN_PROGRESS_FILE):
                    os.remove(UPDATE_IN_PROGRESS_FILE)
                    logger.info("Снят флаг обновления в процессе")
                
                # Отменяем таймер, если он был установлен
                if update_timer is not None:
                    update_timer.cancel()
                    update_timer = None
    except Exception as e:
        logger.error(f"Ошибка при работе с флагом обновления: {e}")
        # Всегда пытаемся удалить флаг в случае ошибки
        try:
            if os.path.exists(UPDATE_IN_PROGRESS_FILE):
                os.remove(UPDATE_IN_PROGRESS_FILE)
        except:
            pass

def is_update_in_progress():
    """Проверяет, идет ли процесс обновления"""
    try:
        # Проверяем наличие файла-флага
        if os.path.exists(UPDATE_IN_PROGRESS_FILE):
            # Дополнительно можно проверить время создания флага
            # и сбросить его автоматически, если прошло слишком много времени
            with open(UPDATE_IN_PROGRESS_FILE, 'r') as f:
                timestamp_str = f.read().strip()
                start_time = datetime.fromisoformat(timestamp_str)
                # Если прошло более 5 минут, считаем, что обновление зависло
                if (datetime.now() - start_time).total_seconds() > SYNC_TIMEOUT_SECONDS:
                    set_update_in_progress(False)
                    logger.warning(f"Автоматический сброс зависшего флага обновления (прошло более {SYNC_TIMEOUT_SECONDS} секунд)")
                    return False
            return True
        return False
    except Exception as e:
        logger.error(f"Ошибка при проверке статуса обновления: {e}")
        # В случае ошибки снимаем флаг на всякий случай
        try:
            if os.path.exists(UPDATE_IN_PROGRESS_FILE):
                os.remove(UPDATE_IN_PROGRESS_FILE)
        except:
            pass
        return False

def get_update_status_message():
    """Возвращает сообщение о статусе обновления для пользователей"""
    if is_update_in_progress():
        return UPDATE_IN_PROGRESS_MESSAGE
    return None

# Функция для асинхронного запуска синхронизации файлов в отдельном потоке
def sync_files_async(force_check=False):
    """
    Запускает процесс синхронизации файлов в отдельном потоке,
    чтобы не блокировать основной поток бота
    """
    # Проверяем, не идет ли уже процесс обновления
    if is_update_in_progress():
        logger.info("Синхронизация файлов уже выполняется, пропускаем запрос")
        return
        
    # Создаем функцию для выполнения в отдельном потоке
    def run_sync():
        try:
            # Устанавливаем флаг, что идет обновление
            set_update_in_progress(True)
            
            # Получаем Dropbox клиент
            dbx = get_dropbox_client()
            if not dbx:
                logger.error("Не удалось получить Dropbox клиент")
                set_update_in_progress(False)
                return

            # Выполняем синхронизацию
            try:
                new_files = sync_files(dbx, force_check)
                if new_files:
                    logger.info(f"Синхронизация успешно завершена, обнаружены новые файлы: {new_files}")
                else:
                    logger.info("Синхронизация успешно завершена, новых файлов не обнаружено")
            except Exception as e:
                logger.error(f"Ошибка при синхронизации файлов: {e}")
                logger.error(traceback.format_exc())
            
            # Снимаем флаг обновления
            set_update_in_progress(False)
            
        except Exception as e:
            logger.error(f"Критическая ошибка в потоке синхронизации: {e}")
            logger.error(traceback.format_exc())
            # Обязательно снимаем флаг даже при ошибке
            set_update_in_progress(False)
    
    # Запускаем синхронизацию в отдельном потоке
    sync_thread = threading.Thread(target=run_sync)
    sync_thread.daemon = True  # Поток завершится вместе с основным
    sync_thread.start()
    logger.info("Запущен отдельный поток для синхронизации файлов")

def sync_files(dbx=None, force_check=False):
    """Синхронизирует файлы замен и расписания групп с LMSMGTU"""
    try:
        logger.info("Начало проверки файлов замен и расписаний групп")

        # Create downloads directory if it doesn't exist
        if not os.path.exists(DOWNLOADS_DIR):
            os.makedirs(DOWNLOADS_DIR)

        # Get Dropbox client if not provided
        if dbx is None:
            dbx = get_dropbox_client()
            if not dbx:
                logger.error("Failed to get Dropbox client")
                return []

        new_files = []

        # Синхронизация файлов замен
        new_replacement_files = sync_replacements(dbx, force_check)
        if new_replacement_files:
            new_files.extend(new_replacement_files)

        # Проверяем, нужно ли обновлять расписания групп
        first_run = is_first_run()
        if force_check or should_update_schedules(new_replacement_files):
            # Синхронизация файлов расписаний групп
            new_schedule_files = sync_group_schedules(dbx, force_check)
            if new_schedule_files:
                new_files.extend(new_schedule_files)
            
            # Обновляем время последнего обновления расписаний
            set_last_schedule_update()
            
            # Если это был первый запуск, отмечаем его как завершенный
            if first_run:
                mark_first_run_completed()
        else:
            logger.info("Пропуск обновления расписаний групп - недавно уже обновлялись или нет новых замен")

        # Если обнаружены новые файлы, отправляем уведомления
        if new_files:
            # Очищаем кэш после обновления с указанием причины
            try:
                from cache_utils import selective_cache_clear
                selective_cache_clear(reason="new_replacements")
                logger.info("Кэш успешно очищен после обновления файлов")
            except Exception as e:
                logger.error(f"Ошибка при очистке кэша: {e}")
                
            # Отправляем уведомления подписчикам о новых файлах
            notify_subscribers(new_files)
        else:
            logger.info("Новых файлов не обнаружено")

        return new_files

    except Exception as e:
        logger.error(f"Ошибка при синхронизации: {str(e)}")
        logger.error(traceback.format_exc())
        return []

def sync_replacements(dbx, force_check=False):
    """Синхронизирует только файлы замен с LMSMGTU"""
    try:
        logger.info("Синхронизация файлов замен")

        # Получаем только файлы замен (теперь проверяем через is_replacement_file)
        current_files = [f for f in os.listdir(DOWNLOADS_DIR) 
                       if f.endswith('.xlsx') and is_replacement_file(f)]

        # Получаем список новых файлов с сайта
        response = requests.get(REPLACEMENTS_URL)
        response.raise_for_status()
        soup = BeautifulSoup(response.text, 'html.parser')
        links = soup.find_all('a')

        # Получаем информацию о файлах замен
        replacement_files = []
        for link in links:
            href = link.get('href')
            filename = link.text.strip()
            # Проверяем формат имени файла замен
            if (href and ('.xlsx' in href or '.xls' in href)):
                if not filename.endswith('.xlsx'):
                    filename += '.xlsx'
                try:
                    # Проверяем, что это действительно файл замен с датами
                    if is_replacement_file(filename):
                        replacement_files.append((href, filename))
                except Exception as e:
                    logger.warning(f"Ошибка при проверке формата даты для файла {filename}: {e}")
                    continue

        # Сортируем по дате в имени файла (пробуем оба формата даты)
        def get_sort_date(filename):
            try:
                date_part = filename[1]
                # Проверяем, является ли это файлом с диапазоном дат или одной датой
                if '-' in date_part:
                    date_str = date_part.split('-')[0]
                else:
                    date_str = date_part.replace('.xlsx', '')
                    
                try:
                    return datetime.strptime(date_str, '%d.%m.%y')
                except ValueError:
                    return datetime.strptime(date_str, '%d.%m.%Y')
            except Exception:
                # В случае ошибки возвращаем минимальную дату
                return datetime.min
                
        replacement_files.sort(
            key=get_sort_date,
            reverse=True
        )

        # Берем только два последних файла
        latest_files = replacement_files[:2]
        latest_filenames = [f[1] for f in latest_files]

        # Проверяем наличие новых файлов
        has_new_files = False
        new_files = []
        for _, filename in latest_files:
            if filename not in current_files:
                has_new_files = True
                new_files.append(filename)

        if has_new_files or force_check:
            logger.info("Обнаружены новые файлы замен")

            # Удаляем старые файлы замен
            for old_file in current_files:
                try:
                    old_path = os.path.join(DOWNLOADS_DIR, old_file)
                    if os.path.exists(old_path):
                        os.remove(old_path)
                        logger.info(f"Удален старый файл замен: {old_file}")

                    # Удаляем файл из Dropbox
                    if dbx:
                        try:
                            dbx.files_delete_v2(f"/{old_file}")
                            logger.info(f"Удален старый файл замен из Dropbox: {old_file}")
                        except Exception as e:
                            logger.error(f"Ошибка при удалении файла из Dropbox: {e}")

                except Exception as e:
                    logger.error(f"Ошибка при удалении старого файла {old_file}: {e}")

            # Загружаем новые файлы
            for href, filename in latest_files:
                try:
                    # Загружаем файл
                    file_response = requests.get(href)
                    file_response.raise_for_status()

                    # Сохраняем локально
                    local_path = os.path.join(DOWNLOADS_DIR, filename)
                    with open(local_path, 'wb') as f:
                        f.write(file_response.content)
                    logger.info(f"Загружен новый файл замен: {filename}")

                    # Загружаем в Dropbox
                    if dbx:
                        with open(local_path, 'rb') as f:
                            dbx.files_upload(f.read(), f"/{filename}", mode=dropbox.files.WriteMode.overwrite)
                        logger.info(f"Загружен новый файл замен в Dropbox: {filename}")

                except Exception as e:
                    logger.error(f"Ошибка при загрузке нового файла {filename}: {e}")
            
            return new_files
        else:
            logger.info("Новых файлов замен не обнаружено")
            return []

    except Exception as e:
        logger.error(f"Ошибка при проверке замен: {str(e)}")
        return []

def sync_group_schedules(dbx, force_check=False):
    """Синхронизирует расписания групп с LMSMGTU"""
    try:
        logger.info("Синхронизация расписаний групп")
        
        # Получаем список файлов, которые НЕ являются файлами замен
        current_files = [f for f in os.listdir(DOWNLOADS_DIR) 
                       if f.endswith('.xlsx') and not is_replacement_file(f)]
        
        # Удаляем все существующие файлы расписаний групп
        for old_file in current_files:
            try:
                old_path = os.path.join(DOWNLOADS_DIR, old_file)
                if os.path.exists(old_path):
                    os.remove(old_path)
                    logger.info(f"Удален старый файл расписания: {old_file}")

                # Удаляем файл из Dropbox
                if dbx:
                    try:
                        dbx.files_delete_v2(f"/{old_file}")
                        logger.info(f"Удален старый файл расписания из Dropbox: {old_file}")
                    except Exception as e:
                        logger.error(f"Ошибка при удалении файла из Dropbox: {e}")
            except Exception as e:
                logger.error(f"Ошибка при удалении старого файла {old_file}: {e}")
        
        downloaded_files = []
        
        # Обрабатываем каждый URL с расписаниями групп
        for url in GROUP_SCHEDULE_URLS:
            try:
                logger.info(f"Обработка URL: {url}")
                
                # Получаем список файлов с сайта
                response = requests.get(url)
                response.raise_for_status()
                soup = BeautifulSoup(response.text, 'html.parser')
                links = soup.find_all('a')
                
                # Обрабатываем каждую ссылку
                for link in links:
                    href = link.get('href')
                    filename = link.text.strip()
                    
                    # Проверяем, что это Excel-файл
                    if href and ('.xlsx' in href or '.xls' in href):
                        if not filename.endswith('.xlsx'):
                            filename += '.xlsx'
                        
                        try:
                            # Загружаем файл
                            file_response = requests.get(href)
                            file_response.raise_for_status()
                            
                            # Сохраняем локально
                            local_path = os.path.join(DOWNLOADS_DIR, filename)
                            with open(local_path, 'wb') as f:
                                f.write(file_response.content)
                            logger.info(f"Загружено расписание группы: {filename}")
                            
                            # Загружаем в Dropbox
                            if dbx:
                                with open(local_path, 'rb') as f:
                                    dbx.files_upload(f.read(), f"/{filename}", mode=dropbox.files.WriteMode.overwrite)
                                logger.info(f"Загружено расписание группы в Dropbox: {filename}")
                            
                            downloaded_files.append(filename)
                            
                        except Exception as e:
                            logger.error(f"Ошибка при загрузке файла {filename}: {e}")
            
            except Exception as e:
                logger.error(f"Ошибка при обработке URL {url}: {e}")
        
        logger.info(f"Загружено расписаний групп: {len(downloaded_files)}")
        
        # Для уведомлений вернем только новые файлы, которых не было раньше
        new_files = [f for f in downloaded_files if f not in current_files]
        if new_files:
            logger.info(f"Обнаружены новые расписания групп: {len(new_files)}")
        else:
            logger.info("Новых расписаний групп не обнаружено")
        
        return new_files
        
    except Exception as e:
        logger.error(f"Ошибка при синхронизации расписаний групп: {e}")
        return []

# Вспомогательная функция для проверки, является ли файл файлом замен
def is_replacement_file(filename):
    """Проверяет, является ли файл файлом замен (имеет формат даты в имени)."""
    if not filename.endswith('.xlsx'):
        return False
    
    try:
        # Проверяем формат файла с диапазоном дат (DD.MM.YY-DD.MM.YY.xlsx)
        if '-' in filename:
            date_parts = filename.replace('.xlsx', '').split('-')
            if len(date_parts) != 2:
                return False
            
            # Проверяем формат первой даты
            start_date_str = date_parts[0]
            parts = start_date_str.split('.')
            
            # Должно быть три части (день, месяц, год)
            if len(parts) != 3:
                return False
                
            # Проверяем, что части можно преобразовать в числа
            try:
                day = int(parts[0])
                month = int(parts[1])
                year = int(parts[2])
                
                # Простая проверка на валидность даты
                if not (1 <= day <= 31 and 1 <= month <= 12):
                    return False
                    
                return True
            except ValueError:
                return False
        else:
            # Проверяем формат файла с одной датой (DD.MM.YY.xlsx)
            date_pattern = re.compile(r'^(\d{2}\.\d{2}\.\d{2,4})\.xlsx$')
            match = date_pattern.match(filename)
            if not match:
                return False
                
            date_str = match.group(1)
            parts = date_str.split('.')
            
            # Должно быть три части (день, месяц, год)
            if len(parts) != 3:
                return False
                
            # Проверяем, что части можно преобразовать в числа
            try:
                day = int(parts[0])
                month = int(parts[1])
                year = int(parts[2])
                
                # Простая проверка на валидность даты
                if not (1 <= day <= 31 and 1 <= month <= 12):
                    return False
                    
                return True
            except ValueError:
                return False
    except Exception:
        return False

def schedule_sync():
    """Настраивает расписание синхронизации"""
    logger.info("=== Запуск планировщика синхронизации замен ===")

    def scheduled_sync():
        # Используем асинхронную версию для предотвращения блокировки
        sync_files_async()

    # Запускаем синхронизацию каждые 5 минут
    schedule.every(5).minutes.do(scheduled_sync)

    # Запускаем синхронизацию при старте
    scheduled_sync()

    # Запускаем синхронизацию в фоновом режиме
    while True:
        schedule.run_pending()
        time.sleep(1)


if __name__ == "__main__":
    schedule_sync()