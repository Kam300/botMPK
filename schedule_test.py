import os
import logging
import asyncio
import traceback
from datetime import datetime, timedelta
from было import (
    parse_schedule,
    format_schedule,
    get_week_type,
    run_blocking
)

# Настройка логирования
logging.basicConfig(format='%(asctime)s - %(name)s - %(levelname)s - %(message)s', level=logging.INFO)
logger = logging.getLogger(__name__)

async def test_schedule_for_two_weeks(group_name, subgroup=None):
    """
    Временная функция для проверки расписания на две недели (четную и нечетную) без учета замен.
    
    Args:
        group_name (str): Название группы
        subgroup (int, optional): Номер подгруппы. По умолчанию None.
        
    Returns:
        str: Отформатированное расписание на две недели
    """
    try:
        logger.info(f"Проверка расписания для группы {group_name}, подгруппа {subgroup}")
        
        # Получаем текущую дату
        today = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)
        
        # Находим начало текущей недели (понедельник)
        start_of_week = today - timedelta(days=today.weekday())
        
        # Определяем тип текущей недели
        current_week_type = await run_blocking(get_week_type, start_of_week.strftime('%d.%m.%Y'))
        logger.info(f"Текущая неделя: {'четная' if current_week_type else 'нечетная'}")
        
        # Создаем список дат для проверки (две недели)
        dates_to_check = []
        
        # Добавляем даты текущей недели (кроме воскресенья)
        for i in range(6):  # Понедельник - Суббота
            date = start_of_week + timedelta(days=i)
            dates_to_check.append(date)
        
        # Добавляем даты следующей недели (кроме воскресенья)
        for i in range(6):  # Понедельник - Суббота
            date = start_of_week + timedelta(days=i+7)
            dates_to_check.append(date)
        
        # Получаем список файлов расписания
        logger.info("Получение списка файлов расписания...")
        
        # Проверяем существование директории
        download_dir = "downloaded_files"
        logger.info(f"Проверка директории: {os.path.abspath(download_dir)}")
        
        if not os.path.exists(download_dir):
            logger.error(f"Директория '{download_dir}' не существует")
            return f"Директория '{download_dir}' не существует"
        
        # Выводим содержимое директории
        all_files = os.listdir(download_dir)
        logger.info(f"Всего файлов в директории: {len(all_files)}")
        logger.info(f"Первые 5 файлов: {all_files[:5] if len(all_files) >= 5 else all_files}")
        
        # Проверяем каждое условие отдельно
        xlsx_files = [f for f in all_files if f.endswith('.xlsx')]
        logger.info(f"Файлов с расширением .xlsx: {len(xlsx_files)}")
        if xlsx_files:
            logger.info(f"Первые 5 файлов .xlsx: {xlsx_files[:5] if len(xlsx_files) >= 5 else xlsx_files}")
        
        no_dash_files = [f for f in all_files if '-' not in f]
        logger.info(f"Файлов без дефиса: {len(no_dash_files)}")
        if no_dash_files:
            logger.info(f"Первые 5 файлов без дефиса: {no_dash_files[:5] if len(no_dash_files) >= 5 else no_dash_files}")
        
        # Получаем список файлов расписания
        schedule_files = [
            f for f in all_files
            if f.endswith('.xlsx') and '-' not in f  # Исключаем файлы замен
        ]
        
        logger.info(f"Найдено {len(schedule_files)} файлов расписания")
        if schedule_files:
            logger.info(f"Первые 5 файлов расписания: {schedule_files[:5] if len(schedule_files) >= 5 else schedule_files}")
        
        # Проверяем наличие файла для указанной группы напрямую
        target_file = f"{group_name}.xlsx"
        if target_file in all_files:
            logger.info(f"Найден точный файл: {target_file}")
            group_file = os.path.join(download_dir, target_file)
        else:
            logger.info(f"Точный файл {target_file} не найден, ищем по частичному совпадению")
            
            # Находим файл для указанной группы
            group_file = None
            for file in all_files:
                if file.endswith('.xlsx') and group_name.upper() in file.upper():
                    logger.info(f"Найдено совпадение: {file}")
                    group_file = os.path.join(download_dir, file)
                    break
        
        if not group_file:
            logger.error(f"Не найден файл расписания для группы {group_name}")
            return f"Не найден файл расписания для группы {group_name}"
        
        logger.info(f"Найден файл расписания: {group_file}")
        
        # Формируем расписание для каждого дня
        all_schedules = []
        
        for date in dates_to_check:
            date_str = date.strftime('%d.%m.%Y')
            day_name = ['Понедельник', 'Вторник', 'Среда', 'Четверг', 'Пятница', 'Суббота', 'Воскресенье'][date.weekday()]
            
            # Определяем тип недели для текущей даты
            week_type = await run_blocking(get_week_type, date_str)
            week_type_str = 'четная' if week_type else 'нечетная'
            
            logger.info(f"Обработка {day_name}, {date_str} ({week_type_str} неделя)")
            
            try:
                # Получаем расписание на день без учета замен
                logger.info(f"Вызов parse_schedule для {date_str}...")
                schedule = await run_blocking(parse_schedule, group_file, date_str, subgroup)
                
                logger.info(f"Получено расписание для {date_str}: {schedule}")
                
                # Форматируем расписание
                logger.info(f"Форматирование расписания для {date_str}...")
                formatted_schedule = await run_blocking(
                    format_schedule, 
                    schedule, 
                    group_name, 
                    date_str, 
                    subgroup
                )
                
                # Добавляем информацию о дне и типе недели
                day_header = f"\n📅 {day_name}, {date_str} ({week_type_str} неделя):\n"
                all_schedules.append(day_header + formatted_schedule)
                
            except Exception as e:
                logger.error(f"Ошибка при обработке расписания на {date_str}: {e}")
                logger.error(traceback.format_exc())
                all_schedules.append(f"\n📅 {day_name}, {date_str} ({week_type_str} неделя):\nОшибка при получении расписания: {str(e)}")
        
        # Объединяем все расписания
        full_schedule = "\n".join(all_schedules)
        
        # Добавляем заголовок
        header = f"📚 Расписание для группы {group_name}"
        if subgroup:
            header += f", подгруппа {subgroup}"
        header += " на две недели (без учета замен):\n"
        
        return header + full_schedule
    
    except Exception as e:
        logger.error(f"Общая ошибка при проверке расписания: {e}")
        logger.error(traceback.format_exc())
        return f"Произошла ошибка при проверке расписания: {str(e)}"

async def main():
    """Основная функция для запуска теста расписания"""
    # Укажите здесь группу и подгруппу для проверки
    group_name = "ИСпВ-24-1"  # Замените на нужную группу
    subgroup = 1  # Укажите подгруппу или None для всей группы
    
    print("Начинаем проверку расписания...")
    try:
        result = await test_schedule_for_two_weeks(group_name, subgroup)
        print(result)
    except Exception as e:
        print(f"Ошибка при выполнении теста: {e}")
        print(traceback.format_exc())
    print("\nПроверка завершена!")

if __name__ == "__main__":
    asyncio.run(main()) 