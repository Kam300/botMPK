"""
Патч для улучшения фильтрации дат в расписании преподавателей.
Этот код должен заменить функцию get_teacher_schedule_with_index в файле teacher_schedule_processor.py
"""

"""
async def get_teacher_schedule_with_index(teacher_name: str, start_date: str, end_date: str) -> str:
    """Использует индекс для оптимизации поиска расписания преподавателя"""
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
        
        # Получаем все файлы с заменами
        results = []
        files_dir = "downloaded_files"
        for file in os.listdir(files_dir):
            if file.endswith(".xlsx") and '-' in file:
                try:
                    dates = file.replace('.xlsx', '').split('-')
                    if len(dates) == 2:
                        # Пробуем разные форматы даты
                        try:
                            # Сначала пробуем формат с двузначным годом
                            start_file_date = datetime.strptime(dates[0], '%d.%m.%y').date()
                            end_file_date = datetime.strptime(dates[1], '%d.%m.%y').date()
                        except ValueError:
                            try:
                                # Затем пробуем формат с четырехзначным годом
                                start_file_date = datetime.strptime(dates[0], '%d.%m.%Y').date()
                                end_file_date = datetime.strptime(dates[1], '%d.%m.%Y').date()
                            except ValueError:
                                logger.warning(f"Не удалось распознать формат даты в файле: {file}")
                                continue
                        
                        results.append((start_file_date, end_file_date, file))
                except Exception as e:
                    logger.error(f"Ошибка при обработке файла {file}: {str(e)}")
                    continue

        # Находим самую раннюю и самую позднюю даты для определения диапазона
        latest_end_date = None
        earliest_start_date = None

        for result in results:
            if result:
                start_file_date, end_file_date, file_name = result
                if not latest_end_date or end_file_date > latest_end_date:
                    latest_end_date = end_file_date
                if not earliest_start_date or start_file_date < earliest_start_date:
                    earliest_start_date = start_file_date

        # Сравниваем с запрошенным диапазоном
        earliest_start_date = max(start_date_obj, earliest_start_date) if earliest_start_date else start_date_obj
        latest_end_date = min(end_date_obj, latest_end_date) if latest_end_date else end_date_obj
        
        # Собираем только даты, которые действительно относятся к файлам с заменами
        dates_to_check = set()
        # Перебираем все возможные даты между earliest_start_date и latest_end_date
        if earliest_start_date and latest_end_date:
            current_date = earliest_start_date
            while current_date <= latest_end_date:
                if current_date.weekday() != 6:  # Пропускаем воскресенье
                    # Проверяем, входит ли текущая дата в диапазон хотя бы одного файла с заменами
                    is_date_in_replacement = False
                    for start_file_date, end_file_date, _ in results:
                        if start_file_date <= current_date <= end_file_date:
                            is_date_in_replacement = True
                            break
                    
                    # Добавляем дату в список только если она входит в диапазон замен
                    if is_date_in_replacement:
                        dates_to_check.add(current_date.strftime('%d.%m.%Y'))
                
                current_date += timedelta(days=1)

        logger.info(f"Даты для проверки: {sorted(dates_to_check)}")
        
        # Если нет дат для проверки, возвращаем сообщение
        if not dates_to_check:
            return f"Расписание для {teacher_name} на указанный период не найдено (нет актуальных файлов замен)"
        
        # Получаем все файлы, которые содержат упоминание преподавателя
        teacher_files = set()
        with schedule_index_lock:
            if schedule_index_initialized and teacher_name in schedule_index["teachers"]:
                teacher_files = set(schedule_index["teachers"][teacher_name])
        
        # Собираем все релевантные файлы
        all_relevant_files = teacher_files.union([os.path.join(files_dir, file) for _, _, file in results])
        
        if not all_relevant_files:
            logger.warning(f"Не найдено релевантных файлов для {teacher_name} на период {start_date} - {end_date}")
            return f"Расписание для {teacher_name} на указанный период не найдено"
            
        # Логирование найденных файлов
        logger.info(f"Для {teacher_name} найдено {len(teacher_files)} файлов с упоминанием преподавателя")
        logger.info(f"Найдено {len(results)} файлов замен для дат в диапазоне {start_date} - {end_date}")
        
        # Устанавливаем общий таймаут на всю операцию
        overall_start_time = time.time()
        overall_timeout = 30  # 30 секунд на весь процесс
        
        # Ограничиваем количество одновременно обрабатываемых файлов
        MAX_CONCURRENT_TASKS = 10
        dates_processed = []
        
        # Обрабатываем только даты в диапазоне, отфильтрованные выше
        for date_str in sorted(dates_to_check):
            if time.time() - overall_start_time >= overall_timeout:
                break
                
            dates_processed.append(date_str)
            # Обрабатываем файлы для этой даты
            date_schedule = {}
            
            # Создаем задачи для обработки файлов
            files_to_process = []
            
            for file_path in all_relevant_files:
                if os.path.exists(file_path):
                    # Проверяем, применим ли файл к текущей дате
                    file_name = os.path.basename(file_path)
                    if is_file_applicable_for_date(file_name, date_str) or not '-' in file_name:
                        files_to_process.append(file_path)
                else:
                    logger.warning(f"Файл не найден: {file_path}")
            
            # Обрабатываем файлы партиями, чтобы избежать перегрузки
            for i in range(0, len(files_to_process), MAX_CONCURRENT_TASKS):
                if time.time() - overall_start_time >= overall_timeout:
                    break
                    
                batch = files_to_process[i:i+MAX_CONCURRENT_TASKS]
                batch_tasks = [process_excel_file_for_teacher(file_path, date_str, teacher_name) 
                               for file_path in batch]
                
                if batch_tasks:
                    logger.info(f"Обрабатываем партию из {len(batch_tasks)} файлов для {teacher_name} на дату {date_str}")
                    try:
                        # Добавляем таймаут на уровне партии
                        results = await asyncio.wait_for(
                            asyncio.gather(*batch_tasks, return_exceptions=True),
                            timeout=20  # 20 секунд на партию файлов
                        )
                        
                        # Объединяем результаты, игнорируя ошибки
                        for result in results:
                            if isinstance(result, Exception):
                                logger.error(f"Ошибка при обработке файла: {result}")
                            elif result:
                                date_schedule.update(result)
                    except asyncio.TimeoutError:
                        logger.warning(f"Превышен таймаут обработки партии файлов для {date_str}")
                    except Exception as e:
                        logger.error(f"Ошибка при выполнении задач для даты {date_str}: {e}")
            
            # Сохраняем результаты для этой даты
            if date_schedule:
                all_schedules[date_str] = date_schedule
                logger.info(f"Для даты {date_str} найдено {len(date_schedule)} пар")
            else:
                logger.info(f"Для даты {date_str} не найдено пар")
        
        # Проверяем, не истек ли общий таймаут
        if time.time() - overall_start_time >= overall_timeout:
            logger.warning(f"Превышен общий таймаут обработки расписания для {teacher_name}")
            # Если есть хотя бы какие-то данные, продолжаем обработку
            if not all_schedules:
                return f"Не удалось получить расписание для {teacher_name} в указанный срок. Пожалуйста, попробуйте еще раз."
        
        # Если мы не смогли найти ни одной пары для всех дат
        if not all_schedules:
            logger.warning(f"Расписание не найдено для {teacher_name} на {start_date}-{end_date} (обработано дат: {len(dates_processed)})")
            return f"Расписание для {teacher_name} на указанный период не найдено"
        
        # Форматируем результат
        logger.info(f"Форматирование расписания для {teacher_name} на {start_date}-{end_date}")
        formatted_schedule = format_teacher_schedule(all_schedules, teacher_name, start_date, end_date)
        
        # Кэшируем результат
        cache_teacher_schedule(teacher_name, start_date, end_date, formatted_schedule)
        
        # Логирование результата
        schedule_lines = formatted_schedule.count('\n') + 1
        logger.info(f"Успешно сформировано расписание для {teacher_name} ({schedule_lines} строк)")
        
        return formatted_schedule
        
    except Exception as e:
        logger.error(f"Ошибка при получении расписания для {teacher_name}: {e}")
        import traceback
        logger.error(traceback.format_exc())
        return f"Произошла ошибка при обработке запроса: {str(e)[:100]}"
"""

"""
Инструкция по применению:

1. Откройте файл teacher_schedule_processor.py
2. Найдите функцию async def get_teacher_schedule_with_index
3. Замените эту функцию кодом из этого файла
4. Сохраните изменения и перезапустите бота

Основные изменения:
1. Используем логику из enter_teacher для определения дат, которые попадают в файлы замен
2. Обрабатываем только те даты, которые фактически попадают в диапазоны замен
3. Добавлены таймауты на всех уровнях обработки
""" 