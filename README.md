# Бот расписания для учебного заведения

Телеграм-бот для получения расписания занятий для групп, преподавателей и учебных кабинетов.

## Модульная структура

Бот имеет модульную структуру, где различные функциональности вынесены в отдельные файлы:

- `было.py` - основной файл бота с ключевой логикой
- `classroom_schedule.py` - модуль для работы с расписанием кабинетов
- `cache_utils.py` - утилиты для кэширования данных

## Функциональность

### Расписание кабинетов

Модуль `classroom_schedule.py` обеспечивает следующие возможности:
- Получение полного расписания для конкретного кабинета на выбранную дату
- Учёт замен и перемещений занятий между кабинетами
- Кэширование результатов для ускорения работы
- Обработка различных форматов обозначения кабинетов (включая замену латинских букв на кириллические)

Для получения расписания кабинета используйте команду:
```
/classroom У505 06.03.2025
```

Формат ответа:
```
📅 Расписание для кабинета У505 на четверг 06.03.2025 (четная неделя):

2️⃣ ✏️ 1. (Лаб) Метрология и электротех.измерения 🎓Иванченко А.П. 👥 [Кс-23-1, 1-я подгруппа]

3️⃣ (КП) МДК.03.01 Тех. обсл. и рем. ком.сист 🎓Иванченко А.П. 👥 [КсК-21-1, 1-я подгруппа]

4️⃣ (КП) МДК.03.01 Тех. обсл. и рем. ком.сист 🎓Иванченко А.П. 👥 [Кс-21-1, 1-я подгруппа]
```

Где:
- 2️⃣, 3️⃣, 4️⃣ - номер пары
- ✏️ - обозначение замены
- 🎓 - преподаватель
- 👥 - группа и подгруппа

# Enhanced Telegram Bot with True Concurrent Processing

This is an enhanced version of the Telegram bot that significantly improves performance when handling multiple users and processing Excel files.

## Key Improvements

1. **True Concurrent Command Handling**: 
   - Multiple users can interact with the bot simultaneously
   - Commands from different users are processed in parallel
   - Long-running operations don't block other users
   - Patched at the core dispatcher level for true concurrency

2. **Enhanced Teacher Schedule Processing**:
   - Parallel processing of Excel files with larger thread pools
   - Smart file selection based on date relevance
   - Request deduplication to avoid redundant work
   - Efficient caching of results
   - Preloading of Excel files for faster first-time access

3. **Advanced Excel File Caching**:
   - In-memory caching of Excel workbooks
   - Proactive preloading of Excel files at startup
   - Asynchronous file loading to avoid blocking
   - Automatic cache invalidation when files change
   - Significant performance improvement for Excel operations

4. **Non-Invasive Integration**:
   - Works without modifying the original code
   - Falls back to original implementation if enhanced version fails
   - Maintains backward compatibility

## How to Use

Instead of running the original bot directly, use the enhanced version by running:

```bash
python main.py
```

This will start the bot with all the performance enhancements while maintaining the same functionality.

## How It Works

The enhancement works through several mechanisms:

### True Concurrent Command Handling
- Patches the core dispatcher to process updates in separate tasks
- Wraps all command handlers to run as asyncio tasks
- Allows multiple commands to be processed simultaneously
- Tracks tasks by user ID for proper cleanup

### Teacher Schedule Processing
- Uses dedicated thread pools for Excel file processing
- Implements request deduplication to avoid redundant work
- Only processes Excel files relevant to the requested dates
- Caches results to avoid reprocessing
- Preloads Excel files to improve first-time performance

### Advanced Excel File Caching
- Maintains an in-memory cache of Excel workbooks
- Proactively preloads Excel files at startup
- Monitors files for changes to invalidate cache entries
- Limits cache size to prevent memory issues
- Significantly reduces file I/O operations

## Files

- `bot_concurrency.py`: Enables true concurrent command handling
- `teacher_schedule_processor.py`: Optimizes teacher schedule processing
- `excel_cache.py`: Implements advanced Excel file caching
- `schedule_wrapper.py`: Provides integration with the original code
- `main.py`: Entry point that applies all enhancements

## Performance Benefits

The enhanced version provides significant performance improvements:

1. **Multiple Users**: Bot can handle many users simultaneously with true concurrency
2. **Faster Response Times**: Excel processing is much faster, especially after first use
3. **Reduced Resource Usage**: Avoids redundant processing and file loading
4. **Better Scalability**: Can handle more users and larger workloads
5. **Improved First-Time Performance**: Proactive preloading reduces initial delays

## Troubleshooting

If you encounter any issues with the enhanced version, you can revert to the original implementation by running the original bot directly. 