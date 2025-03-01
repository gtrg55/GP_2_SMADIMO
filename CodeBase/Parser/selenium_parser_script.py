"""
Steam Market Parser - Улучшенная версия с использованием Selenium
"""

import time
import re
import json
import os
import locale
from datetime import datetime, timedelta
import matplotlib.pyplot as plt
import numpy as np
from loguru import logger
import pandas as pd

from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC

try:
    locale.setlocale(locale.LC_TIME, 'en_US.UTF-8')
except:
    try:
        locale.setlocale(locale.LC_TIME, 'C')
    except:
        print("Предупреждение: Не удалось установить английскую локаль, парсинг дат может работать некорректно")

script_dir = os.path.dirname(os.path.abspath(__file__))
log_file = os.path.join(script_dir, "steam_parser.log")

logger.remove()
logger.add(log_file, rotation="10 MB", level="INFO", 
           format="{time:YYYY-MM-DD HH:mm:ss} | {level} | {message}")
logger.add(lambda msg: print(f"LOG: {msg}"), level="INFO", 
          filter=lambda record: record["level"].name in ["WARNING", "ERROR", "CRITICAL"])


def setup_selenium_driver():
    """
    Настройка и возвращение Selenium WebDriver
    
    Returns:
        webdriver: Настроенный экземпляр драйвера Chrome
    """
    logger.info("Настройка Selenium Chrome WebDriver")
    
    chrome_options = Options()
    chrome_options.add_argument("--headless")
    chrome_options.add_argument("--no-sandbox")
    chrome_options.add_argument("--disable-dev-shm-usage")
    chrome_options.add_argument("--window-size=1920,1080")
    chrome_options.add_argument("--disable-gpu")
    chrome_options.add_argument(f"user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36")
    
    try:
        try:
            from webdriver_manager.chrome import ChromeDriverManager
            driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=chrome_options)
            logger.info("WebDriver успешно инициализирован с помощью webdriver_manager")
        except:
            driver = webdriver.Chrome(options=chrome_options)
            logger.info("WebDriver успешно инициализирован напрямую")
            
        logger.success("Настройка Selenium WebDriver завершена")
        return driver
    except Exception as e:
        logger.error(f"Ошибка при инициализации Selenium WebDriver: {str(e)}")
        print(f"Ошибка при инициализации Selenium WebDriver: {str(e)}")
        print("Убедитесь, что у вас установлен Chrome и драйвер Chrome.")
        print("Установите необходимые пакеты: pip install selenium webdriver-manager")
        raise


def parse_steam_market_data(url):
    """
    Парсинг данных истории цен с рынка Steam с использованием Selenium
    
    Args:
        url (str): URL листинга на рынке Steam
        
    Returns:
        dict: Словарь, содержащий название предмета и данные о ценах
    """
    logger.info(f"Начало парсинга данных с: {url}")
    
    try:
        driver = setup_selenium_driver()
    except Exception as e:
        logger.critical(f"Не удалось настроить Selenium WebDriver: {str(e)}")
        return None
    
    try:
        logger.info("Загрузка страницы Steam Market")
        driver.get(url)
        
        logger.info("Ожидание загрузки содержимого страницы")
        WebDriverWait(driver, 30).until(
            EC.presence_of_element_located((By.ID, "market_commodity_forsale_table"))
        )
        
        screenshot_path = os.path.join(script_dir, "steam_page_screenshot.png")
        driver.save_screenshot(screenshot_path)
        logger.info(f"Сохранен скриншот страницы: {screenshot_path}")
        
        item_name = "Chroma 3 Case"  
        
        try:
            item_name_element = driver.find_element(By.CLASS_NAME, "market_listing_item_name")
            item_name = item_name_element.text.strip()
            logger.info(f"Обнаружено название предмета: {item_name}")
        except Exception as e:
            logger.warning(f"Не удалось получить название предмета через class: {str(e)}")
            try:
                
                item_name_element = driver.find_element(By.XPATH, "//span[contains(@class, 'market_listing_item_name')]")
                item_name = item_name_element.text.strip()
                logger.info(f"Обнаружено название предмета через xpath: {item_name}")
            except Exception as e2:
                logger.warning(f"Не удалось получить название предмета через xpath: {str(e2)}")
                
                try:
                    item_name = url.split('/')[-1].replace('%20', ' ').replace('%3A', ':')
                    logger.info(f"Извлечено название предмета из URL: {item_name}")
                except:
                    logger.warning("Не удалось извлечь название предмета из URL, используем значение по умолчанию")
        
        if not item_name or item_name.strip() == "":
            item_name = "Chroma 3 Case"
            logger.warning(f"Название предмета пустое, используем значение по умолчанию: {item_name}")
        
        logger.info("Извлечение данных истории цен через JavaScript")
        
        try:
            price_data = driver.execute_script("return line1;")
            if price_data and len(price_data) > 0:
                logger.info(f"Успешно получены данные истории цен через переменную line1 ({len(price_data)} точек)")
            else:
                logger.warning("Не удалось получить данные через переменную line1, пробуем альтернативный метод")
                price_data = None
        except Exception as e:
            logger.warning(f"Ошибка при доступе к переменной line1: {str(e)}")
            price_data = None
        
        if not price_data:
            logger.info("Поиск данных истории цен в скрипте")
            try:
                script_elements = driver.find_elements(By.TAG_NAME, "script")
                
                for script in script_elements:
                    try:
                        script_content = script.get_attribute("innerHTML")
                        if "var line1=" in script_content:
                            logger.info("Найден скрипт с данными истории цен")
                            
                            match = re.search(r"var line1=(\[.*?\]);", script_content, re.DOTALL)
                            if match:
                                price_data_str = match.group(1)
                                
                                try:
                                    price_data = json.loads(price_data_str)
                                    logger.info(f"Успешно извлечены данные истории цен ({len(price_data)} точек)")
                                    break
                                except json.JSONDecodeError:
                                    price_data = eval(price_data_str)
                                    logger.info(f"Успешно извлечены данные истории цен с использованием eval ({len(price_data)} точек)")
                                    break
                    except Exception as script_error:
                        logger.debug(f"Ошибка обработки скрипта: {str(script_error)}")
                        continue
            except Exception as e:
                logger.error(f"Ошибка при поиске данных в скриптах: {str(e)}")
        
        if not price_data:
            logger.error("Не удалось извлечь данные истории цен")
            return None
            
        logger.info(f"Пример данных (первые 3 точки): {price_data[:3]}")
        logger.info(f"Итоговое название предмета для анализа: {item_name}")
        
        return {
            'item_name': item_name,
            'price_data': price_data
        }
        
    except Exception as e:
        logger.error(f"Ошибка при парсинге данных с помощью Selenium: {str(e)}")
        return None
        
    finally:
        try:
            logger.info("Закрытие Selenium WebDriver")
            driver.quit()
        except:
            pass


def analyze_price_data(price_data):
    """
    Анализ данных о ценах: расчет временных промежутков, фильтрация за последние 3 года,
    и возврат статистики
    
    Args:
        price_data (list): Список точек [timestamp, price]
        
    Returns:
        dict: Статистика и отфильтрованные данные
    """
    if not price_data or len(price_data) < 2:
        logger.error("Недостаточно данных для анализа")
        return None
    
    logger.info(f"Анализ {len(price_data)} точек данных о ценах")
    logger.info(f"Первые 5 точек данных: {price_data[:5]}")
    
    timestamps = []
    prices = []
    
    for i, point in enumerate(price_data):
        if len(point) < 2:
            logger.warning(f"Пропуск точки данных {point}, недостаточно элементов")
            continue
            
        ts, price = point[0], point[1]
        
        if isinstance(ts, str):
            try:
                clean_date_str = ts.replace(": +0", "").replace(": -0", "")
                dt = datetime.strptime(clean_date_str, "%b %d %Y %H")
                ts = dt.timestamp()
                logger.debug(f"Успешно преобразована дата {ts} из строки {point[0]}")
            except (ValueError, TypeError) as e:
                logger.warning(f"Невозможно преобразовать временную метку '{ts}' в дату: {str(e)}")
                continue
        
        if isinstance(price, str):
            try:
                price = price.replace('$', '').replace('€', '').replace('£', '').replace(',', '.')
                price = float(price)
            except ValueError:
                logger.warning(f"Невозможно преобразовать цену '{price}' в число, пропуск")
                continue
        
        timestamps.append(ts)
        prices.append(price)
    
    if not timestamps:
        logger.error("Не удалось извлечь действительные временные метки из данных")
        return None
    
    logger.info(f"Преобразование {len(timestamps)} временных меток в объекты datetime")
    dates = []
    for ts in timestamps:
        try:
            if ts > 10000000000:
                ts = ts / 1000
            dates.append(datetime.fromtimestamp(ts))
        except Exception as e:
            logger.warning(f"Ошибка преобразования временной метки {ts}: {str(e)}")
    
    if not dates:
        logger.error("Не удалось преобразовать временные метки в даты")
        return None
    
    data_points = list(zip(timestamps, prices, dates))
    data_points.sort(key=lambda x: x[2])
    
    three_years_ago = datetime.now() - timedelta(days=3*365)
    logger.info(f"Фильтрация данных с {three_years_ago} по настоящее время")
    
    filtered_data = [point for point in data_points if point[2] >= three_years_ago]
    
    if not filtered_data:
        logger.warning("Данные за последние 3 года не найдены")
        filtered_data = data_points
        logger.info("Используем все доступные данные вместо фильтрации за 3 года")
    
    logger.info(f"Отфильтровано {len(filtered_data)} точек данных")
    
    gaps = []
    for i in range(1, len(filtered_data)):
        current = filtered_data[i][2]
        previous = filtered_data[i-1][2]
        gap_minutes = (current - previous).total_seconds() / 60
        gaps.append(gap_minutes)
    
    stats = {
        'start_date': filtered_data[0][2],
        'end_date': filtered_data[-1][2],
        'total_points': len(filtered_data),
        'avg_gap_minutes': sum(gaps) / len(gaps) if gaps else 0,
        'min_gap_minutes': min(gaps) if gaps else 0,
        'max_gap_minutes': max(gaps) if gaps else 0,
        'filtered_data': filtered_data
    }
    
    logger.success("Анализ данных завершен")
    return stats


def plot_price_data(stats, item_name):
    """
    Построение графика данных о ценах и отображение статистики
    
    Args:
        stats (dict): Статистика из analyze_price_data
        item_name (str): Название предмета
    """
    if not stats:
        logger.error("Нет статистики для построения графика")
        return
    
    logger.info("Построение графика данных о ценах")
    
    filtered_data = stats['filtered_data']
    dates = [point[2] for point in filtered_data]
    prices = [point[1] for point in filtered_data]
    
    plt.figure(figsize=(15, 8))
    plt.plot(dates, prices, marker='.', linestyle='-', color='orange', linewidth=1)
    plt.xlabel('Дата')
    plt.ylabel('Цена')
    plt.title(f'История цен для {item_name} (С {stats["start_date"].strftime("%Y-%m-%d")} по {stats["end_date"].strftime("%Y-%m-%d")})')
    plt.grid(True, alpha=0.3)
    
    plt.gcf().autofmt_xdate()
    
    info_text = (
        f"Период данных: с {stats['start_date'].strftime('%Y-%m-%d')} по {stats['end_date'].strftime('%Y-%m-%d')}\n"
        f"Всего точек данных: {stats['total_points']}\n"
        f"Средний промежуток между точками: {stats['avg_gap_minutes']:.2f} минут\n"
        f"Минимальный промежуток: {stats['min_gap_minutes']:.2f} минут\n"
        f"Максимальный промежуток: {stats['max_gap_minutes']:.2f} минут"
    )
    
    plt.figtext(0.02, 0.02, info_text, wrap=True, fontsize=10, 
                bbox=dict(facecolor='white', alpha=0.8))
    
    filename = os.path.join(script_dir, f"{item_name.replace(' ', '_').replace('|', '').replace(':', '')}_price_history.png")
    plt.savefig(filename, dpi=300, bbox_inches='tight')
    logger.info(f"График сохранен как {filename}")
    
    plt.show()
    
    logger.info("\n" + info_text)
    print("\n" + info_text)


def export_to_csv(stats, item_name):
    """Экспорт отфильтрованных данных в CSV-файл"""
    if not stats:
        logger.error("Нет данных для экспорта")
        return
    
    logger.info("Экспорт данных в CSV")
    
    filtered_data = stats['filtered_data']
    
    df_detailed = pd.DataFrame({
        'timestamp': [point[0] for point in filtered_data],
        'date': [point[2].strftime('%Y-%m-%d %H:%M:%S') for point in filtered_data],
        'price': [point[1] for point in filtered_data]
    })
    
    detailed_filename = os.path.join(script_dir, f"{item_name.replace(' ', '_').replace('|', '').replace(':', '')}_price_data.csv")
    df_detailed.to_csv(detailed_filename, index=False)
    logger.success(f"Подробные данные экспортированы в {detailed_filename}")
    
    short_name = item_name
    if len(item_name) > 15:
        short_name = item_name.split()[0]
    
    
    if not short_name or short_name.strip() == "":
        short_name = "Chroma Case"  # Дефолтное значение
    
    logger.info(f"Используем короткое имя инструмента: '{short_name}'")
    
    simple_data = []
    for point in filtered_data:
        simple_data.append({
            'instrument': short_name,
            'price_usd': round(point[1], 2),
            'timestamp': int(point[0])
        })
    
    df_simple = pd.DataFrame(simple_data)
    
    simple_filename = os.path.join(script_dir, "steam_market_prices.csv")
    df_simple.to_csv(simple_filename, index=False)
    logger.success(f"Единый датафрейм экспортирован в {simple_filename}")
    
    print(f"Данные экспортированы в:\n- {detailed_filename} (подробные)\n- {simple_filename} (единый датафрейм с {len(simple_data)} записями)")


def main():
    """Основная функция для запуска парсера"""
    logger.info(f"Запуск парсера рынка Steam с использованием Selenium. Логи сохраняются в: {log_file}")
    print(f"Парсер рынка Steam (Selenium-версия)")
    print(f"Логи сохраняются в: {log_file}")
    
    default_url = "https://steamcommunity.com/market/listings/730/Chroma%203%20Case"
    
    print("-" * 50)
    url = input(f"Введите URL рынка Steam (или нажмите Enter для URL по умолчанию - {default_url}): ")
    
    if not url:
        url = default_url
        logger.info(f"Использование URL по умолчанию: {url}")
        print(f"Использование URL по умолчанию: {url}")
    else:
        logger.info(f"Пользователь предоставил URL: {url}")
    
    print("Начало парсинга данных с помощью Selenium...")
    
    result = parse_steam_market_data(url)
    
    if not result:
        logger.error("Не удалось получить данные. Выход.")
        print("Не удалось получить данные. Пожалуйста, проверьте файл steam_parser.log для получения подробной информации об ошибке.")
        return
    
    print(f"Получены данные для предмета: {result['item_name']}")
    print(f"Количество точек данных: {len(result['price_data'])}")
    
    stats = analyze_price_data(result['price_data'])
    
    if not stats:
        logger.error("Не удалось проанализировать данные. Выход.")
        print("Не удалось проанализировать данные. Проверьте лог-файл для получения подробной информации.")
        return
    
    print(f"Анализ успешно завершен, найдено {stats['total_points']} точек данных.")
    
    plot_price_data(stats, result['item_name'])
    
    export_to_csv(stats, result['item_name'])
    
    logger.info("Выполнение парсера завершено")
    print("Выполнение парсера успешно завершено!")


if __name__ == "__main__":
    main()
