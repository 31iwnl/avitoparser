import csv
import logging
import random
import re
import os
import time
from threading import Lock
from concurrent.futures import ThreadPoolExecutor, as_completed
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.common.exceptions import WebDriverException, NoSuchElementException
from webdriver_manager.chrome import ChromeDriverManager

USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/115.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:115.0) Gecko/20100101 Firefox/115.0",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 13_5) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/16.5 Safari/605.1.15",
]

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s %(levelname)s %(threadName)s %(message)s',
)

# Глобальный замок для записи в CSV
write_lock = Lock()


def create_driver(headless: bool = True) -> webdriver.Chrome:
    options = Options()
    if headless:
        options.add_argument("--headless=new")
    options.add_argument("--disable-gpu")
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")
    options.add_argument("--window-size=1920,1080")
    options.add_argument("--lang=ru-RU")
    options.add_argument("--disable-software-rasterizer")
    options.add_argument("--disable-extensions")
    options.add_argument("--disable-features=VizDisplayCompositor")

    user_agent = random.choice(USER_AGENTS)
    options.add_argument(f"user-agent={user_agent}")
    logging.info(f"Using User-Agent: {user_agent}")

    driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=options)
    driver.set_page_load_timeout(60)
    return driver


def is_blocked(driver: webdriver.Chrome) -> bool:
    try:
        title = driver.title.lower()
        if "доступ ограничен" in title or "access denied" in title:
            return True
        if "captcha" in driver.page_source.lower():
            return True
        return False
    except Exception:
        return False


def wait_for_captcha(city: str, timeout: int = 5):
    logging.warning(f"[{city}] Обнаружена капча! Ждем {timeout} секунд перед повтором...")
    time.sleep(timeout)
    logging.info(f"[{city}] Продолжаем работу после паузы.")


def get_listing_count(driver: webdriver.Chrome) -> int:
    # Улучшенная функция для парсинга чисел с пробелами (формат 4 970)
    def parse_count(text: str) -> int:
        # Удаляем все нецифровые символы, кроме пробелов
        cleaned = re.sub(r'[^\d\s]', '', text)
        # Заменяем пробелы и преобразуем в число
        return int(cleaned.replace(' ', '')) if cleaned.strip() else 0

    # Попробуем разные стратегии поиска
    try:
        # Стратегия 1: основной элемент с количеством
        count_elem = driver.find_element(By.CSS_SELECTOR, ".page-title-count")
        count = parse_count(count_elem.text)
        if count > 0:
            return count
    except NoSuchElementException:
        pass

    try:
        # Стратегия 2: альтернативный элемент
        count_elem = driver.find_element(By.CSS_SELECTOR, "[data-marker='page-title/count']")
        count = parse_count(count_elem.text)
        if count > 0:
            return count
    except NoSuchElementException:
        pass

    try:
        # Стратегия 3: поиск в заголовке
        title = driver.title
        match = re.search(r'(\d[\d\s]*) (?:объявлен|объявления|объявлений)', title)
        if match:
            return parse_count(match.group(1))
    except Exception:
        pass

    try:
        # Стратегия 4: поиск по всему контенту
        match = re.search(r'(\d[\d\s]*) (?:объявлен|объявления|объявлений)', driver.page_source)
        if match:
            return parse_count(match.group(1))
    except Exception:
        pass

    logging.warning("Не удалось найти количество объявлений ни одним методом")
    return 0


def test(driver, city_name: str, city_id: str, city_url: str, category_name: str, category_url: str,
         max_retries: int = 3):
    url = category_url.replace("ulyanovsk", city_url)
    for attempt in range(1, max_retries + 1):
        try:
            logging.info(f"[{city_name}][{category_name}] Попытка {attempt}: открываю URL: {url}")
            driver.get(url)
            time.sleep(random.uniform(4, 7))

            if is_blocked(driver):
                logging.warning(f"[{city_name}][{category_name}] Обнаружена блокировка или капча.")
                wait_for_captcha(city_name)
                driver.get(url)
                time.sleep(random.uniform(4, 7))

            count = get_listing_count(driver)
            logging.info(f"[{city_name}][{category_name}] Найдено объявлений: {count}")
            return (city_name, city_id, city_url, category_name, count)

        except WebDriverException as e:
            logging.error(f"[{city_name}][{category_name}] Ошибка WebDriver: {e}")
            if attempt == max_retries:
                logging.error(f"[{city_name}][{category_name}] Превышено число попыток, пропускаем.")
                return (city_name, city_id, city_url, category_name, 0)
            else:
                logging.info(f"[{city_name}][{category_name}] Ошибка, повтор через паузу...")
                time.sleep(10)
        except Exception as e:
            logging.error(f"[{city_name}][{category_name}] Неожиданная ошибка: {e}")
            return (city_name, city_id, city_url, category_name, 0)
    return (city_name, city_id, city_url, category_name, 0)


def worker(jobs, output_file):
    logging.info(f"Поток стартовал с {len(jobs)} задачами")
    driver = create_driver(headless=True)
    try:
        for job in jobs:
            city_name, city_id, city_url, category_name, category_url = job
            result = test(driver, city_name, city_id, city_url, category_name, category_url)

            # Запись результата в CSV с блокировкой
            with write_lock:
                with open(output_file, 'a', newline='', encoding='utf-8') as fout:
                    writer = csv.writer(fout, delimiter=';')
                    writer.writerow(result)
                    fout.flush()  # Принудительный сброс буфера
                    logging.info(f"Записано в CSV: {result}")

            sleep_time = random.uniform(8, 15)  # Увеличил случайную задержку
            logging.info(f"Ждем {sleep_time:.1f} сек. перед следующим запросом.")
            time.sleep(sleep_time)
    except Exception as e:
        logging.error(f"Ошибка в worker: {e}")
    finally:
        driver.quit()
        logging.info(f"Поток завершил работу")
    return len(jobs)


def main():
    cities_file = "avito.csv"
    categories_file = "avito.txt"
    output_file = time.strftime("%d-%m-%Y") + ".csv"

    # Получаем абсолютный путь для логов
    abs_output_file = os.path.abspath(output_file)
    logging.info(f"Результаты будут записаны в файл: {abs_output_file}")

    # Создаем файл и записываем заголовок
    with open(output_file, 'w', newline='', encoding='utf-8') as fout:
        writer = csv.writer(fout, delimiter=';')
        writer.writerow(["Город", "ID", "URL_имя", "Категория", "Количество"])

    # Загрузка категорий
    categories = []
    with open(categories_file, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line or line.startswith("#"):
                continue
            parts = line.split(None, 1)
            if len(parts) == 2:
                categories.append((parts[0], parts[1]))

    # Загрузка городов
    cities = []
    with open(cities_file, "r", encoding="utf-8") as fin:
        reader = csv.reader(fin)
        for row in reader:
            if len(row) < 3:
                continue
            city_name = row[0].strip()
            city_id = row[1].strip()
            city_url = row[2].strip()
            cities.append((city_name, city_id, city_url))

    # Создание задач
    all_jobs = []
    for city_name, city_id, city_url in cities:
        for category_name, category_url in categories:
            all_jobs.append((city_name, city_id, city_url, category_name, category_url))

    # Разбиение на части для потоков
    num_workers = 3
    chunk_size = (len(all_jobs) + num_workers - 1) // num_workers
    chunks = [all_jobs[i:i + chunk_size] for i in range(0, len(all_jobs), chunk_size)]

    # Запуск потоков
    with ThreadPoolExecutor(max_workers=num_workers) as executor:
        futures = [executor.submit(worker, chunk, output_file) for chunk in chunks]

        # Ожидание завершения всех задач
        for future in as_completed(futures):
            try:
                processed_count = future.result()
                logging.info(f"Поток обработал {processed_count} задач")
            except Exception as e:
                logging.error(f"Ошибка в потоке: {e}")

    logging.info(f"Готово! Результаты сохранены в {abs_output_file}")


if __name__ == "__main__":
    main()