# Генератор спрощених Artist Report (.xlsx)

Сервіс читає вашу **Google Sheet** (з кількома містами — окремі аркуші), підставляє спрощені дані у локальний Excel-шаблон **`template.xlsx`** у репозиторії та повертає **один .xlsx** з окремим аркушем на кожне обране місто.

Форматування (кольори, шрифти, ширина колонок) береться з аркуша **`Template`** у `template.xlsx` (якщо такого аркуша немає — використовується перший аркуш).

## 1) Встановлення

```bash
python -m venv .venv
.venv\Scripts\activate
pip install -r requirements.txt
```

## 2) Доступ до Google Sheets

1. Створіть **Service Account** у Google Cloud.
2. Увімкніть **Google Sheets API**.
3. Завантажте JSON ключ, наприклад `service-account.json`.
4. Додайте email цього Service Account у **Share** вашої **вихідної** таблиці (як Editor).

Окремий **Google Drive API** для цього сценарію не потрібен: новий Google-файл не створюється, лише читається source.

## 3) Шаблон `template.xlsx`

- Файл лежить у корені репозиторію поруч із кодом.
- Замініть його на свій дизайн; залиште аркуш з назвою **`Template`** (рекомендовано).
- Для кожного міста сервіс **копіює** цей аркуш, очищає клітинки і записує до **5 колонок** даних (як у спрощеній логіці).

Шлях до шаблону можна перевизначити змінною середовища **`TEMPLATE_XLSX_PATH`** (абсолютний шлях до .xlsx).

## 4) Запуск

```bash
python main.py
```

## 5) Деплой на Vercel (API)

У репозиторій додано Python entrypoint `main.py` з endpoint:

- `POST /cities-ui` — повертає список міст (назви аркушів);
- `POST /sync-ui` — same-origin, повертає **файл .xlsx** (завантаження в браузері);
- `POST /api/sync` — JSON з полями `filename`, `xlsx_base64`, `debug` (для великих файлів зверніть увагу на ліміти розміру відповіді).

Також веб-інтерфейс на головній сторінці `/`:

- поле: посилання на загальну Google Sheet;
- завантажити міста → вибір (або «всі»);
- кнопка генерації → завантаження `.xlsx`.

### Environment Variables у Vercel

- `SERVICE_ACCOUNT_JSON` — повний JSON ключ сервісного акаунта одним рядком.
- `SYNC_TOKEN` — секретний токен для захисту endpoint.
- `SOURCE_SHEET_URL` — (опційно) URL вашої таблиці за замовчуванням.
- `TEMPLATE_XLSX_PATH` — (опційно) шлях до кастомного шаблону на сервері.

### Виклик API

```bash
curl -X POST "https://YOUR-PROJECT.vercel.app/api/sync" ^
  -H "Authorization: Bearer YOUR_SYNC_TOKEN" ^
  -H "Content-Type: application/json" ^
  -d "{\"source_url\":\"https://docs.google.com/spreadsheets/d/YOUR_ID/edit\",\"selected_cities\":[\"Berlin\"]}"
```

Якщо `SOURCE_SHEET_URL` заданий в env, `source_url` у body можна не передавати.
