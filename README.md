# Автозаповнення Artist Report

Скрипт переносить:
- фактичну суму (`FACT / FACT EUR`) з вашої таблиці;
- посилання на інвойс (як `HYPERLINK`) у таблицю артиста.

Зіставлення йде за назвою рядка витрати (наприклад `hotel band and artist`).

## 1) Встановлення

```bash
python -m venv .venv
.venv\Scripts\activate
pip install -r requirements.txt
```

## 2) Доступ до Google Sheets

1. Створіть **Service Account** у Google Cloud.
2. Увімкніть Google Sheets API.
3. Завантажте JSON ключ, наприклад `service-account.json`.
4. Додайте email цього Service Account у `Share` обох таблиць (як Editor).

## 3) Запуск

```bash
python sync_artist_report.py ^
  --service-account "C:\path\to\service-account.json" ^
  --source-url "https://docs.google.com/spreadsheets/d/170iP6rlKZFqL7qho9okUbL9i4AT-vwkAh4bQXCNN8Vg/edit?usp=sharing" ^
  --target-url "https://docs.google.com/spreadsheets/d/1VMwwk7fzsCWMo6XOQQWQdXkMbJ4r0g9StR5-FCnanz8/edit?usp=sharing"
```

## Корисні параметри

- `--source-sheet-name "Tab name"`: якщо треба не перший лист.
- `--target-sheet-name "Tab name"`: якщо треба не перший лист.
- `--source-header-row 15`: рядок із заголовками у вашій таблиці (для блоку Plan costs).
- `--target-header-row 9`: рядок із заголовками у таблиці артиста.

## Як працює зіставлення

- Нормалізує текст (нижній регістр, прибирає зайві пробіли).
- Шукає в source рядок із такою ж назвою, як у target.
- Якщо знайдено:
  - оновлює суму;
  - вставляє `HYPERLINK` на інвойс, якщо в source знайдений URL.

## Деплой на Vercel (API)

У репозиторій додано Python entrypoint `main.py` з endpoint `POST /api/sync`.
Також додано веб-інтерфейс на головній сторінці `/`:
- поле 1: загальна таблиця;
- поле 2: таблиця артиста;
- кнопка `Синхронізувати`.

### Environment Variables у Vercel

- `SERVICE_ACCOUNT_JSON` — повний JSON ключ сервісного акаунта одним рядком.
- `SYNC_TOKEN` — секретний токен для захисту endpoint.
- `SOURCE_SHEET_URL` — (опційно) URL вашої таблиці за замовчуванням.
- `TARGET_SHEET_URL` — (опційно) URL таблиці артиста за замовчуванням.

### Робота через UI

- Відкрий `https://YOUR-PROJECT.vercel.app/`
- Встав 2 Google Sheets URL
- Натисни `Синхронізувати`

UI використовує `POST /sync-ui` (same-origin only). Для зовнішніх інтеграцій використовуй `POST /api/sync` з `Authorization: Bearer <SYNC_TOKEN>`.

### Виклик endpoint

```bash
curl -X POST "https://YOUR-PROJECT.vercel.app/api/sync" ^
  -H "Authorization: Bearer YOUR_SYNC_TOKEN" ^
  -H "Content-Type: application/json" ^
  -d "{\"source_url\":\"https://docs.google.com/spreadsheets/d/170iP6rlKZFqL7qho9okUbL9i4AT-vwkAh4bQXCNN8Vg/edit?usp=sharing\",\"target_url\":\"https://docs.google.com/spreadsheets/d/1VMwwk7fzsCWMo6XOQQWQdXkMbJ4r0g9StR5-FCnanz8/edit?usp=sharing\"}"
```

Якщо `SOURCE_SHEET_URL` і `TARGET_SHEET_URL` задані в env, body можна не передавати.
