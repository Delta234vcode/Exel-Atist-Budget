# Генератор спрощених Artist Report аркушів

Сервіс бере велику таблицю (де може бути багато міст), залишає тільки потрібні міста
та видаляє зайві рядки/стовпчики, щоб вийшов спрощений формат.

Результат: нова Google Sheet із окремими аркушами по вибраних містах.

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
python main.py
```

## Корисні параметри

- `--source-sheet-name "Tab name"`: якщо треба не перший лист.
- `--target-sheet-name "Tab name"`: якщо треба не перший лист.
- `--source-header-row 15`: рядок із заголовками у вашій таблиці (для блоку Plan costs).
- `--target-header-row 9`: рядок із заголовками у таблиці артиста.

## Деплой на Vercel (API)

У репозиторій додано Python entrypoint `main.py` з endpoint:
- `POST /cities-ui` — повертає список міст (назви аркушів);
- `POST /sync-ui` або `POST /api/sync` — створює спрощену таблицю.

Також додано веб-інтерфейс на головній сторінці `/`:
- поле 1: загальна таблиця;
- кнопка завантажити міста;
- вибір міст (або всі);
- кнопка генерації.

### Environment Variables у Vercel

- `SERVICE_ACCOUNT_JSON` — повний JSON ключ сервісного акаунта одним рядком.
- `SYNC_TOKEN` — секретний токен для захисту endpoint.
- `SOURCE_SHEET_URL` — (опційно) URL вашої таблиці за замовчуванням.
- `TARGET_SHEET_URL` — не обов'язкова (сервіс створює нову таблицю через Sheets API).

### Робота через UI

- Відкрий `https://YOUR-PROJECT.vercel.app/`
- Встав URL загальної таблиці
- Натисни `Завантажити міста`
- Вибери потрібні міста або `Вибрати всі`
- Натисни `Згенерувати спрощені аркуші`

UI використовує `POST /sync-ui` (same-origin only). Для зовнішніх інтеграцій використовуй `POST /api/sync` з `Authorization: Bearer <SYNC_TOKEN>`.

### Виклик endpoint

```bash
curl -X POST "https://YOUR-PROJECT.vercel.app/api/sync" ^
  -H "Authorization: Bearer YOUR_SYNC_TOKEN" ^
  -H "Content-Type: application/json" ^
  -d "{\"source_url\":\"https://docs.google.com/spreadsheets/d/170iP6rlKZFqL7qho9okUbL9i4AT-vwkAh4bQXCNN8Vg/edit?usp=sharing\",\"target_url\":\"https://docs.google.com/spreadsheets/d/1VMwwk7fzsCWMo6XOQQWQdXkMbJ4r0g9StR5-FCnanz8/edit?usp=sharing\"}"
```

Якщо `SOURCE_SHEET_URL` заданий в env, body можна не передавати.
