# goit-de-hw-07


docker-compose create - загрузка образів докера
docker-compose start - запуск образів

p_1 - нормальне виконання
p_2 - результати виконання завданнь
p_3 - виконання з затримкою

Структура DAG:
DAG складається з кількох чітко визначених завдань, кожне з яких виконує певну функцію: створення таблиці, вибір медалі, виконання SQL-розрахунків, затримка та перевірка результатів.
У ньому застосовуються як умовні оператори (BranchPythonOperator), так і стандартні Python та SQL-оператори.
DAG налаштований так, що його можна легко модифікувати: змінювати логіку вибору медалі або коригувати SQL-запити без порушення загальної структури.
Використання Python забезпечує інтеграцію складної бізнес-логіки.