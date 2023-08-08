# Создание витрины данных для RFM-классификации пользователей приложения

## Легенда
Наш заказчик — компания, которая разрабатывает приложение по доставке еды.

### Что такое RFM

RFM (от англ. Recency, Frequency, Monetary Value) — способ сегментации клиентов, при котором анализируют их лояльность: как часто, на какие суммы и когда в последний раз тот или иной клиент покупал что-то. На основе этого выбирают клиентские категории, на которые стоит направить маркетинговые усилия.

Каждого клиента оценивают по трём факторам:

- Recency (пер. «давность») — сколько времени прошло с момента последнего заказа.
- Frequency (пер. «частота») — количество заказов.
- Monetary Value (пер. «денежная ценность») — сумма затрат клиента.

### Как провести RFM-сегментацию

1. Присвоим каждому клиенту три значения — значение фактора Recency, значение фактора Frequency и значение фактора Monetary Value:
    - Фактор Recency измеряется по последнему заказу. Распределим клиентов по шкале от одного до пяти, где значение `1` получат те, кто либо вообще не делал заказов, либо делал их очень давно, а `5` — те, кто заказывал относительно недавно.
    - Фактор Frequency оценивается по количеству заказов. Распределим клиентов по шкале от одного до пяти, где значение `1` получат клиенты с наименьшим количеством заказов, а `5` — с наибольшим.
    - Фактор Monetary Value оценивается по потраченной сумме. Распределим клиентов по шкале от одного до пяти, где значение `1` получат клиенты с наименьшей суммой, а `5` — с наибольшей.
2. Проверим, что количество клиентов в каждом сегменте одинаково. Например, если в базе всего 10 клиентов, то 2 клиента должны получить значение `1`, ещё 2 — значение `2` и т.д. 


## 1.1. Требования к целевой витрине.

* Для анализа нужно отобрать только успешно выполненные заказы/(Это заказ со статусом Closed) 

* Витрина должна располагаться в той же базе в схеме analysis 

* Витрина должна состоять из таких полей: 

    * user_id 

    * recency (число от 1 до 5) 

    * frequency (число от 1 до 5) 

    * monetary_value (число от 1 до 5) 

* В витрине нужны данные с начала 2022 года. 

* Название витрины dm_rfm_segments 

* Обновления не нужны. 


## 1.2. Изучим структуру исходных данных. 

Поля, которые мы будем использовать для расчета витрины: 

- Метрика Recency:  

    - table: orders  
        - columns: user_id, status, order_ts;             
    - table: users  
        - columns: id;    

* Метрика Frequency: 
    * table: orders,  
        * columns: user_id, status; 

 
* Метрика Monetary_value: 
    * table: orders,  
        * columns: user_id, status, cost. 

## 1.3. Анализ качества данных 

Данные хорошего качества, в таблице users перепутаны названия столбцов "name", login. 

Инструменты обеспечения качества данных: 

Ограничения NOT NULL, Ограничения-проверки, Ограничения уникальности, Первичный ключ, Ограничение внешнего ключа. 

Для витрины используются две таблицы: orders и orderstatuslog.

В таблице orders используются колонки:
* order_id NOT NULL, PRIMARY KEY,
* order_ts timestamp NOT NULL,
* user_id int4 NOT NULL,
* "cost" numeric(19, 5) NOT NULL DEFAULT 0,
* status int4 NOT NULL,
	
CONSTRAINT:
* orders_check CHECK ((cost = (payment + bonus_payment))).
 
 В таблице orderstatuslog используются колонки:
* id int4 NOT NULL GENERATED ALWAYS AS IDENTITY,
* order_id int4 NOT NULL,
* status_id int4 NOT NULL,
* dttm timestamp NOT NULL,

CONSTRAINT:
* orderstatuslog_order_id_status_id_key UNIQUE (order_id, status_id),
* orderstatuslog_pkey PRIMARY KEY (id),
* orderstatuslog_order_id_fkey FOREIGN KEY (order_id) REFERENCES production.orders(order_id),
* orderstatuslog_status_id_fkey FOREIGN KEY (status_id) REFERENCES production.orderstatuses(id)


## 1.4. Подготовим витрину данных

### 1.4.1. Сделаем VIEW для таблиц из базы production.

Нас просят при расчете витрины обращаться только к объектам из схемы analysis. Чтобы не дублировать данные (данные находятся в этой же базе), мы решаем сделать view. Таким образом, View будут находиться в схеме analysis и вычитывать данные из схемы production. 

SQL-запросы для создания пяти VIEW (по одному на каждую таблицу) в файле views.sql

### 1.4.2. DDL-запрос для создания витрины.

Далее нам необходимо создать витрину. Напишем CREATE TABLE запрос и выполним его на предоставленной базе данных в схеме analysis.

запрос в файле datamart_ddl.sql

### 1.4.3. SQL запрос для заполнения витрины

Создаем промежуточные таблицы analysis.tmp_rfm_recency, analysis.tmp_rfm_frequency, analysis.tmp_rfm_monetary_value: запрос в файле datamart_ddl_tmp.sql

* SQL-запрос для заполнения  analysis.tmp_rfm_recency в файле tmp_rfm_recency.sql

* SQL-запрос для заполнения  analysis.tmp_rfm_frequency в файле tmp_rfm_frequency.sql

* SQL-запрос для заполнения  analysis.tmp_rfm_monetary_value в файле tmp_rfm_monetary_value.sql

* запрос, который на основе данных, подготовленных в таблицах analysis.tmp_rfm_recency, analysis.tmp_rfm_frequency и analysis.tmp_rfm_monetary_value, заполнит витрину analysis.dm_rfm_segments: в файле datamart_query.sql

## 2. Доработка представлений

Вместо поля с одним статусом разработчики добавили таблицу для журналирования всех изменений статусов заказов — production.OrderStatusLog.

Структура таблицы production.OrderStatusLog:
* id — синтетический автогенерируемый идентификатор записи,
* order_id — идентификатор заказа, внешний ключ на таблицу production.Orders,
* status_id — идентификатор статуса, внешний ключ на таблицу статусов заказов production.OrderStatuses,
* dttm — дата и время получения заказом этого статуса.

#### код, который обновляет представление analysis.Orders

В файле orders_view.sql

