create table default.orders_good engine=MergeTree order by OrderID settings index_granularity = 1 as 
select OrderID, toDate(parseDateTimeBestEffort(OrderDate)) OrderDate, ProductName, ProductBrand, ProductSubcategory, ProductCategory, DeliveryType, PaymentType, ClientGender, Sales, ClientStatus, ShopName, ShopAddress, ShopAddressCoord
from orders
;


-- ищем, по какому пути находится физически хранение таблицы, нужно поле path
select * from system.parts where active=1 and table = 'orders_good';


-- убеждаемся в весе таблицы - data_compressed_bytes - сколько занимает места при уже примененном сжатии
select formatReadableSize(data_compressed_bytes) from system.parts where active=1 and table = 'orders_good';


-- отключаем чтение из кэша для запросов типа select, для честности тестов. Работает только в рамках сеанса. При разрыве соединения
-- dbeaver и ClickHouse необходимо выполнить команду еще раз. Менять настройку глобально крайне не рекоммендуется, но можно (+ это сложно).
SET enable_reads_from_query_cache = 0;


------- system.query_log Описание нужных колонок, для удобства
-- query (String) — текст запроса.
-- read_rows (UInt64) — общее количество строк, считанных из всех таблиц и табличных функций, участвующих в запросе.
-- read_bytes (UInt64) — общее количество байтов, считанных из всех таблиц и табличных функций, участвующих в запросе.
-- result_rows (UInt64) — количество строк в результате запроса SELECT или количество строк в запросе INSERT.
-- result_bytes (UInt64) — объём RAM в байтах, использованный для хранения результата запроса.
-- memory_usage (UInt64) — потребление RAM запросом.
-- query_duration_ms (UInt64) — длительность выполнения запроса в миллисекундах.


------- ФИКСЫ ПРОБЛЕМ (сюда заходим толькот при наличии проблем)
---- неверное логирование в system.query_log. Бывает так, что логирование работает неверно. Такая проблема присутствует в версии 
---- 24.5.3.5 - в партиционированных таблицах неверно логируются все метрики, будто бы не дожидается, когда получит результат
---- обработки каждой партиции. Фиксится оборачиванием тестируемого запроса в конструкцию
---- create table test engine=MergeTree order by OrderID as (и вот тут тестируемый запрос)
---- получается так, что сначала результат обработки всех партиций собирается воедино, и лишь потом записывается в таблицу test. При таком
---- запросе логирование происходит правильно






------- КОЛОНОЧНОЕ ХРАНЕНИЕ
-- убеждаемся, что хранение действительно колоночное

-- отбираем все колонки
select * from orders_good;

-- смотрим потребление ресурсов
select query, read_rows, read_bytes, result_rows, result_bytes, memory_usage, query_duration_ms 
from system.query_log
where query ilike '%from orders_good%'
	and query not ilike '%system%'
	and type = 2
;

-- очищаем таблицу, в которой хранится информация обо всех сделанных запросах
alter table system.query_log delete where 1;

-- отбираем лишь одну колонку
select OrderID from orders_good;

-- смотрим потребление ресурсов
select query, read_rows, read_bytes, result_rows, result_bytes, memory_usage, query_duration_ms 
from system.query_log
where query ilike '%orders_good%'
	and query not ilike '%system%'
	and type = 2
;

-- результаты сравнения
--12963 5808315 12963 5811631 20976214 35  - select *
--12963 259260  12963 259515  3937497  3   - select OrderID

-- видим многократную разницу, по чтению с диска - потребление снизилось в 22 раза ,
-- по оперативной памяти - в 5 раз, по скорости выполнения запроса - в 15 раз

-- очищаем таблицу, в которой хранится информация обо всех сделанных запросах
alter table system.query_log delete where 1;

------- КОЛОНОЧНОЕ ХРАНЕНИЕ\





------- LIMIT
---- первый кейс - с гранулярностью индекса 1

-- отбираем все колонки без limit
select * from orders_good;

-- смотрим потребление ресурсов
select query, read_rows, read_bytes, result_rows, result_bytes, memory_usage, query_duration_ms 
from system.query_log
where query ilike '%from orders_good%'
	and query not ilike '%system%'
	and type = 2
;

-- очищаем таблицу, в которой хранится информация обо всех сделанных запросах
alter table system.query_log delete where 1;

-- отбираем все колонки с limit
select * from orders_good limit 1;

-- смотрим потребление ресурсов
select query, read_rows, read_bytes, result_rows, result_bytes, memory_usage, query_duration_ms 
from system.query_log
where query ilike '%orders_good%'
	and query not ilike '%system%'
	and type = 2
;

-- очищаем таблицу, в которой хранится информация обо всех сделанных запросах
alter table system.query_log delete where 1;

-- отбираем ОДНУ колонку с limit
select DeliveryType from orders_good limit 1;

-- смотрим потребление ресурсов
select query, read_rows, read_bytes, result_rows, result_bytes, memory_usage, query_duration_ms 
from system.query_log
where query ilike '%orders_good%'
	and query not ilike '%system%'
	and type = 2
;

-- результаты сравнения
-- 12963 5808315 12963 5811631 20080798 436  - * no limit 
-- 1     443     1     3759    4192413  36   - * limit
-- 1     25      1     275     4196305  25   - OrderID limit

-- видим многократную разницу по всем ресурсам, очевидно, что чем меньше строк мы выводим - тем меньше потребление ресурсов.
-- более того, ClickHouse даже понимает, что достаточно считать с диска одну строку - это зависит от гранулярности индекса
-- и более того, внутри одной строки ClickHouse может считать лишь одну колонку - фактически, мы получили уровень хранения - ячейка

-- очищаем таблицу, в которой хранится информация обо всех сделанных запросах
alter table system.query_log delete where 1;



---- второй кейс - с гранулярностью индекса 8192

-- отбираем все колонки без limit
select * from orders_good;

-- смотрим потребление ресурсов
select query, read_rows, read_bytes, result_rows, result_bytes, memory_usage, query_duration_ms 
from system.query_log
where query ilike '%from orders_good%'
	and query not ilike '%system%'
	and type = 2
;

-- отбираем все колонки с limit
select * from orders_good limit 1;

-- смотрим потребление ресурсов
select query, read_rows, read_bytes, result_rows, result_bytes, memory_usage, query_duration_ms 
from system.query_log
where query ilike '%orders_good%'
	and query not ilike '%system%'
	and type = 2
;

-- результаты сравнения
-- 12963 5808315 12963 5811631 20583390 342  - no limit
-- 8192  3668208 1     6656    8391728  8    - limit

-- видим разницу по всем ресурсам, но уже не такую существенную. Это вопрос ПРАВИЛЬНОГО выбора индекса (и будет еще кейс в секции WHERE)

-- очищаем таблицу, в которой хранится информация обо всех сделанных запросах
alter table system.query_log delete where 1;

-- ИТОГОВЫЙ ВЫВОД - примение LIMIT всегда положительно влияет на потребление ресурсов.

------- LIMIT\




------- WHERE
---- первый кейс - where по колонке, которая не является ключом сортировки (индекс)/партиционирования, и без where. Поле из WHERE добавлять
---- в SELECT не будет

-- отбираем лишь одну колонку без where
select OrderID from orders_good;

-- смотрим потребление ресурсов
select query, read_rows, read_bytes, result_rows, result_bytes, memory_usage, query_duration_ms 
from system.query_log
where query ilike '%orders_good%'
	and query not ilike '%system%'
	and type = 2
;

-- отбираем лишь одну колонку c where
select OrderID from orders_good where ClientGender = 'Мужчина';

-- смотрим потребление ресурсов
select query, read_rows, read_bytes, result_rows, result_bytes, memory_usage, query_duration_ms 
from system.query_log
where query ilike '%orders_good%'
	and query not ilike '%system%'
	and type = 2
;

-- результаты сравнения
--12963 259260 12963 259515 259515   2  - not where
--12963 557297 9988  200015 3997813  3  - where

-- очищаем таблицу, в которой хранится информация обо всех сделанных запросах
alter table system.query_log delete where 1;

-- делаем вывод, что фильтрация по колонке, которая не является ключом сортировки/партиционирования, уменьшает итоговую выборку, но
-- увеличивает потребление других ресурсов.
-- Это происходит потому, что в запросе без фильтрации никаких манипуляций с колонкой ClientGender
-- не производилось, она даже не считывалась с диска. А во втором запросе, за счет фильтрации, итоговая выборка стала меньше.
-- Ради честности теста, очевидно, колонку ClientGender нужно включить в итоговую выборку.



---- второй кейс - where по колонке, которая не является ключом сортировки/партиционирования, и без where. Поле из WHERE добавим
---- в SELECT

-- отбираем OrderID, ClientGender без where
select OrderID, ProductName from orders_good;

-- смотрим потребление ресурсов
select query, read_rows, read_bytes, result_rows, result_bytes, memory_usage, query_duration_ms 
from system.query_log
where query ilike '%orders_good%'
	and query not ilike '%system%'
	and type = 2
;

-- отбираем OrderID, ClientGender c where
select OrderID, ProductName from orders_good where ProductName = 'Выпрямитель для волос';

-- смотрим потребление ресурсов
select query, read_rows, read_bytes, result_rows, result_bytes, memory_usage, query_duration_ms 
from system.query_log
where query ilike '%orders_good%'
	and query not ilike '%system%'
	and type = 2
;

-- результаты сравнения
--12963 1007639 12963 1008149 3741254  99  - not where
--12963 757559  459   32181   4165575  4  - where

-- очищаем таблицу, в которой хранится информация обо всех сделанных запросах
alter table system.query_log delete where 1;

-- делаем вывод, что в данном случае экономия ресурсов происходит за счет уменьшения итоговой выборки. С секцией where лишь немного
-- увеличено потребление оперативной памяти - это затраты на обработку считанной колонки. Фактически - "стоимость" операции where



---- третий кейс - where по колонке, которая является ключом сортировки, и без where. Поле из WHERE добавим в SELECT. Индекс сделаем
---- разреженным - 8192

-- разреженный индекс с гранулярностью 8192 означает, что проиндексировано будет лишь каждое 8192 значение колонки.

-- отбираем OrderID, ClientGender без where - полный аналог предыдущего кейса
select OrderID, ClientGender from orders_good;

-- смотрим потребление ресурсов
select query, read_rows, read_bytes, result_rows, result_bytes, memory_usage, query_duration_ms 
from system.query_log
where query ilike '%orders_good%'
	and query not ilike '%system%'
	and type = 2
;

-- отбираем OrderID, ClientGender c where
select OrderID, ClientGender from orders_good where OrderID in ('2017-000216', '2017-255215', '2017-531062', '2018-975519');

-- смотрим потребление ресурсов
select query, read_rows, read_bytes, result_rows, result_bytes, memory_usage, query_duration_ms 
from system.query_log
where query ilike '%orders_good%'
	and query not ilike '%system%'
	and type = 2
;

-- результаты сравнения
--12963 557297 12963 557807 3639613  5  - not where
--12963 557297 10    940    4196048  5  - where

-- очищаем таблицу, в которой хранится информация обо всех сделанных запросах
alter table system.query_log delete where 1;

-- делаем вывод, что в данном случае экономия ресурсов происходит за счет уменьшения итоговой выборки, С секцией where 
-- уже побольше, чем в предыдущем кейсе,
-- увеличено потребление оперативной памяти - это "стоимость" операции where по колонке, являющейся индексом. Сам же индекс в ClickHouse  
-- хранится частично на жестком диске, частично в оперативной памяти - поэтому и происходит увеличение потребления относительно
-- предыдущего кейса



---- четвертый кейс - where по колонке, которая является ключом сортировки, и без where. Поле из WHERE добавим в SELECT. Индекс сделаем
---- максимально НЕ сжатым - 1


-- отбираем OrderID, ClientGender без where - полный аналог предыдущего кейса
select OrderID, ClientGender from orders_good;

-- смотрим потребление ресурсов
select query, read_rows, read_bytes, result_rows, result_bytes, memory_usage, query_duration_ms 
from system.query_log
where query ilike '%orders_good%'
	and query not ilike '%system%'
	and type = 2
;

-- отбираем OrderID, ClientGender c where
select OrderID, ClientGender from orders_good where OrderID in ('2017-000216', '2017-255215', '2017-531062', '2018-975519');

-- смотрим потребление ресурсов
select query, read_rows, read_bytes, result_rows, result_bytes, memory_usage, query_duration_ms 
from system.query_log
where query ilike '%orders_good%'
	and query not ilike '%system%'
	and type = 2
;

-- результаты сравнения
--12963 557297 12963 557807 3639117  101  - not where
--14    602    10    940    4197040  3    - where

-- очищаем таблицу, в которой хранится информация обо всех сделанных запросах
alter table system.query_log delete where 1;

-- делаем вывод, что ПРАВИЛЬНО подобранный индекс максимально оптимизует потребление всех ресурсов, затрачивая при этом лишь
-- оперативную память. Отсюда вывод - максимально полезно с точки зрения потребления ресурсов делать фильтрацию по ключу сортировки.
-- при выборе гранулярности индекса стоит учитывать размер таблицы, на больших данных (миллиарды строк) рекоммендован индекс 8192.
-- на малых же таблицах гранулярность можно уменьшать.
-- Само же применение фильтрации по индексу уменьшит потребление ресурсов в любой БД, не только ClickHouse.



---- пятый кейс - where по колонке, которая является ключом партиционирования

-- создаем таблицу, полностью аналогичную orders_good, только добавим партиционирование по дате
create table default.orders_good_parts engine=MergeTree order by OrderID partition by toYYYYMM(OrderDate) settings index_granularity = 1 as 
select * from orders_good
;

SELECT toYYYYMM(OrderDate) YYMM, OrderDate dt_date FROM orders_good ORDER BY YYMM;
SELECT toYYYYMM(OrderDate) YYMM, count() dt_date FROM orders_good GROUP BY YYMM ORDER BY YYMM;

-- ищем, по какому пути находится физически хранение таблицы, нужно поле path
select * from system.parts where active=1 and table = 'orders_good_parts';

-- видим много партиций - это физически независмые куски данных одной таблицы. Как вырванный листок из тетрадки.
-- внутри одной партиции будут исключительно одинаковые значения toYYYYMM(OrderDate) (можно проверить запросом
-- select distinct toYYYYMM(OrderDate) from orders_good_parts - будет те же 12 строк, что эквивалентно количеству партиций).
-- партиционирование необходимо как раз для экономии ресурсов, ну и чтобы не потерять все данные таблицы разом.
-- уже очевидно, что фильтрация по ключу партиционирования максимально уменьшит потребление ресурсов, так как партиции,
-- не подходящие по условию, не будут даже считываться с диска.

-- отбираем OrderID, ClientGender без where - полный аналог предыдущего кейса, но здесь обнаружена ошибка логирования,
-- поэтому обернем весь запрос в конструкцию create table
create table test engine=MergeTree order by OrderID as (select OrderID, ClientGender from orders_good_parts);


-- смотрим потребление ресурсов
select query, read_rows, read_bytes, result_rows, result_bytes, memory_usage, query_duration_ms 
from system.query_log
where query ilike '%orders_good_parts%'
	and query not ilike '%system%'
	and type = 2
;

-- удаляем таблицу test
drop table if exists test;

-- отбираем OrderID, ClientGender c where по полю партиционирования, но здесь обнаружена ошибка логирования,
-- поэтому обернем весь запрос в конструкцию create table
create table test engine=MergeTree order by OrderID as (select OrderID, ClientGender from orders_good_parts where OrderDate = '2019-01-30');

-- смотрим потребление ресурсов
select query, read_rows, read_bytes, result_rows, result_bytes, memory_usage, query_duration_ms 
from system.query_log
where query ilike '%orders_good_parts%'
	and query not ilike '%system%'
	and type = 2
;

-- удаляем таблицу test
drop table if exists test;

-- результаты сравнения
-- 12963 557297 12963 557297 4468597  187  - not where
-- 772   2447   21    903    960      51   - where

-- видим, очевидно, многократное снижение потребления ресурсов. Видим, что с диска было прочитано 772 строки. Давайте убедимся, что это
-- размер партиции, в которой хранятся все строки, у которых OrderDate = '2019-01-30'. В этом нам поможет магическое поле _partition_id - 
-- заполняется автоматически, это идентификатор партиции, в которой хранится строка.

-- берем идентификатор партиции
select OrderID, ClientGender, _partition_id from orders_good_parts where OrderDate = '2019-01-30';

--смотрим путь, где на диске хранится партиция, потом идем в докер смотреть на файл count.txt - первый способ убедиться, 772 строки
select * from `system`.parts where partition_id = '201901';

-- второй способ - запросить все записи, у которых необходимый _partition_id - на выходе те же 772 строки
select * from orders_good_parts where _partition_id = '201901';

-- очищаем таблицу, в которой хранится информация обо всех сделанных запросах
alter table system.query_log delete where 1;

-- делаем вывод, что фильтрация по полю партиционирования максимально оптимизует потребление всех ресурсов.


-- ИТОГОВЫЙ ВЫВОД - примение фильтрации всегда положительно влияет на потребление ресурсов. Но, ради чего все делалось - 
-- оптимальное использование фильтрации выглядит так:
-- сначала фильтр по ключу партиционирования, затем по ключу сортировки, затем все остальные (при необходимости и наличии, конечно же).
-- ClickHouse сам анализирует ваш запрос, и, если видит фильтр по ключу партиционирования НЕ первым в секции WHERE - то под капотом он
-- все равно выполнит его первым. Но, по правилам хорошего тона, все равно рекоммендуется соблюдать порядок фильтрации

EXPLAIN SYNTAX -- маленький спойлер, не смог удержаться
SELECT *
FROM orders_good_parts ogp 
WHERE OrderDate = '2019-01-30'
    AND OrderID = '2017-000268'
    AND ClientGender = 'мужчина'
FORMAT TSV
;

------- WHERE\





------- HAVING
---- первый кейс - с having и без

-- count() без having
select OrderDate dt, count() from orders_good_parts group by OrderDate;

-- смотрим потребление ресурсов
select query, read_rows, read_bytes, result_rows, result_bytes, memory_usage, query_duration_ms 
from system.query_log
where query ilike '%from orders_good_parts%'
	and type = 2
order by event_time desc
;

-- count() c having по одному месяцу 2019-03
select OrderDate dt, count() from orders_good_parts group by OrderDate having toYYYYMM(OrderDate) = '201903';

-- смотрим потребление ресурсов
select query, read_rows, read_bytes, result_rows, result_bytes, memory_usage, query_duration_ms 
from system.query_log
where query ilike '%orders_good_parts%'
	and query not ilike '%system%'
	and type = 2
;

-- результаты сравнения
-- 12963 25926 365 3916 5221516 43 - not having
-- 921   1842  31  568  4222340 8  - having

-- видим очевидное - потребление ресурсов снизилось. Но правильно ли мы все сделали? - и да и нет

-- очищаем таблицу, в которой хранится информация обо всех сделанных запросах
alter table system.query_log delete where 1;



---- второй кейс - HAVING vs WHERE

-- count() c having по одному месяцу 2019-03
select OrderDate dt, count() from orders_good_parts group by OrderDate having toYYYYMM(OrderDate) = '201903';

-- смотрим потребление ресурсов
select query, read_rows, read_bytes, result_rows, result_bytes, memory_usage, query_duration_ms 
from system.query_log
where query ilike '%orders_good_parts%'
	and query not ilike '%system%'
	and type = 2
;

-- count() c where по одному месяцу 2019-03
select OrderDate dt, count() from orders_good_parts where toYYYYMM(OrderDate) = '201903' group by OrderDate;

-- смотрим потребление ресурсов
select query, read_rows, read_bytes, result_rows, result_bytes, memory_usage, query_duration_ms 
from system.query_log
where query ilike '%orders_good_parts%'
	and query not ilike '%system%'
	and type = 2
;

-- результаты сравнения
-- 921   1842  31  568  4221212 2  - having
-- 921   1842  31  568  4221948 2  - where

-- видим абсолютно одинаковый результат, но это лишь потому, что оптимизатор запросов ClichHouse работает отлично. Хорошей практикой 
-- все равно считается применение фильтрации до аггрегации. HAVING же нужен, чтобы фильтровать по результатам аггрегации. Отсюда
-- плавно переходим к explain.

-- очищаем таблицу, в которой хранится информация обо всех сделанных запросах
alter table system.query_log delete where 1;

------- HAVING\





------- EXPLAIN
---- EXPLAIN - позволяет посмотреть планы выполнения запросов, ситаксические преобразования и т.д. Продвинутый уровень.
---- EXPLAIN SYNTAX - как выглядит запрос после преобразований оптимизатора ClickHouse

EXPLAIN SYNTAX select OrderDate dt, count() from orders_good_parts group by OrderDate having toYYYYMM(OrderDate) = '201903' FORMAT TSV;

EXPLAIN SYNTAX select OrderDate dt, count() from orders_good_parts where toYYYYMM(OrderDate) = '201903' group by OrderDate FORMAT TSV;

-- видим, что оказывается, ClickHouse проанализировал запрос и понял, что условие из секции HAVING выгоднее переместить
-- в секцию WHERE, и фактически на сервере выполнился именно тот запрос, что выводится методом EXPLAIN SYNTAX! Именно поэтому
-- в предыдущем пункте мы видели абсолютно одинаковые результаты по потреблению ресурсов.



---- EXPLAIN PLAN - показывает план выполнения запроса. Для читабельности необходимы настройки

EXPLAIN PLAN json = 1, indexes = 1, description = 1
select OrderDate dt, count() from orders_good_parts group by OrderDate having toYYYYMM(OrderDate) = '201903';

/*
[
  {
    "Plan": {
      "Node Type": "Expression",
      "Description": "(Project names + (Projection + ))",
      "Plans": [
        {
          "Node Type": "Aggregating",
          "Plans": [
            {
              "Node Type": "Filter",
              "Description": "( + (Before GROUP BY + Change column names to column identifiers))",
              "Plans": [
                {
                  "Node Type": "ReadFromMergeTree",
                  "Description": "default.orders_good_parts",
                  "Indexes": [
                    {
                      "Type": "MinMax",
                      "Keys": ["OrderDate"],
                      "Condition": "(toYYYYMM(OrderDate) in [201903, 201903])",
                      "Initial Parts": 12,
                      "Selected Parts": 1,
                      "Initial Granules": 12963,
                      "Selected Granules": 921
                    },
                    {
                      "Type": "Partition",
                      "Keys": ["toYYYYMM(OrderDate)"],
                      "Condition": "(toYYYYMM(OrderDate) in [201903, 201903])",
                      "Initial Parts": 1,
                      "Selected Parts": 1,
                      "Initial Granules": 921,
                      "Selected Granules": 921
                    },
                    {
                      "Type": "PrimaryKey",
                      "Condition": "true",
                      "Initial Parts": 1,
                      "Selected Parts": 1,
                      "Initial Granules": 921,
                      "Selected Granules": 921
                    }
                  ]
                }
              ]
            }
          ]
        }
      ]
    }
  }
]
*/

EXPLAIN PLAN json = 1, indexes = 1, description = 1
select OrderDate dt, count() from orders_good_parts where toYYYYMM(OrderDate) = '201903' group by OrderDate;

/*
[
  {
    "Plan": {
      "Node Type": "Expression",
      "Description": "(Project names + Projection)",
      "Plans": [
        {
          "Node Type": "Aggregating",
          "Plans": [
            {
              "Node Type": "Expression",
              "Description": "Before GROUP BY",
              "Plans": [
                {
                  "Node Type": "Filter",
                  "Description": "(WHERE + Change column names to column identifiers)",
                  "Plans": [
                    {
                      "Node Type": "ReadFromMergeTree",
                      "Description": "default.orders_good_parts",
                      "Indexes": [
                        {
                          "Type": "MinMax",
                          "Keys": ["OrderDate"],
                          "Condition": "(toYYYYMM(OrderDate) in [201903, 201903])",
                          "Initial Parts": 12,
                          "Selected Parts": 1,
                          "Initial Granules": 12963,
                          "Selected Granules": 921
                        },
                        {
                          "Type": "Partition",
                          "Keys": ["toYYYYMM(OrderDate)"],
                          "Condition": "(toYYYYMM(OrderDate) in [201903, 201903])",
                          "Initial Parts": 1,
                          "Selected Parts": 1,
                          "Initial Granules": 921,
                          "Selected Granules": 921
                        },
                        {
                          "Type": "PrimaryKey",
                          "Condition": "true",
                          "Initial Parts": 1,
                          "Selected Parts": 1,
                          "Initial Granules": 921,
                          "Selected Granules": 921
                        }
                      ]
                    }
                  ]
                }
              ]
            }
          ]
        }
      ]
    }
  }
]
*/

-- видим, что планы почти одинаковые, только в случае с HAVING секции Expression и Filter объединены.
-- Самое важное, что нам показывает EXPLAIN PLAN - это сколько партиций и гранул будет прочитано. Эти показатели всегда нужно
-- стремиться минимизировать.



---- EXPLAIN ESTIMATE
-- позволяет отдельно вывести потребление ресурсов, что не совсем удобно, но возможность такая есть
EXPLAIN ESTIMATE select OrderDate dt, count() from orders_good_parts where toYYYYMM(OrderDate) = '201903' group by OrderDate;


-- Рассматривать остальные варианты не будем, так как они менее полезны.

------- EXPLAIN\





------- JOIN
------- В случае с JOIN нужно понимать, как работает под капотом каждый из видов соединений. И, в зависимости от задачи, выбирать 
------- наименее ресурсный тип соединения. Рассмотрим несколько примеров
---- кейс 1 - INNER vs LEFT 

-- left
create table test engine=MergeTree order by OrderID as (
select * from orders_good_parts t1
left join 
(
	select * 
	from district
	limit 2000 -- так как таблицы имеют одинаковые OrderID разницы мы не увидим, нужно, чтобы таблицы были разного размера
) t2 
on t1.OrderID=t2.OrderID);

-- смотрим потребление ресурсов
select query, read_rows, read_bytes, result_rows, result_bytes, memory_usage, query_duration_ms 
from system.query_log
where query ilike '%from orders_good%'
	and type = 2
;

-- удаляем таблицу test
drop table if exists test;

-- inner
create table test engine=MergeTree order by OrderID as (
select * from orders_good_parts t1
inner join 
(
	select * 
	from district
	limit 2000 -- так как таблицы имеют одинаковые OrderID разницы мы не увидим, нужно, чтобы таблицы были разного размера
) t2 
on t1.OrderID=t2.OrderID);

-- смотрим потребление ресурсов
select query, read_rows, read_bytes, result_rows, result_bytes, memory_usage, query_duration_ms 
from system.query_log
where query ilike '%orders_good%'
	and query not ilike '%system%'
	and type = 2
;

-- удаляем таблицу test
drop table if exists test;

-- результаты сравнения
-- 14963 5911605 16553 7903729 22652103 256  - left
-- 14963 5911605 5590  2794422 6306669  201  - inner

-- видим разницу, очевидно, inner потребляет меньше ресурсов, так как не выводит ту часть левой таблицы, у которой
-- не нашлось совпадений в правой. Поэтому, по возможности следует применять именно inner join.

-- очищаем таблицу, в которой хранится информация обо всех сделанных запросах
alter table system.query_log delete where 1;



---- кейс 2 - LEFT vs LEFT ANY vs INNER
---- LEFT ANY - специфичный тип соединения, при котором к левой таблице присоединяется первое найденное совпадение правой таблицы.
---- результаты LEFT и INNER у нас уже есть, поэтому сделаем LEFT ANY и сравним

-- left any
create table test engine=MergeTree order by OrderID as (
select * from orders_good_parts t1
left any join 
(
	select * 
	from district
	limit 2000 -- так как таблицы имеют одинаковые OrderID разницы мы не увидим, нужно, чтобы таблицы были разного размера
) t2 
on t1.OrderID=t2.OrderID);

-- смотрим потребление ресурсов
select query, read_rows, read_bytes, result_rows, result_bytes, memory_usage, query_duration_ms 
from system.query_log
where query ilike '%from orders_good%'
	and query not ilike '%system%'
	and type = 2
;

-- удаляем таблицу test
drop table if exists test;

-- результаты сравнения
-- 14963 5911605 16553 7903729 23656820 163  - left
-- 14963 5911605 5590  2794422 5135518  153  - inner
-- 14963 5911605 12963 6108939 14953328 215  - left any

-- видим, что inner лидирует по всем показателям. Но, есть новый вывод, если нас устраивает присоединение только одного первого вхождения 
-- из правой таблицы - left any join однозначно лучше, чем left join. Но, есть еще один джойн

-- очищаем таблицу, в которой хранится информация обо всех сделанных запросах
alter table system.query_log delete where 1;



---- кейс 3 - LEFT vs LEFT ANY vs INNER VS LEFT SEMI
---- SEMI - специфичный тип соединения, который возвращает значения столбцов для каждой строки из левой таблицы, которая имеет 
---- хотя бы одно совпадение ключа соединения в правой таблице. Возвращается только первое найденное совпадение 
---- (декартово произведение отключено). Добавим SEMI к уже имеющимся результатам и сравним

-- LEFT SEMI JOIN 
create table test engine=MergeTree order by OrderID as (
select * from orders_good_parts t1
left semi join 
(
	select * 
	from district
	limit 2000 -- так как таблицы имеют одинаковые OrderID разницы мы не увидим, нужно, чтобы таблицы были разного размера
) t2 
on t1.OrderID=t2.OrderID);

-- смотрим потребление ресурсов
select query, read_rows, read_bytes, result_rows, result_bytes, memory_usage, query_duration_ms 
from system.query_log
where query ilike '%from orders_good%'
	and query not ilike '%system%'
	and type = 2
;

-- удаляем таблицу test
drop table if exists test;

-- результаты сравнения
-- 14963 5911605 16553 7903729 23656820 163  - left
-- 14963 5911605 5590  2794422 5135518  153  - inner
-- 14963 5911605 12963 6108939 15006856 153  - left any
-- 14963 5911605 2000  999632  4430047  233  - left semi

-- видим, что left semi лидирует по всем показателям. Если есть возможность использовать его - нужно использовать его.

-- очищаем таблицу, в которой хранится информация обо всех сделанных запросах
alter table system.query_log delete where 1;


------- JOIN\





------- РЕЗЮМИРУЕМ ПЕРЕД ЗАКЛЮЧЕНИЕМ

-- И так, мы разобрали основные способы снизить потребление ресурсов одиночных запросов (в которых один оператор FROM,
-- JOIN - исключения, которые сейчас и будем разбирать). Классический запрос имеет вид SELECT - FROM - WHERE - GROUP BY - HAVING - 
-- ORDER BY - LIMIT - OFFSET. И принципы оптимизации можно изложить следующие (общие для всех БД, не только ClickHouse):
--     1) Стараемся не использовать select * 
--     2) Активно пользуемся where в приоритете: по ключу партиционирования - по ключу сортировки - затем остальные условия
--     3) Активно пользуемся limit 
--     4) Подбираем правильно ключи партиционирования/сортировки
--     5) Активно пользуемся having
--     6) Не откладывай на having то, что можно сделать в where
--     7) EXPLAIN - ваш друг, активно пользуемся
--     8) Подбираем правильный тип соединения (JOIN-ы)
-- А теперь приступим к разбору JOIN-а и найдем еще один способ оптимизации

------- РЕЗЮМИРУЕМ ПЕРЕД ЗАКЛЮЧЕНИЕМ\





------- Порядок выполнения кода sql-сервером
------- Важно понимать, в каком именно порядке написанный вами запрос исполняется на сервере. Так вышло, что он не соответствует
------- порядку написания операторов в sql-коде. И так, порядок именно ИСПОЛНЕНИЯ кода таков:
------- FROM — оператор определяет источники данных для запроса. Сначала сервер ищет данные в указанных источниках.
------- PREWHERE — специфический оператор ClickHouse, сейчас разберем
------- JOIN — опертор соединения данных (гипотетически проблемное место, которое сейчас разберем)
------- WHERE — этот оператор фильтрует строки, полученные из источника. Сервер применяет условия WHERE после того (!!!), как получены все данные из FROM/JOIN.
------- GROUP BY — если запрос содержит GROUP BY, сервер группирует строки по указанным столбцам после применения условий WHERE.
------- HAVING — сервер применяет условия HAVING после группировки строк (если есть GROUP BY).
------- SELECT — сервер выбирает указанные столбцы и выполняет вычисления на них.
------- DISTINCT — если DISTINCT присутствует, сервер удаляет дубликаты строк из результата.
------- ORDER BY — сервер сортирует результаты по указанным столбцам.
------- LIMIT — сервер ограничивает количество возвращаемых строк.
---- Представленный порядок - лишь примерный, так как существует главная фича - оптимизатор запросов. Мы уже убеждались, что добавив
---- LIMIT 1, при гранулярности индекса равным единице, мы считывали с диска всего одну строку. Это благодаря оптимизатору - он понимал,
---- что нет смысла считывать больше. Оптимизатор постоянно улучшается, в каждом релизе добавляется что-то новое. Поэтому, наиграть 
---- проблему не получится, но, делать нужно все правильно, не полагаясь на оптимизатор.

---- И так, проблему поправили в релизе 24.4 - вот дока, где конкретно расписано, что сделали и зачем
---- https://clickhouse.com/blog/clickhouse-release-24-04#join-performance-improvements

---- Официальная дока клика про секцию JOIN
---- https://clickhouse.com/docs/ru/sql-reference/statements/select/join#syntax-limitations

select version(); -- мы уже далеко после рокового релиза

-- создадим district_good с нормальным движком MergeTree, в данном случае это очень важно - гранулярность
create table default.district_good engine=MergeTree order by OrderID settings index_granularity = 1 as 
select * from default.district 
;

---- кейс 1 - сначала джойним, потом фильтруем. И сначала фильтруем, а затем джойним
---- Задача классическая adhoc - вывести информацию по заказу 2017-022268, включая район доставки (таблица district)

create table test engine=MergeTree order by OrderID as 
explain plan json = 1, indexes = 1, description = 1 -- смотри план, все станет ясно
(
	select * 
	from orders_good_parts t1
	left join district_good t2 
	on t1.OrderID=t2.OrderID
	where t1.OrderID = '2017-022268' -- фильтруем после джойна
);



-- смотрим потребление ресурсов
select query, read_rows, read_bytes, result_rows, result_bytes, memory_usage, query_duration_ms 
from system.query_log
where query ilike '%from orders_good%'
	and query not ilike '%system%'
	and type = 2
;

-- удаляем таблицу test
drop table if exists test;



create table test engine=MergeTree order by OrderID as 
--explain plan json = 1, indexes = 1, description = 1 -- смотри план, все станет ясно
(
	select * from 
	(
		select * 
		from orders_good_parts 
		where OrderID = '2017-022268' -- фильтруем до джойна
	) t1
	left join 
	(
		select * 
		from district_good -- а вот тут пропустим специально фильтр
	) t2 
	on t1.OrderID=t2.OrderID
);

-- смотрим потребление ресурсов
select query, read_rows, read_bytes, result_rows, result_bytes, memory_usage, query_duration_ms 
from system.query_log
where query ilike '%from orders_good%'
	and query not ilike '%system%'
	and type = 2
;

-- удаляем таблицу test
drop table if exists test;



--create table test engine=MergeTree order by OrderID as 
explain plan json = 1, indexes = 1, description = 1 -- смотри план, все станет ясно
(
	select * from 
	(
		select * 
		from orders_good_parts 
		where OrderID = '2017-022268' -- фильтруем до джойна
	) t1
	left join 
	(
		select * 
		from district_good
		where OrderID = '2017-022268' -- фильтруем до джойна
	) t2 
	on t1.OrderID=t2.OrderID
);

-- смотрим потребление ресурсов
select query, read_rows, read_bytes, result_rows, result_bytes, memory_usage, query_duration_ms 
from system.query_log
where query ilike '%from orders_good%'
	and query not ilike '%system%'
	and type = 2
;

-- удаляем таблицу test
drop table if exists test;


-- результаты сравнения
-- 22 3252 25 12590 114287 237          - после джойна
-- 12979 670364 25  12590 2339034  221  - до джойна только в левой части
-- 22 3252 25 12590 116031 221          - до джойна в обеих частях

-- 

-- очищаем таблицу, в которой хранится информация обо всех сделанных запросах
alter table system.query_log delete where 1;



------- Порядок выполнения кода sql-сервером\







------- ДЕКОМПОЗИЦИЯ
---- Далеко не все запросы будут простыми. Вернее, лишь некоторые. В аналитике подавляющее большинство запросов будут сложными, с кучей
---- соединений, подзапросов и т.д. Поэтому, необходимо знать еще один способ оптимизации - декомпозиция. Способ очень простой - 
---- дробим сложный запрос на более мелкие. НО! Не меньше, чем классический запрос, рассмотренный выше. Нельзя дробить запрос вида
---- select * from ... where ... group by ... having ... - это не приведет к уменьшению потребления ресурсов

-- немного усложним left semi join из примера выше
--create table test engine=MergeTree order by OrderID as (
--explain plan --json = 1, indexes = 1, description = 1
select *
from
(
	select * 
	from orders_good_parts
	where ProductBrand = 'Хлеб-соль'
		and ProductSubcategory = 'Кухонные товары'
		and ProductCategory = 'Бытовые товары'
		and DeliveryType = 'Самовывоз'
		and PaymentType = 'Наличные'
) t1
left semi join 
(
	select OrderID, concat('район доставки: ', DeliveryDistrictName) DeliveryDistrictName
	from district
	limit 2000
) t2 
on t1.OrderID=t2.OrderID
;


-- мы добавили в левую часть много условий не по ключу сортировки/партиционирования. В правую также добавили concat. Напрашивается
-- фильтрация в левую часть такая, чтобы отбирать только такие OrderID, которые будут в правой части. Также стоит учитывать, 
-- что и левая и правая часть джойна будут выполняться одновременно - это можно увидеть с помощью explain.
-- В некоторых случаях, это может быть опасно, так как на сервер пойдет высокая разовая нагрузка. И, способ
-- избавиться от всех проблем - декомпозировать

-- создаем первую временную таблицу, состоящую из запроса правой части, так как его результат пойдет в левую часть.
create temporary table t1 as
select OrderID, concat('район доставки: ', DeliveryDistrictName) DeliveryDistrictName
from district
limit 2000 -- вот эти 2000 заказов мы и хотим отфильтровать в левой части джойна
;

-- создаем вторую временную таблицу, состоящую из запроса левой части с фильтрацией по OrderID.
create temporary table t2 as
select * 
from orders_good_parts
where OrderID in (select OrderID from t1) -- вот условие фильтрации, которое позволит снизить потребление
	and ProductBrand = 'Хлеб-соль'
	and ProductSubcategory = 'Кухонные товары'
	and ProductCategory = 'Бытовые товары'
	and DeliveryType = 'Самовывоз'
	and PaymentType = 'Наличные'
;

-- создаем результирующую таблицу
create table test engine=MergeTree order by OrderID as
select * from t1 left semi join t2 on t1.OrderID = t2.OrderID
;

SELECT * FROM test;

-- удаляем таблицу test
drop table if exists test;

-- очищаем таблицу, в которой хранится информация обо всех сделанных запросах
alter table system.query_log delete where 1;

-- и вот здесь уже важно не сколько сравнение потребления ресурсов, сколько то, что мы сделали последовательными запросы
-- правой и левой части джойна, разложив их по временным таблицам.
-- ИТОГ: декомпозировать нужно лишь тогда, когда это необходимо. Не нужно раскладывать запрос на части просто так, "потому что умею".
-- Но, это очень сильный инструмент при необходимости снизить потребление ресурсов.

------- ДЕКОМПОЗИЦИЯ\





------- ОДИН ИСТОЧНИК - ОДНА ОПЕРАЦИЯ ЧТЕНИЯ
---- Все очень просто - не нужно читать одну и ту же таблицу несколько раз в рамках одной задачи, берегите жесткий диск

--explain plan
SELECT ClientGender, count()
FROM orders_good_parts ogp 
WHERE ClientGender = 'Женщина'
GROUP BY ClientGender
UNION ALL 
SELECT ClientGender, count()
FROM orders_good_parts ogp 
WHERE ClientGender = 'Мужчина'
GROUP BY ClientGender
;

-- смотрим потребление ресурсов
select query, read_rows, read_bytes, result_rows, result_bytes, memory_usage, query_duration_ms 
from system.query_log
where query ilike '%from orders_good%'
	and query not ilike '%system%'
	and type = 2
;

-- очищаем таблицу, в которой хранится информация обо всех сделанных запросах
alter table system.query_log delete where 1;

--explain plan
SELECT ClientGender, count()
FROM orders_good_parts ogp
WHERE ClientGender in ('Женщина', 'Мужчина')
GROUP BY ClientGender
;

-- смотрим потребление ресурсов
select query, read_rows, read_bytes, result_rows, result_bytes, memory_usage, query_duration_ms 
from system.query_log
where query ilike '%from orders_good%'
	and query not ilike '%system%'
	and type = 2
;

-- очищаем таблицу, в которой хранится информация обо всех сделанных запросах
alter table system.query_log delete where 1;

-- результаты сравнения
-- 25926 596074 2 1072 5355883 70  - UNION
-- 12963 298037 2 552  4270044 41  - NO UNION


