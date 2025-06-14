-- Data Cleaning

-- Steps in Data Cleaning
-- 1. Remove Duplicates
-- 2. Standardize data
-- 3. Handle Null Values
-- 4. Remove unnecessary columns

select * from train; -- store, dept, date, weekly_sales, IsHoliday
select * from features; -- store, date, temperature, fuel_price, mk1, 2, 3, 4, 5, CPI, Unemployment, IsHoliday
select * from stores; -- store, type, size

-- Counting the records from each table
select count(*) from train; -- 31636
select count(*) from features; -- 7605
select count(*) from stores; -- 45. One type and size for each store

-- Left Joining train, stores on store column
CREATE TABLE train_with_stores AS
(SELECT t.store, t.Dept, s.Type, s.size, t.date , t.Weekly_Sales, t.IsHoliday
FROM train t
LEFT JOIN stores s ON t.store = s.store);

select * from train_with_stores;

-- Left joining train_with_stores with features on store, date, IsHoliday
CREATE TABLE train_clubbed AS
(SELECT tws.store, tws.Dept, tws.Type, tws.size, tws.date , tws.Weekly_Sales, tws.IsHoliday,
 f.temperature, f.fuel_price, f.MarkDown1, f.MarkDown2, f.MarkDown3, f.MarkDown4, f.MarkDown5, f.CPI, f.Unemployment
FROM train_with_stores tws
LEFT JOIN features f ON tws.store = f.store
and tws.date = f.date
and tws.IsHoliday = f.IsHoliday);

select * from train_clubbed;


#creating staging table to work on
CREATE TABLE `train_clubbed_staging` (
  `store` int DEFAULT NULL,
  `Dept` int DEFAULT NULL,
  `Type` text,
  `size` int DEFAULT NULL,
  `date` text,
  `Weekly_Sales` double DEFAULT NULL,
  `IsHoliday` text,
  `temperature` double DEFAULT NULL,
  `fuel_price` double DEFAULT NULL,
  `MarkDown1` text,
  `MarkDown2` text,
  `MarkDown3` text,
  `MarkDown4` text,
  `MarkDown5` text,
  `CPI` double DEFAULT NULL,
  `Unemployment` double DEFAULT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

insert into train_clubbed_staging
select * from train_clubbed;

select * from train_clubbed_staging;

#checking for duplicates
with duplicates_cte as (
select *, row_number() 
over(partition by store, Dept, Type, size, `date`, weekly_Sales, IsHoliday, temperature, fuel_price,
MarkDown1, MarkDown2, MarkDown3, MarkDown4, MarkDown5, CPI, Unemployment) as row_num 
from train_clubbed_staging)
select * from duplicates_cte
where row_num > 1;
-- No records found. Hence no duplicates in the data.
-- If duplicates are found, we usually create a new table instead of a and do the deletion in that table.
-- delete from train_clubbed_staging
-- where row_num > 1;


#standardizing data

-- round CPI to three decimal places and Unemployment to two decimal places
update train_clubbed_staging
set CPI = round(CPI, 3);

update train_clubbed_staging
set Unemployment = round(Unemployment, 2);

update train_clubbed_staging
set fuel_price = round(fuel_price, 2);

#this doesnt change datatype of date
update train_clubbed_staging
set `date` = str_to_date(`date`, '%Y-%m-%d');

#changing date to DATE datatype
alter table train_clubbed_staging
modify column `date` DATE;

#select distinct(IsHoliday) from train_clubbed_staging;

UPDATE train_clubbed_staging
SET IsHoliday = CASE
    WHEN LOWER(IsHoliday) = 'true' THEN '1'
    WHEN LOWER(IsHoliday) = 'false' THEN '0'
    ELSE NULL
END;

ALTER TABLE train_clubbed_staging
MODIFY IsHoliday TINYINT(1);

select * from train_clubbed_staging;


#Handling NA and blanks
 
-- select * from train_clubbed_staging
-- where weekly_sales is null
-- or weekly_sales = "";

-- Apart from markDown columns, none of the other columns have NA values.
-- We can just replace NA's with 0
update train_clubbed_staging
set MarkDown1 = 0
where MarkDown1 = "NA"
or MarkDown1 = '';

update train_clubbed_staging
set MarkDown2 = 0
where MarkDown2 = "NA"
or MarkDown2 = '';

update train_clubbed_staging
set MarkDown3 = 0
where MarkDown3 = "NA"
or MarkDown3 = '';

update train_clubbed_staging
set MarkDown4 = 0
where MarkDown4 = "NA"
or MarkDown4 = '';

update train_clubbed_staging
set MarkDown5 = 0
where MarkDown5 = "NA"
or MarkDown5 = '';

ALTER TABLE train_clubbed_staging
MODIFY MarkDown1 INT,
MODIFY MarkDown2 INT,
MODIFY MarkDown3 INT,
MODIFY MarkDown4 INT,
MODIFY MarkDown5 INT;

select * from train_clubbed_staging
where MarkDown1 <> 0;







