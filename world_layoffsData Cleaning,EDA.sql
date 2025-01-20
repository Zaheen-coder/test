-- SQL Project - Data Cleaning

-- https://www.kaggle.com/datasets/swaptr/layoffs-2022

select * from layoffs;

-- first thing we want to do is create a staging table. This is the one we will work in and clean the data. We want a table with the raw data in case something happens

CREATE TABLE layoffs_staging
LIKE layoffs;

INSERT INTO layoffs_staging
select * from layoffs;

select * from layoffs_staging;

-- now when we are data cleaning we usually follow a few steps
-- 1. check for duplicates and remove any
-- 2. standardize data and fix errors
-- 3. Look at null values and see what 
-- 4. remove any columns and rows that are not necessary - few ways

-- 1. Remove Duplicates

# First let's check for duplicates

SELECT *
FROM (
	SELECT company, industry, total_laid_off,`date`,
		ROW_NUMBER() OVER (
			PARTITION BY company, industry, total_laid_off,`date`
			) AS row_num
	FROM 
		world_layoffs.layoffs_staging
) duplicates
WHERE 
	row_num > 1;

-- let's just look at ola to confirm

select * from layoffs_staging
where company='Ola';

-- it looks like these are all legitimate entries and shouldn't be deleted. We need to really look at every single row to be accurate

-- these are our real duplicates 
SELECT *
FROM (
	SELECT company, location, industry, total_laid_off,percentage_laid_off,`date`, stage, country, funds_raised_millions,
		ROW_NUMBER() OVER (
			PARTITION BY company, location, industry, total_laid_off,percentage_laid_off,`date`, stage, country, funds_raised_millions
			) AS row_num
	FROM 
		world_layoffs.layoffs_staging
) duplicates
WHERE 
	row_num > 1;
    
-- one solution, which I think is a good one. Is to create a new column and add those row numbers in. Then delete where row numbers are over 2, then delete that column
-- so let's do it!!


ALTER TABLE layoffs_staging ADD row_num INT;

select * from layoffs_staging;


CREATE TABLE `layoffs_staging2` (
`company` text,
`location`text,
`industry`text,
`total_laid_off` INT,
`percentage_laid_off` text,
`date` text,
`stage`text,
`country` text,
`funds_raised_millions` int,
row_num INT
);

INSERT INTO layoffs_staging2
SELECT company, location, industry, total_laid_off,percentage_laid_off,`date`, stage, country, funds_raised_millions,
		ROW_NUMBER() OVER (
			PARTITION BY company, location, industry, total_laid_off,percentage_laid_off,`date`, stage, country, funds_raised_millions
			) AS row_num
	FROM 
		layoffs_staging;
        
select * from layoffs_staging2
where row_num>1;

-- now that we have this we can delete rows were row_num is greater than 2

DELETE from layoffs_staging2
where row_num>1;

-- 2. Standardize Data

--  we have some "United States" and some "United States." with a period at the end. Let's standardize this.

SELECT DISTINCT country
FROM world_layoffs.layoffs_staging2
ORDER BY country;


select distinct country,trim(trailing '.' from country)
from layoffs_staging2
order by 1;

UPDATE layoffs_staging2
SET country=trim(trailing '.' from country)
where country like 'United States%';

-- now if we run this again it is fixed
select distinct country
from layoffs_staging2
order by 1;

-- Let's also fix the date columns:
select date, 
str_to_date(date,'%m/%d/%Y')
 from layoffs_staging2;

-- we can use str to date to update this field
UPDATE layoffs_staging2
SET date=str_to_date(date,'%m/%d/%Y');


-- now we can convert the data type properly
ALTER TABLE layoffs_staging2
MODIFY COLUMN date date;


-- if we look at industry it looks like we have some null and empty rows, let's take a look at these

SELECT *
from layoffs_staging2
where industry IS NULL
or industry = '';

-- it looks like airbnb is a travel, but this one just isn't populated.
-- I'm sure it's the same for the others. What we can do is
-- write a query that if there is another row with the same company name, it will update it to the non-null industry values
-- makes it easy so if there were thousands we wouldn't have to manually check them all
SELECT *
from layoffs_staging2
where company = 'Airbnb';

-- we should set the blanks to nulls since those are typically easier to work with
UPDATE world_layoffs.layoffs_staging2
SET industry = NULL
WHERE industry = '';

-- now if we check those are all null

SELECT *
FROM world_layoffs.layoffs_staging2
WHERE industry IS NULL 
OR industry = ''
ORDER BY industry;

-- now we need to populate those nulls if possible

UPDATE layoffs_staging2 t1
JOIN layoffs_staging2 t2
ON t1.company = t2.company
SET t1.industry = t2.industry
WHERE t1.industry IS NULL
AND t2.industry IS NOT NULL;

-- and if we check it looks like Bally's was the only one without a populated row to populate this null values
SELECT *
FROM world_layoffs.layoffs_staging2
WHERE industry IS NULL 
OR industry = ''
ORDER BY industry;


-- I also noticed the Crypto has multiple different variations. We need to standardize that - let's say all to Crypto
select * from layoffs_staging2
where industry like 'Crypto%';

select distinct industry
from layoffs_staging2
order by 1;

UPDATE layoffs_staging2
SET industry = 'Crypto'
where industry like 'Crypto%';

-- 3. Look at Null Values

-- the null values in total_laid_off, percentage_laid_off, and funds_raised_millions all look normal. I don't think I want to change that
-- I like having them null because it makes it easier for calculations during the EDA phase

-- so there isn't anything I want to change with the null values

-- 4. remove any columns and rows we need to

SELECT *
FROM layoffs_staging2
WHERE (total_laid_off IS NULL OR  total_laid_off='')
AND (percentage_laid_off is null or percentage_laid_off='');

 -- delete the rows having both total_laid_off and percentage_laid_off it shows the laid off didnt happen at all
DELETE
FROM layoffs_staging2
WHERE (total_laid_off IS NULL OR  total_laid_off='')
AND (percentage_laid_off is null or percentage_laid_off='');

 -- drop the row_num column
ALTER TABLE layoffs_staging2
DROP column row_num;

select * from layoffs_staging2;


-- EDA

-- Here we are jsut going to explore the data and find trends or patterns or anything interesting like outliers

-- normally when you start the EDA process you have some idea of what you're looking for

-- with this info we are just going to look around and see what we find!

SELECT * 
FROM world_layoffs.layoffs_staging2;

-- EASIER QUERIES

select company, max(total_laid_off)
from layoffs_staging2
where total_laid_off is not null
group by 1
order by 2 DESC;

-- Looking at Percentage to see how big these layoffs were
select max(percentage_laid_off), min(percentage_laid_off)
from layoffs_staging2
where percentage_laid_off is not null;

-- Which companies had 1 which is basically 100 percent of they company laid off
select company
from layoffs_staging2
where percentage_laid_off =1;

-- if we order by funcs_raised_millions we can see how big some of these companies were
select *
from layoffs_staging2
where percentage_laid_off =1
order by funds_raised_millions DESC;
-- BritishVolt looks like an EV company, Quibi! I recognize that company - wow raised like 2 billion dollars and went under - ouch


-- Companies with the biggest single Layoff
select company,total_laid_off
from layoffs_staging2
order by 2 DESC
LIMIT 5;
-- now that's just on a single day

-- Companies with the most Total Layoffs
SELECT company, SUM(total_laid_off)
FROM world_layoffs.layoffs_staging2
GROUP BY company
ORDER BY 2 DESC
LIMIT 10;

-- by location
SELECT location, SUM(total_laid_off)
FROM world_layoffs.layoffs_staging2
GROUP BY location
ORDER BY 2 DESC
LIMIT 10;


-- by country
SELECT country, SUM(total_laid_off)
FROM world_layoffs.layoffs_staging2
GROUP BY country
ORDER BY 2 DESC;

-- by year
SELECT YEAR(date), SUM(total_laid_off)
from layoffs_staging2
group by YEAR(date)
order by 2 ;

-- by industry
SELECT industry, SUM(total_laid_off)
FROM world_layoffs.layoffs_staging2
GROUP BY industry
ORDER BY 2 DESC;

-- by stage
SELECT stage, SUM(total_laid_off)
FROM world_layoffs.layoffs_staging2
GROUP BY stage
ORDER BY 2 DESC;

-- Earlier we looked at Companies with the most Layoffs. Now let's look at that per year. It's a little more difficult.
-- I want to look at 

WITH Company_Year AS(select company, YEAR(date) AS years, SUM(total_laid_off) as total_laid_off
from layoffs_staging2
group by 1,2
)
,Company_Year_Rank AS(select company,years,total_laid_off,
DENSE_RANK() over(partition by years order by total_laid_off DESC) as ranking
from Company_Year)

select company,years,total_laid_off,ranking
from Company_Year_Rank
where ranking<=3
AND years is not null
order by years ASC, total_laid_off DESC;


-- Rolling Total of Layoffs Per Month
with date_cte AS(
select substring(date,1,7) as dates, sum(total_laid_off) as total_laid_off
from layoffs_staging2
where date is not null
group by dates
order by dates ASC)
select dates,sum(total_laid_off) over (order by dates ASC) as rolling_lay_offs
from date_cte
order by dates ASC;