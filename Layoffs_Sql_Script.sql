-- SQL Project - Data Cleaning


SELECT * 
FROM world_job_layoffs.layoffs;


-- first thing we want to do is create a staging table. This is the one we will work in and clean the data. We want a table with the raw data in case something happens
CREATE TABLE world_job_layoffs.layoffs_staging 
LIKE world_job_layoffs.layoffs;

INSERT layoffs_staging 
SELECT * FROM world_job_layoffs.layoffs;


-- now when we are cleaning data we usually follow a few steps

-- 1. Remove Duplicates

# First let's check for duplicates


SELECT *
FROM world_job_layoffs.layoffs_staging;


SELECT *
FROM (
	SELECT company, industry, total_laid_off,`date`,
		ROW_NUMBER() OVER (
			PARTITION BY company, industry, total_laid_off,`date`
			) AS row_num
	FROM 
		world_job_layoffs.layoffs_staging
) duplicates
WHERE 
	row_num > 1;
    
-- let's just look at oda to confirm
SELECT *
FROM world_job_layoffs.layoffs_staging
WHERE company = 'Oda'
;
-- it looks like these are all legitimate entries and shouldn't be deleted. We need to really look at every single row to be accurate

-- these are our real duplicates 
SELECT *
FROM (
	SELECT company, location, industry, total_laid_off,percentage_laid_off,`date`, stage, country, funds_raised_millions,
		ROW_NUMBER() OVER (
			PARTITION BY company, location, industry, total_laid_off,percentage_laid_off,`date`, stage, country, funds_raised_millions
			) AS row_num
	FROM 
		world_job_layoffs.layoffs_staging
) duplicates
WHERE 
	row_num > 1;

-- these are the ones we want to delete where the row number is > 1 or 2or greater essentially

-- assigning a temporary table called 'DELETE_CTE'

WITH DELETE_CTE AS 
(
SELECT *
FROM (
	SELECT company, location, industry, total_laid_off,percentage_laid_off,`date`, stage, country, funds_raised_millions,
		ROW_NUMBER() OVER (
			PARTITION BY company, location, industry, total_laid_off,percentage_laid_off,`date`, stage, country, funds_raised_millions
			) AS row_num
	FROM 
		world_job_layoffs.layoffs_staging
) duplicates
WHERE 
	row_num > 1
)
DELETE
FROM DELETE_CTE
;


WITH DELETE_CTE AS (
	SELECT company, location, industry, total_laid_off, percentage_laid_off, `date`, stage, country, funds_raised_millions, 
    ROW_NUMBER() OVER (PARTITION BY company, location, industry, total_laid_off, percentage_laid_off, `date`, stage, country, funds_raised_millions) AS row_num
	FROM world_job_layoffs.layoffs_staging
)
DELETE FROM world_job_layoffs.layoffs_staging
WHERE (company, location, industry, total_laid_off, percentage_laid_off, `date`, stage, country, funds_raised_millions, row_num) IN (
	SELECT company, location, industry, total_laid_off, percentage_laid_off, `date`, stage, country, funds_raised_millions, row_num
	FROM DELETE_CTE
) AND row_num > 1;

-- MySQL does not support using the table being modified (in this case, layoffs_staging) in a subquery in a DELETE statement.

-- Another approach is to create a new column and add those row numbers in. Then delete where row numbers are over 2, then delete that column


ALTER TABLE world_job_layoffs.layoffs_staging ADD row_num INT;

SELECT *
FROM world_job_layoffs.layoffs_staging
;

-- Creating a layoffs_staging2 table with the data of  duplicates 

CREATE TABLE `world_job_layoffs`.`layoffs_staging2` (
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

INSERT INTO `world_job_layoffs`.`layoffs_staging2`
(`company`,
`location`,
`industry`,
`total_laid_off`,
`percentage_laid_off`,
`date`,
`stage`,
`country`,
`funds_raised_millions`,
`row_num`)
SELECT `company`,
`location`,
`industry`,
`total_laid_off`,
`percentage_laid_off`,
`date`,
`stage`,
`country`,
`funds_raised_millions`,
		ROW_NUMBER() OVER (
			PARTITION BY company, location, industry, total_laid_off,percentage_laid_off,`date`, stage, country, funds_raised_millions
			) AS row_num
	FROM 
		world_job_layoffs.layoffs_staging;

-- now that we have this we can delete rows were row_num is greater than 2

DELETE FROM world_job_layoffs.layoffs_staging2
WHERE row_num >= 2;

-- This will be empty as we have deleted the duplicate data and now left with distinct data
-- in layoffs_staging2 table
 
SELECT * FROM world_job_layoffs.layoffs_staging2
WHERE row_num >= 2;


-- 2. Standardize Data

SELECT * 
FROM world_job_layoffs.layoffs_staging2;

-- if we look at industry it looks like we have some null and empty rows, let's take a look at these
SELECT DISTINCT industry
FROM world_job_layoffs.layoffs_staging2
ORDER BY industry;

SELECT *
FROM world_job_layoffs.layoffs_staging2
WHERE industry IS NULL 
OR industry = ''
ORDER BY industry;

-- let's take a look at these
SELECT *
FROM world_job_layoffs.layoffs_staging2
WHERE company LIKE 'Bally%';

-- nothing wrong here
SELECT *
FROM world_job_layoffs.layoffs_staging2
WHERE company LIKE 'airbnb%';

-- it looks like airbnb is a travel, but this one just isn't populated.
-- I'm sure it's the same for the others. What we can do is
-- write a query that if there is another row with the same company name, it will update it to the non-null industry values
-- makes it easy so if there were thousands we wouldn't have to manually check them all

-- we should set the blanks to nulls since those are typically easier to work with
UPDATE world_job_layoffs.layoffs_staging2
SET industry = NULL
WHERE industry = '';

-- now if we check those are all null

SELECT *
FROM world_job_layoffs.layoffs_staging2
WHERE industry IS NULL 
OR industry = ''
ORDER BY industry;

-- now we need to populate those nulls if possible
-- Populating the missing industry data from another row of the same company using a self join

UPDATE layoffs_staging2 t1
JOIN layoffs_staging2 t2
ON t1.company = t2.company
SET t1.industry = t2.industry
WHERE t1.industry IS NULL
AND t2.industry IS NOT NULL;

-- and if we check it looks like Bally's was the only one without a populated row to populate this null values
SELECT *
FROM world_job_layoffs.layoffs_staging2
WHERE industry IS NULL 
OR industry = ''
ORDER BY industry;

-- ---------------------------------------------------

-- I also noticed the Crypto has multiple different variations. We need to standardize that - let's say all to Crypto
SELECT DISTINCT industry
FROM world_job_layoffs.layoffs_staging2
ORDER BY industry;

UPDATE layoffs_staging2
SET industry = 'Crypto'
WHERE industry IN ('Crypto Currency', 'CryptoCurrency');

-- now that's taken care of:
SELECT DISTINCT industry
FROM world_job_layoffs.layoffs_staging2
ORDER BY industry;

-- --------------------------------------------------
-- we also need to look at 

SELECT *
FROM world_job_layoffs.layoffs_staging2;

-- everything looks good except apparently we have some "United States" and some "United States." with a period at the end. Let's standardize this.
SELECT DISTINCT country
FROM world_job_layoffs.layoffs_staging2
ORDER BY country;

UPDATE layoffs_staging2
SET country = TRIM(TRAILING '.' FROM country);

-- now if we run this again it is fixed
SELECT DISTINCT country
FROM world_job_layoffs.layoffs_staging2
ORDER BY country;


-- Fixing the date columns:
SELECT *
FROM world_job_layoffs.layoffs_staging2;

-- we can use str to date to update this field
UPDATE layoffs_staging2
SET `date` = STR_TO_DATE(`date`, '%m/%d/%Y');

-- now we can convert the data type properly
ALTER TABLE layoffs_staging2
MODIFY COLUMN `date` DATE;


SELECT *
FROM world_job_layoffs.layoffs_staging2;


-- 3. Look at Null Values

-- the null values in total_laid_off, percentage_laid_off, and funds_raised_millions all look normal. I don't think I want to change that
-- I like having them null because it makes it easier for calculations during the EDA phase

-- so there isn't anything I want to change with the null values

-- 4. remove any columns and rows we need to

SELECT *
FROM world_job_layoffs.layoffs_staging2
WHERE total_laid_off IS NULL;

SELECT *
FROM world_job_layoffs.layoffs_staging2
WHERE total_laid_off IS NULL
AND percentage_laid_off IS NULL;

-- Delete Useless data we can't really use. where 'total_laid_off' and 'percentage_laid_off' both are null.
DELETE FROM world_job_layoffs.layoffs_staging2
WHERE total_laid_off IS NULL
AND percentage_laid_off IS NULL;

SELECT * 
FROM world_job_layoffs.layoffs_staging2;

-- Dropping the row_num column
ALTER TABLE layoffs_staging2
DROP COLUMN row_num;

-- All set
SELECT * 
FROM world_job_layoffs.layoffs_staging2;