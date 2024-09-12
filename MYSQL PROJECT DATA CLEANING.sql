select *
from layoffs;

-- 1. check for duplicates and remove any
-- 2. standardize data and fix errors
-- 3. Look at null values and see what 
-- 4. remove any columns and rows that are not necessary - few ways

CREATE TABLE layoffs_staging LIKE layoffs;

SELECT 
    *
FROM
    layoffs_staging;
    
insert into layoffs_staging
select*
from layoffs;

SELECT *, row_number() 
over(partition by company,location, industry, total_laid_off, percentage_laid_off,
 'date',stage,country, funds_raised_millions) as row_num
from layoffs_staging;

with duplicate_cte as
(
SELECT *, row_number() 
over(partition by company,location, industry, total_laid_off, percentage_laid_off,
 'date',stage,country, funds_raised_millions) as row_num
from layoffs_staging
)
select *
from duplicate_cte
where row_num >1;

SELECT 
    *
FROM
    layoffs_staging
where company = 'Casper';

-- error not updateble
/*
with duplicate_cte as
(
SELECT *, row_number() 
over(partition by company,location, industry, total_laid_off, percentage_laid_off,
 'date',stage,country, funds_raised_millions) as row_num
from layoffs_staging
)
delete
from duplicate_cte
where row_num >1;        
*/

CREATE TABLE `layoffs_staging2` (
  `company` text,
  `location` text,
  `industry` text,
  `total_laid_off` int DEFAULT NULL,
  `percentage_laid_off` text,
  `date` text,
  `stage` text,
  `country` text,
  `funds_raised_millions` int DEFAULT NULL,
  `row_num` int
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

SELECT *
FROM layoffs_staging2;

insert into layoffs_staging2
SELECT *, row_number() 
over(partition by company,location, industry, total_laid_off, percentage_laid_off,
 'date',stage,country, funds_raised_millions) as row_num
from layoffs_staging;

delete 
FROM layoffs_staging2
where row_num > 1;

select *
FROM layoffs_staging2;

-- STANDARDIZING DATA

SELECT company, trim(company)
from layoffs_staging2;

update layoffs_staging2
set company = trim(company);

select *
from layoffs_staging2
where industry like 'Crypto%';

update layoffs_staging2
set industry = 'Crypto'
where industry like 'Crypto%';

select distinct industry
from layoffs_staging2;

select *
from layoffs_staging2
where country like 'United States%'
order by 1;

SELECT DISTINCT country, trim(trailing '.' from country)
from layoffs_staging2
order by 1;

update layoffs_staging2
set country = trim(trailing '.' from country)
where country like 'United States%';

select `date`,
str_to_date(`date`, '%m/%d/%Y') as date  ## str_to_date changes the date fro 'text' to 'string'
from layoffs_staging2;

select `date`
from layoffs_staging2;

update layoffs_staging2
set `date` = str_to_date(`date`, '%m/%d/%Y');

alter table layoffs_staging2
modify column `date` date;

select *
from layoffs_staging2;

-- NULL VALUES
SELECT *
from layoffs_staging2
WHERE total_laid_off is null
and percentage_laid_off is null;

update layoffs_staging2
set industry = null
where industry = '';

SELECT *
from layoffs_staging2
WHERE industry is null
or industry = '';

select *
from layoffs_staging2
where company ='Airbnb';

select *
from layoffs_staging2 t1
join layoffs_staging2 t2
on t1.company = t2.company
where (t1.industry is null or t1.industry ='')
and t2.industry is not null;

update layoffs_staging2 t1
join layoffs_staging2 t2
on t1.company = t2.company
set t1.industry = t2.industry
where t1.industry is null
and t2.industry is not null;

select *
from layoffs_staging2
where company like 'B ally%';

SELECT *
from layoffs_staging2
WHERE total_laid_off is null
and percentage_laid_off is null;

delete
from layoffs_staging2
WHERE total_laid_off is null
and percentage_laid_off is null;

SELECT *
from layoffs_staging2;

alter table layoffs_staging2
drop column row_num;
