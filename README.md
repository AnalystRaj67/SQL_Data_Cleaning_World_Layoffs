Steps i performed for sql data cleaning

1. Backup Your Data:
-Before making any changes, it's always a good idea to create a backup of your data by creating staging table

2. Remove Duplicates:
- I used ROW_NUMBER() window function. This function assigns a sequential integer to each row within a partition of the result set.
- The PARTITION BY clause partitions the result set based on the combination of columns
-Identify and remove duplicate rows from your tables. I created a new staging table 'layoffs_staging2' for the duplicated data and deleted it from there

3. Standardize Data:
-Standardize data by converting to a consistent format.
-converting Date column to date time format
-populating the null or '' by using self join
- triming the whitespaces etc.
- Removing the null columns

4. Remove any columns and rows we need to
