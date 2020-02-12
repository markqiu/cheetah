# Cheetah
High-speed and flexible data frame caching service

# Requirements
## Overall demand
There is some data in the database that can be obtained through the database interface. Because the amount of data is very large, users need to use this data frequently and put it into the pandas dataframe for data processing, every time they get data from the database is very slow, we need to develop a flexible, high-speed dataframe caching service to speed up the user's data acquisition precess.  
At the same time, users use data not by tables, but often across multiple tables to take some of the fields in the table to form a new dataframe, so the data service needs to be flexible to implement this requirement.  
Finally, because there are so many fields in the database (tens of thousands), you want to provide a configuration that sets what data needs to be cached.

## Data update requirements
In addition, the data in the database grows every day, but after inserted data rarely modified, so data can be updated by indexing the date.
## User demand
Users are flexible to assemble the dataframes they need according to their needs, and instead of predicting the user's behavior, we allow users to access data within all cached fields.

# Technical requirements
1. Python
2. pandas
3. In memory using pyarrow processing, data stored in plasma store. If there is a better scheme can also be used Of course.
4. The user gets the data with a basic data box, indexed by the date field and symbol, and then filled in the field that the user wants.

# Use Example
```python
start = datetime(2010,1,1)
end = datetime(2020,1,1)
data = get_data(start, end, cols=['col1','col2','colx', 'coly'])
```
