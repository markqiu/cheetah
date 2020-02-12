# cheetah
High-speed and flexible data frame caching service

# requirements
There is some data in the database that can be obtained through the database interface. Because the amount of data is very large, users need to use this data frequently and put it into the pandas dataframe for data processing, every time they get data from the database is very slow, we need to develop a flexible, high-speed dataframe caching service to speed up the user's data acquisition precess.  
At the same time, users use data not by tables, but often across multiple tables to take some of the fields in the table to form a new dataframe, so the data service needs to be flexible to implement this requirement.  
Finally, because there are so many fields in the database (tens of thousands), you want to provide a configuration that sets what data needs to be cached.
