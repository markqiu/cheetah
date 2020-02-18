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

# Code design

## The data service class
Responsible for running in the background, taking data according to configuration, checking data, updating data, and saving data to the plasma store
```python
class DataService(Singleton, threading.Thread):
    """Data loaded into shared memory"""

    def __init__(self, plasma_store_name: str, object_list: List[Column], plasma_shm_size: int = 2000, update_interval: int = 300):
        """ Initialize the plasma store and list of data to maintain
        :param plasma_store_name: The name of the process that started the plasma store server
        :param plasma_shm_size: The amount of memory allocated for the cache, in M, by default 2000
        :param object_list: List of objects to be cached
        :param update_interval: Update interval (s), default 300 seconds
        """
        logging.info("Starting the plasma server...")
        if hasattr(self, "proc"):
            logging.debug("Killing the old service...")
            self.proc.kill()
        self.plasma_store_name, self.proc = start_plasma_store(plasma_store_name, int(plasma_shm_size * 1e6))
        logging.info("Plasma server started successfully!")

        self.__objects__ = {item.id: item for item in object_list}

        threading.Thread.__init__(self)
        self.stop_event = threading.Event()
        self.update_interval = update_interval

    def add_col(self, item: Column):
        """ add a column to be cached """
        self.__objects__[item.id] = item

    def remove_col(self, object_id: str):
        """ remove a column to be cached """
        if self.contains(object_id):
            self._delete_plasma_object(None, self.objects[object_id].id)
        return self.__objects__.pop(object_id)

    def has_col(self, object_id: str):
        return object_id in self.__objects__

    @property
    def columns(self) -> Dict[str, Column]:
        return self.__objects__

    def get_column_status(self, col_id):
        return self.__objects__[col_id]

    def deal_col(self, colman: Column):
        """ Get data or update data """
        plasma_client = plasma.connect(self.plasma_store_name)
        try:
            object_id = equipment_id_to_object_id(dataman.id)
            stored_data = plasma_client.get(object_id, timeout_ms=0, serialization_context=dataman.get_serialization_context())
            if stored_data is ObjectNotAvailable:
                logging.info(f"{dataman.name}: Data is not yet available in memory.")
            missing_data_ranges = dataman.check_data(stored_data)
            if missing_data_ranges:
                logging.info(f"{dataman.name}: Need to update, missing data interval is：{missing_data_ranges}")
                merged_data = stored_data
                for start, end in missing_data_ranges:
                    fetched_data = dataman.fetch_data(start, end)
                    if merged_data is not ObjectNotAvailable:
                        merged_data = dataman.merge_data(merged_data, fetched_data)
                    else:
                        merged_data = fetched_data
                    logging.info(f"{dataman.name}: ({start},{end}), the data acquisition and consolidation was successful.")
                if plasma_client.contains(object_id):
                    logging.info(f"{dataman.name}: deleting old data, please wait...")
                    plasma_client = self._delete_plasma_object(plasma_client, object_id)
                    logging.info(f"{dataman.name}: old data deleted successfully!")
                dataman.id = plasma_client.put(merged_data, object_id=object_id, serialization_context=dataman.get_serialization_context())
                dataman.date_range = max(missing_data_ranges)[1]  # Extending the back boundof of the data interval
                logging.info(f"{dataman.name}: The data update is complete, the ID is resaved as: dataman.data get ID, the new data interval is: sdataman.data interval.")
        except (KeyboardInterrupt, SystemExit):
            self.stop_event.set()
        except PlasmaStoreFull:
            logging.error(f"{dataman.name}: The memory is full yin and the data cannot be saved. Please clean up the memory!!!!")
        except Exception:
            logging.exception(f"{dataman.name}: This update is wrong, try again at the next update, or ask the administrator to check the cause.")
        finally:
            plasma_client.disconnect()

    def run(self):
        """ start the service's main thread """
        while not self.stop_event.is_set():
            for dataman in self.objects.copy().values():
                self.deal_object(dataman)
                if self.stop_event.is_set():
                    break
            logging.info(f"This update is complete, and the data update will be re-executed after the seconds of the sself.update_interval...")
            self.check_object_list()
            self.stop_event.wait(self.update_interval)

    def contains(self, object_id: str, plasma_store_contains=False):
        """ if a column with the object_id in the service """
        
    def _plasma_store_contains(self, object_id: str):
        """ if a column with the object_id in the service """
        
    def check_column_list(self) -> List[bool]:
        """ check column list in the plasma store and pring out information about it """
        plasma_client = plasma.connect(self.plasma_store_name)
        object_dict = plasma_client.list()
        result = []
        for item in self.objects:
            is_ok = plasma_client.contains(self.objects[item].id)
            result.append(is_ok)
            if is_ok:
                data_size = round(object_dict[self.objects[item].id]["data_size"] / 1024 / 1024, 2)
            else:
                data_size = 0
            logging.info(
                f"Server self-examination information: data name: .self.objects.object.data name,, data interval: .self.objects.object.data interval,"data identifier": .self.objects.object.data identifier," data exists: is_ok data size: s.data_size.M."
            )
        plasma_client.disconnect()
        return result

    def stop(self):
        """ stop the thread """
        logging.info("Receive a stop data service instruction to start stopping the service... it may take a few minutes, please be patient!")
        self.stop_event.set()
        if self.proc.poll() is None:
            self.proc.kill()
        logging.info("Data service stopped!")
```

## Data client
According to the user-specified list of fields, the data for the relevant fields is organized into a dataframe return. This dataframe is indexed by date.
```python
class DataClient(Singleton):
    def __init__(self, plasma_store_name: str = None):
        self.plasma_store_name = plasma_store_name or get_config("DATA_SERVICE_NAME")
        self.__plasma_client__ = plasma.connect(plasma_store_name)
        logging.info("The plasma server was successfully connected!")

    def get(self, equipment_id, serialization_context):
        """Get data in shared memory"""
        object_id = equipment_id_to_object_id(equipment_id)
        result = self.__plasma_client__.get(object_id, timeout_ms=0, serialization_context=serialization_context)
        return result

    def contains(self, equipment_id):
        return self.__plasma_client__.contains(equipment_id_to_object_id(equipment_id))

    def list(self):
        return [object_id_to_equipment_id(object_id) for object_id in self.__plasma_client__.list()]

    def store_capacity(self):
        return self.__plasma_client__.store_capacity()

    def get_hishty(self, start: datetime, end: datetime) -> Hishty or None:
        """get dividend data, note: If there is no data will always wait, please call the first to determine if you have the data!"""
        hishty = self.get(HishtyMan.__HISHTY_OBJECT_ID__, serialization_context=HishtyMan.get_serialization_context())
        return Hishty(start, end, {key: hishty.symbol_infos[key] for key in hishty.symbol_infos if start.strftime("%Y%m%d") <= key <= end.strftime("%Y%m%d")})

    def get_data(start: datetime, end: datetime, cols=List[str]) -> pd.DataFrame:
        """ Get the data frames defined in all cols """
        raise NotImplement    
```
