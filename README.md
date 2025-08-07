# Victorian Motor Vehicle Fleet data

This project stores Vicroads motor vehicle fleet data as parquet files, accumulated to ABS Statistical Area 3. 
See folder `data`. 

The data is published quarterly, and is available from [http://data.vic.gov.au](https://discover.data.vic.gov.au/dataset/whole-fleet-vehicle-registration-snapshot-by-postcode). 

Query the parquet files using an in process sql tool such as DuckDB as: 

```sql
select * from 'https://github.com/kletts/vic-fleet/tree/main/data/*.parquet' 
```


