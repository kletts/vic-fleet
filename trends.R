
library(tidyverse)
library(duckdb)
library(dbplyr)

con <- dbConnect(duckdb(), 
                 dbdir = "my-db.duckdb", 
                 read_only = FALSE)

dbExecute(con, "DROP TABLE IF EXISTS Fleet;") 
dbExecute(con, "CREATE TABLE Fleet as select * from read_parquet('data/*.parquet')") 

inner_join(tbl(con, "Fleet"), 
           tbl(con, "Fleet") |> 
             distinct(AsatDt) |>
             mutate(MinDt = min(AsatDt),  
                    Quarter = sql("DATEDIFF('quarter', MinDt, AsatDt)")), 
           by="AsatDt") |> 
  summarise(
    N = sum(Total), 
    .by=c(Make, Luxury, Type, Fuel, AsatDt, Quarter)) |> 
  compute(name="#Fleet", overwrite=TRUE)
  
# Trend in number of registered vehicles 
datastock <- tbl(con, "#Fleet") |> 
  summarise(Total = sum(N), 
            ElectPc = sum(N[Fuel=="Electric"])/sum(N), 
            DieselPc = sum(N[Fuel=="Diesel"])/sum(N), 
            MotorcyclePc = sum(N[Type=="Bike"])/sum(N),
            LuxuryPc = sum(N[Luxury=="Luxury"])/sum(N),
            HoldenPc = sum(N[Make=="HOLDEN"])/sum(N), 
            FordPc = sum(N[Make=="FORD"])/sum(N), 
            ToyotaPc = sum(N[Make=="TOYOTA"])/sum(N), 
            .by = AsatDt) |> 
  dbplyr::window_order(AsatDt) |> 
  mutate(Measure = "Stock") |> 
  collect()

# Trends in new registered cars
dataflow <- full_join(
  tbl(con, "#Fleet") |> rename(Nprev=N) |> select(-AsatDt), 
  tbl(con, "#Fleet") |> 
    mutate(Quarter = Quarter -1),
  by=c("Make", "Luxury", "Type", "Fuel", "Quarter")) |> 
  filter(!is.na(AsatDt) & Quarter >= 0) |> 
  mutate(N = ifelse(is.na(N), 0, N), 
         Nprev = ifelse(is.na(Nprev), 0, Nprev),
         Growth = N - Nprev) |> 
  summarise(Total = sum(Growth),
            ElectPc = sum(Growth[Fuel=="Electric"])/sum(Growth), 
            DieselPc = sum(Growth[Fuel=="Diesel"])/sum(Growth), 
            MotorcyclePc = sum(Growth[Type=="Bike"])/sum(Growth),
            LuxuryPc = sum(Growth[Luxury=="Luxury"])/sum(Growth),
            HoldenPc = sum(Growth[Make=="HOLDEN"])/sum(Growth), 
            FordPc = sum(Growth[Make=="FORD"])/sum(Growth), 
            ToyotaPc = sum(Growth[Make=="TOYOTA"])/sum(Growth), 
            .by=AsatDt) |> 
  mutate(Measure= "Flow") |> 
  collect() 

bind_rows(datastock, dataflow) |> 
  select(Measure, AsatDt, everything()) |> 
  write_csv("extracts/summary.csv")



