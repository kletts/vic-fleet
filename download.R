
library(ckanr)
library(tidyverse)
library(arrow)

eoq <- function(x, n=0) {
    lubridate::ceiling_date(x, unit = "quarter") + base::months(n*3) - lubridate::days(1)
    }

make_yq <- function(x, prev=FALSE) { 
    if (prev) { 
        x <- floor_date(x, "quarter") - days(1) }
    paste("Q", quarter(x), year(x), sep="") }

read_yq <- function(x) { 
    tsibble::make_yearquarter(
        year=as.numeric(str_extract(x, "\\d{4}$")), 
        quarter=as.numeric(str_extract(x, "Q(\\d{1})", 1))
    )}

fct_fuel <- function(x) { 
    type <- c("Electric"="E", 
              "Diesel"="D", 
              "Petrol"="P", 
              "MultiFuels"="M", 
              "Other"="O", 
              "Rotary"="R", 
              "Steam"="S",
              "LPG"="G")
    type <- setNames(names(type), type)
    recode(str_trim(x), !!!type, .default = "Other")   }

fct_luxury <- function(x) { 
    luxury <- c("MERC B", "B M W", "AUDI", "LEXUS", "L ROV", "M G", "PORSCH",
                "TESLA", "JAGUAR", "ALFA R", "SAAB", "FERRAR", "BENT", "CUPRA",
                "ASTON", "ROVER", "ROLLS", "AUSTIN", "LOTUS", "LAMBR")
    factor(x %in% luxury, levels=c(FALSE, TRUE), labels=c("Standard", "Luxury"))   
    }


ckanr_setup(url = "https://discover.data.vic.gov.au/", 
            key = Sys.getenv("DATAVIC_APIKEY"))

ds <- resource_search(q="name:Whole Fleet Vehicle Registration Snapshot by Postcode", 
                      as = 'table')
resid <- subset(ds$results, historical=="FALSE")$resource_id
res <- resource_show(id = resid) 

asat <- as.Date(subset(ds$results, historical=="FALSE")$period_end)
data <- ckan_fetch(res$url, format=res$format) 
poa2sa3 <- read_rds("data/poa2sa3.rds")

data <- data |> 
    mutate(POSTCODE = sprintf("%04.0f", POSTCODE)) |> 
    inner_join(poa2sa3, by=c("POSTCODE"="POA_CODE21")) |> 
    summarise(Total = sum(TOTAL1), 
              .by=c("CD_MAKE_VEH1", "CD_CLASS_VEH", "NB_YEAR_MFC_VEH", "CD_CL_FUEL_ENG", "SA3_CODE_2021")) |> 
    mutate(AsatDt = eoq(asat), 
           Fuel=fct_fuel(CD_CL_FUEL_ENG), 
           Make=CD_MAKE_VEH1, 
           AgeYears = year(AsatDt) - NB_YEAR_MFC_VEH,
           Type=ifelse(CD_CLASS_VEH==2, "Car", "Bike"), 
           Luxury = fct_luxury(Make)) |> 
    select(AsatDt, Make, Type, Fuel, AgeYears, Luxury, Sa3Code=SA3_CODE_2021, Total) 

filename <- paste("data/VicRoadsFleetByPostcode_", make_yq(asat), ".parquet", sep="")
arrow::write_parquet(data, filename)


