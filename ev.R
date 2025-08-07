library(tidyverse)
library(sf)
library(arrow)
library(tsibble)
library(patchwork)
library(grid)
library(gridExtra)
library(ckanr)
library(duckdb)

#---- sources ----- 
"https://vicroadsopendatastorehouse.vicroads.vic.gov.au/opendata/Registration_Licencing/2023/Whole_Fleet_Vehicle_Registration_Snapshot_by_Postcode_Q3_2023.csv"

# ----- functions ----- 

vicroads_yq <- function(x="Q22023") {  
  x <- str_match(x, "Q(\\d)(\\d{4})") 
  tsibble::make_yearquarter(year=as.numeric(x[,3]), quarter=as.numeric(x[,2]))  }


#---- read ----- 
con <- DBI::dbConnect(duckdb::duckdb(dbdir  = ":memory:"))

data <- DBI::dbGetQuery(con, "select * from 'data/*.parquet' 
    where AsatDt in (select max(AsatDt) FROM 'data/*.parquet');")

prev <- DBI::dbGetQuery(con, "select * from 'data/*.parquet' 
    where AsatDt in (select max(AsatDt) from 'data/*.parquet'
      where AsatDt not in (select max(AsatDt) from 'data/*.parquet'));")

DBI::dbGetQuery(con, "select * from 'data/*.parquet' 
    where AsatDt in (select distinct AsatDt from 'data/*.parquet' 
                order by AsatDt desc limit 1 offset 1);") 

duckplyr::df_from_parquet("data/*.parquet") |> 
  filter(AsatDt == max(AsatDt)) |> 
  summarise(FuelPc = sum(Total[Fuel=="Electric"], na.rm = TRUE)/sum(Total, na.rm = TRUE), 
            .by=c("AsatDt", "Sa3Code")) 



sa3map <- st_read("~/Documents/ABSMAPS/2021 Structures/ASGS_2021_Main_Structure_GDA2020.gpkg", 
                  layer="SA3_2021_AUST_GDA2020") |> 
  rmapshaper::ms_simplify(keep = 0.1) 




data.fuel <- data %>% 
  filter(Type=="Car") %>% 
  inner_join(temp, by=c("Postcode"="POA_CODE21")) %>% 
  group_by(SA3_CODE_2021, Period, Fuel) %>% 
  summarise(N=sum(N)) %>% 
  arrange(SA3_CODE_2021, Fuel, Period) %>% 
  group_by(SA3_CODE_2021, Fuel) %>% 
  summarise(New = diff(N), 
            N=N[Period==yearquarter("2023 Q4")], 
            Ngrowth= New/N) %>% 
  group_by(SA3_CODE_2021) %>% 
  mutate(ElecPc = ifelse(Fuel=="Electric", N/sum(N), NA_real_)) %>% 
  filter(Fuel=="Electric") %>% 
  inner_join(sa3map, by="SA3_CODE_2021") %>%  
  st_as_sf()


ggplot_fuel <- function(data.fuel, 
                        region=c("State", "2GMEL", "2RVIC"),
                        meas=c("N", "ElecPc", "New", "Ngrowth", "LuxPc"), 
                        colour="#153460") {  
  meas <- match.arg(meas) 
  region <- match.arg(region)
  title <- c(N="Total registered EVs", 
             ElecPc="EVs as proportion of all registered MVs", 
             New="EVs registered in the last quarter", 
             Ngrowth="Percentage increase in registered EVs", 
             LuxPc="Luxury MVs as proportion of all MVs")
  temp <- data.fuel %>% st_as_sf() %>% st_simplify(dTolerance=100) 
  limits <- range(temp[[meas]], na.rm = TRUE)
  temp %>% 
    { if (region=="State") (.) else filter(., GCCSA_CODE_2021==region) } %>% 
    ggplot() + 
    geom_sf(aes(fill=.data[[meas]]), lwd=0.1, col="lightgray") +  
    scale_fill_gradient(low = "#ffffff", 
                        high = colour,
                        label= if (meas %in% c("ElecPc", "Ngrowth", "LuxPc")) { scales::percent } else { scales::number },
                        limits=limits) + 
    theme_void() + 
    labs(fill=NULL) + 
    theme(legend.position = c(0.95,0.95), 
          legend.justification = c(1,1))  } 
 

p1 <- ggplot_fuel(data.fuel, region="State", meas="ElecPc")
p2 <- ggplot_fuel(data.fuel, region="2GMEL", meas="ElecPc") + 
  theme(legend.position = "none")
p12 <- wrap_elements(p1 + p2 + plot_annotation(title="EVs as proportion of all MVs"))
  
p3 <- ggplot_fuel(data.fuel, region="State", meas="Ngrowth", colour="#3EBCB6")
p4 <- ggplot_fuel(data.fuel, region="2GMEL", meas="Ngrowth", colour="#3EBCB6") + 
    theme(legend.position = "none")
p34 <- wrap_elements(p3 + p4 + plot_annotation(title="Growth in EVs")) 

lg <- grid::rasterGrob(png::readPNG('~/Bizscisolve/home/images/Logo_3.png'), 
                       height=unit(3, "lines"))

t0 <- grid::textGrob(
  glue::glue("Victorian Electric Vehicle Ownership — {format(max(data$Period), 'Q%q %Y')}"), 
  gp=gpar(fontsize = 16, fontface="bold"))
t0 <- arrangeGrob(t0, lg, nrow=1, widths=c(10,1))

t3 <- grid::textGrob(
  glue::glue("APRA Quarterly Performance Statistics,
                All ADIs Residential Property Expsoures
               Prepared by: Christian Klettner 
               christian@bizscisolve.com.au 
               Printed: {format(Sys.Date(), '%d %b %Y')}"), 
  gp=gpar(fontsize = 9, fontface="italic")) 

t2 <- grid::textGrob(
  "Delinquent Loans: 30 to 89 DPD 
    Non-performing Loans: > 90 DPD, Unlikely to Pay, Restructured
    Loan purpose: OO Owner-Occupied, RI Residential Investment, 
    NRP Non-Residential (eg Business) Purpose, LOC Secured Line of Credit",
  gp=gpar(fontsize = 9, fontface="italic"))

tt <- grid.arrange(t3, t2, ncol=2)

wrap_plots(
  t0, p12, p34, t1, tt,
  nrow = 5, heights = c(1, 5, 5, 1.5, 1))  

# Makes ----- 

label_growth <- function(x) { 
  scales::label_percent(accuracy = 1, style_positive="plus")(x)
  }

temp <- data %>% 
  ungroup()  %>% 
  filter(Fuel=="Electric") %>% 
  mutate(Make=fct_lump_n(Make, 10, w=N)) %>% 
  group_by(Period, Make) %>% 
  summarise(N=sum(N)) %>% 
  pivot_wider(id_cols = (Period), names_from=Make, values_from=N) %>% 
  ungroup() 

t1 <- bind_rows(
  temp %>% mutate(
    Period = as.character(Period), 
    across(-Period, ~format(.x, big.mark=","))), 
  temp %>% summarise(
    Period="%chg",
    across(-Period, ~scales::label_percent(accuracy=0.1)(tail(.x,1)/head(.x, 1) -1)))) %>% 
  Rfunlib::make_tableGrob(title="Market Share of Major Models")


p1 <- ggplot_fuel(data.fuel %>% filter(Fuel=="Electric"), 
                  meas="ElecPc")
p2 <- ggplot_fuel(data.fuel %>% filter(Fuel=="Electric"), 
                  meas="Ngrowth", 
                  region="State", colour="#3EBCB6")


ggsave("~/Documents/Vicroads/EVownership.pdf", 
       patchwork::wrap_plots(t0, p1, p2, t1,
                             ncol=1, heights=c(1, 8, 6, 2)) & 
         plot_annotation(tag_levels = list("", "EV (% MV)", "% chg in EVs", "")),
       device="pdf", 
       scale=1, 
       width=22, 
       height=29, 
       units="cm") 
