#' Script for downloading and processing FAOSTAT Food balance sheet
#' Developed by Andry Rajaoberison
#' Contributors: ...
#' 
#' Necessary packages: utils, dplyr, tidyr, readr
#' 
#' Data source: https://www.fao.org/faostat/en/#data/FBS
#' Output format: ISO3, Area, cpc_name1, Item, Element, Unit, Year, value, grouped
#' @source: \url{https://github.com/rajaoberison/publicdata}
library(dplyr)

## geo data
# get iso3 code
iso3 <- read.csv("../iso3_regions.csv", check.names = F)

## idx data
thisYear <- as.numeric(format(Sys.Date(), "%Y"))
thisMonth <- format(Sys.Date(), "%m")

# get commodity list
FBS_commodity_list <- read.csv("../FBS_commodity_list.csv", check.names = F)

# download file
tmp_file <- tempfile(fileext = ".zip")
dataURL <- "https://bulks-faostat.fao.org/production/FoodBalanceSheets_E_All_Data.zip"
download.file(dataURL, destfile=tmp_file, mode='wb')

# extract file
tmp_folder <- tempdir()
utils::unzip(tmp_file, exdir = tmp_folder)

# check files and folders
#list.files(tmp_folder)

# read raw faostat data with noflag
tmp_fbs <- read.csv(file.path(tmp_folder, "FoodBalanceSheets_E_All_Data_NOFLAG.csv"), check.names = F)
# vars in food supply eqn
## production + imports - Δstocks = exports + food + food processing + feed + seed + tourist consumption + industrial use + loss + residual use
balance_vars <- c("Production", "Import quantity", "Stock Variation", "Export quantity", "Food", "Processing", "Feed", "Seed", "Tourist consumption", "Other uses (non-food)", "Losses", "Residuals")
## per capita supply per day
percap_supply <- c("Food supply (kcal/capita/day)", "Food supply quantity (kg/capita/yr)", "Protein supply quantity (g/capita/day)", "Fat supply quantity (g/capita/day)")
keyVars <- c(balance_vars, percap_supply)
tmp_keyVars <- tmp_fbs %>% filter(Element %in% keyVars)

# select key columns
## find max year
maxYear <- names(tmp_keyVars %>% select(starts_with("Y"))) %>% gsub("Y", "", .) %>% as.integer() %>% max(na.rm=T)
## get cols
tmp_keyCols <- tmp_keyVars %>% select(`Area Code (M49)`, Element, Item, Unit, Y2010:paste0("Y",maxYear))

## filter to relevant items and summarize
tmp_relItem <- tmp_keyCols %>% filter(Item %in% FBS_commodity_list$`FBS Group`) %>% left_join(FBS_commodity_list %>% select(`FBS Group`, `FBS Class`), by = c("Item" = "FBS Group")) %>% group_by(`Area Code (M49)`, Element, `FBS Class`, Unit) %>% summarise(across(Y2010:paste0("Y",maxYear), \(x) sum(x, na.rm = T))) %>% ungroup()
## add type: balance vs per capita
tmp_wType <- tmp_relItem %>% mutate(Type = if_else(Element %in% balance_vars, "Balance", "Per capita"))

# transpose year and element
tmp_long <- tmp_wType %>% tidyr::pivot_longer(Y2010:paste0("Y",maxYear), names_to = "Year", values_to = "value") %>% mutate(Year = as.integer(gsub("Y", "", Year))) %>% mutate(value = case_when(Unit == "1000 t" ~ value * 1000, T ~ value)) %>% mutate(Unit = if_else(Unit == "1000 t", "t", Unit))

# clean data
clean_data <- tmp_long %>% left_join(iso3, by = c("Area Code (M49)" = "m49cd")) %>% relocate(ISO3) %>% select(-`Area Code (M49)`) %>% select(ISO3, Area, `UN Region`, `EBRD Region`, Element, `FBS Class`, Unit, Type, Year, value)

## region average
out_unregion <- clean_data %>% group_by(`UN Region`, Element, `FBS Class`, Unit, Type, Year) %>% summarise(value = mean(value)) %>% ungroup() %>% filter(!is.na(`UN Region`))
out_ebrdregion <- clean_data %>% group_by(`EBRD Region`, Element, `FBS Class`, Unit, Type, Year) %>% summarise(value = mean(value)) %>% ungroup() %>% filter(!is.na(`EBRD Region`))

## final data
out_data <- bind_rows(clean_data, out_unregion, out_ebrdregion)

## extra data#Type == "Per capita", 
aggSupplyQty <- clean_data %>% filter(!is.na(ISO3)) %>% group_by(ISO3, Area, `UN Region`, `EBRD Region`, Element, Type, Unit, Year) %>% summarise(value = sum(value, na.rm = T)) %>% ungroup()

readr::write_excel_csv(aggSupplyQty, paste0("../data/faostat_aggregates/", "agg_faostatfbs.csv"))


# saving as csv
##split into multiple csv
for (iso3 in unique(out_data$ISO3)){
  thisData <- out_data %>% filter(ISO3 == iso3 | Area == "World")
  thisUnReg <- thisData %>% filter(!is.na(`UN Region`)) %>% pull(`UN Region`) %>% unique()
  thisEbrdReg <- thisData %>% filter(!is.na(`EBRD Region`)) %>% pull(`EBRD Region`) %>% unique()
  
  if(length(thisUnReg) > 0){
    thisUnRegAvg <- out_data %>% filter(is.na(Area), `UN Region` == thisUnReg)
    thisData <- thisData %>% bind_rows(thisUnRegAvg)
  }
  
  if(length(thisEbrdReg) > 0){
    thisEbrdRegAvg <- out_data %>% filter(is.na(Area), `EBRD Region` == thisEbrdReg)
    thisData <- thisData %>% bind_rows(thisEbrdRegAvg)
  }
  
  readr::write_excel_csv(thisData, paste0("../data/faostat_fbs_by_country/", iso3, "_faostatfbs.csv"))
}




