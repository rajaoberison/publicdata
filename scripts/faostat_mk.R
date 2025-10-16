#' Script for downloading and processing FAOSTAT Macro indicators
#' Developed by Andry Rajaoberison
#' Contributors: ...
#' 
#' Necessary packages: utils, dplyr, tidyr, readr
#' 
#' Data source: https://www.fao.org/faostat/en/#data/MK
#' Output format: ISO3, Area, cpc_name1, Item, Element, Unit, Year, value, grouped
#' @source: \url{https://github.com/rajaoberison/publicdata}
library(dplyr)

## geo data
# get iso3 code
iso3 <- read.csv("../iso3_regions.csv", check.names = F)

## idx data
thisYear <- as.numeric(format(Sys.Date(), "%Y"))
thisMonth <- format(Sys.Date(), "%m")

# download file
tmp_file <- tempfile(fileext = ".zip")
dataURL <- "https://bulks-faostat.fao.org/production/Macro-Statistics_Key_Indicators_E_All_Data.zip"
download.file(dataURL, destfile=tmp_file, mode='wb')

# extract file
tmp_folder <- tempdir()
utils::unzip(tmp_file, exdir = tmp_folder)

# check files and folders
#list.files(tmp_folder)

# read raw faostat data with noflag
tmp_mk <- read.csv(file.path(tmp_folder, "Macro-Statistics_Key_Indicators_E_All_Data_NOFLAG.csv"), check.names = F)
# get only current and constant US$
tmp_keyVars <- tmp_mk %>% filter(Item %in% c("Gross Domestic Product", "Value Added (Agriculture, Forestry and Fishing)", "Value Added (Total Manufacturing)", "Gross National Income", "Gross Output (Agriculture, Forestry and Fishing)", "Value Added (Agriculture)", "Gross Output (Agriculture)"))
# fix names
## no need
# select key columns
## find max year
maxYear <- names(tmp_keyVars %>% select(starts_with("Y"))) %>% gsub("Y", "", .) %>% as.integer() %>% max(na.rm=T)
## get cols
tmp_keyCols <- tmp_keyVars %>% select(`Area Code (M49)`, Item, Element, Unit, Y1992:paste0("Y",maxYear))
# transpose year and element
tmp_long <- tmp_keyCols %>% tidyr::pivot_longer(Y1992:paste0("Y",maxYear), names_to = "Year", values_to = "value") %>% mutate(Year = as.integer(gsub("Y", "", Year)))

# clean data
clean_data <- tmp_long %>% left_join(iso3, by = c("Area Code (M49)" = "m49cd")) %>% relocate(ISO3) %>% select(-`Area Code (M49)`) %>% filter(!is.na(Area)) %>% select(ISO3, Area, `UN Region`, `EBRD Region`, Item, Element, Unit, Year, value)

## region average
out_unregion <- clean_data %>% group_by(`UN Region`, Item, Element, Unit, Year) %>% summarise(value = mean(value, na.rm = T)) %>% ungroup() %>% filter(!is.na(`UN Region`))
out_ebrdregion <- clean_data %>% group_by(`EBRD Region`, Item, Element, Unit, Year) %>% summarise(value = mean(value, na.rm = T)) %>% ungroup() %>% filter(!is.na(`EBRD Region`))

## final data
out_data <- clean_data %>% bind_rows(out_ebrdregion) %>% bind_rows(out_unregion)

## saving as csv
#readr::write_excel_csv(out_data, paste0("../data/", "faostatqv_grouped_", thisYear, thisMonth, ".csv"))

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
  
  readr::write_excel_csv(thisData, paste0("../data/faostat_mk_by_country/", iso3, "_faostatmk.csv"))
}






### EBRD analysis
ebrd_countries <- read.csv("../EBRD_countries.csv", check.names = F)
# stats for the region
ebrdMk <- out_data %>% filter(ISO3 %in% ebrd_countries$ISO3, Year %in% 2014:2023)



