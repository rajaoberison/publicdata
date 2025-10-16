#' Script for downloading and processing FAOSTAT Emission intensities of crops and livestock products
#' Developed by Andry Rajaoberison
#' Contributors: ...
#' 
#' Necessary packages: utils, dplyr, tidyr, readr
#' 
#' Data source: https://www.fao.org/faostat/en/#data/EI
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
dataURL <- "https://bulks-faostat.fao.org/production/Environment_Emissions_intensities_E_All_Data.zip"
download.file(dataURL, destfile=tmp_file, mode='wb')

# extract file
tmp_folder <- tempdir()
utils::unzip(tmp_file, exdir = tmp_folder)

# check files and folders
#list.files(tmp_folder)

# read raw faostat data with noflag
tmp_ei <- read.csv(file.path(tmp_folder, "Environment_Emissions_intensities_E_All_Data_NOFLAG.csv"), check.names = F)
# get only Emissions intensity
tmp_keyVars <- tmp_ei %>% filter(Element %in% c("Emissions intensity"))

# select key columns
## find max year
maxYear <- names(tmp_keyVars %>% select(starts_with("Y"))) %>% gsub("Y", "", .) %>% as.integer() %>% max(na.rm=T)
## get cols
tmp_keyCols <- tmp_keyVars %>% select(`Area Code (M49)`, Item, Element, Unit, Y1992:paste0("Y",maxYear))
# transpose year and element
tmp0_long <- tmp_keyCols %>% tidyr::pivot_longer(Y1992:paste0("Y",maxYear), names_to = "Year", values_to = "value") %>% mutate(Year = as.integer(gsub("Y", "", Year)))
# fix Item names
tmp_long <- tmp0_long %>% mutate(revItem = gsub("\\s*\\([^)]*\\)", "", Item)) %>% mutate(revItem = gsub(", fresh or chilled", "", revItem)) %>% mutate(revItem = gsub(", fresh", "", revItem)) %>% mutate(revItem = gsub(", chilled or frozen", "", revItem)) %>% mutate(revItem = gsub(", dry", "", revItem)) %>% mutate(revItem = gsub(", raw", "", revItem)) %>% mutate(revItem = gsub(", green", "", revItem)) %>% mutate(revItem = gsub(", in shell", "", revItem)) %>% mutate(revItem = trimws(revItem)) %>% group_by(`Area Code (M49)`, revItem, Element, Unit, Year) %>% summarise(value = sum(value, na.rm = T)) %>% ungroup() %>% rename(Item = revItem)


# clean data
out_data0 <- tmp_long %>% left_join(iso3, by = c("Area Code (M49)" = "m49cd")) %>% relocate(ISO3) %>% select(-`Area Code (M49)`) %>% filter(!is.na(Area)) %>% select(ISO3, Area, `UN Region`, `EBRD Region`, Item, Element, Unit, Year, value) %>% mutate(grouped = "no")

## region average
out_unregion <- out_data0 %>% group_by(`UN Region`, Item, Element, Unit, Year) %>%  summarise(value = mean(value, na.rm=T)) %>% filter(!is.na(`UN Region`)) %>% mutate(grouped = "no")

out_ebrdregion <- out_data0 %>% group_by(`EBRD Region`, Item, Element, Unit, Year) %>%  summarise(value = mean(value, na.rm=T)) %>% filter(!is.na(`EBRD Region`)) %>% mutate(grouped = "no")

## final data
out_data <- bind_rows(out_data0, out_unregion, out_ebrdregion)

# # saving as csv
# readr::write_excel_csv(out_data_grouped, paste0("../data/", "faostatqv_grouped_", thisYear, thisMonth, ".csv"))

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
  
  readr::write_excel_csv(thisData, paste0("../data/faostat_ei_by_country/", iso3, "_faostatei.csv"))
}



### EBRD analysis
ebrd_countries <- read.csv("../EBRD_countries.csv", check.names = F)
# ebrdOp <- ebrd_countries %>% filter(OPERATION == 1)
# ebrdRegions <- ebrd_countries %>% left_join(cty_code %>% select(`ISO-alpha3 Code`, `Country or Area`, `Sub-region Name`), by = c("ISO3"="ISO-alpha3 Code"))
# readr::write_excel_csv(ebrdRegions, "../EBRD_countries.csv")

# stats for the region
ebrdQv <- out_data %>% filter(ISO3 %in% ebrd_countries$ISO3, Year %in% 2014:2023)


