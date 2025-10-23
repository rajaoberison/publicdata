#' Script for downloading and processing FAOSTAT trade data
#' Developed by Andry Rajaoberison
#' Contributors: ...
#' 
#' Necessary packages: utils, dplyr, tidyr, readr
#' 
#' Data source: https://www.fao.org/faostat/en/#data/TM
#' Output format: ISO3, Area, cpc_name1, Item, Element, Unit, Year, value, grouped
#' @source: \url{https://github.com/rajaoberison/publicdata}
library(dplyr)

## geo data
# get iso3 code
iso3 <- read.csv("../iso3_regions.csv", check.names = F)

## idx data
thisYear <- as.numeric(format(Sys.Date(), "%Y"))
thisMonth <- format(Sys.Date(), "%m")

## CPC names
CPCv21 <- readxl::read_xlsx("../CPC_Ver_2.1_Exp_Notes_Updated_3Apr2025.xlsx", sheet = 1)
cpc_group1 <- CPCv21 %>% filter(nchar(`CPC Ver. 2.1 Code`) == 3) %>% rename(cpc_code1 = 1, cpc_name1 = 2) %>% select(cpc_code1, cpc_name1)
cpc_group2 <- CPCv21 %>% filter(nchar(`CPC Ver. 2.1 Code`) == 4) %>% rename(cpc_code2 = 1, cpc_name2 = 2) %>% mutate(cpc_code1 = substr(cpc_code2, 1, 3)) %>% select(cpc_code1, cpc_code2, cpc_name2) %>% left_join(cpc_group1, by = "cpc_code1")

# download file
tmp_file <- tempfile(fileext = ".zip")
dataURL <- "https://bulks-faostat.fao.org/production/Trade_DetailedTradeMatrix_E_All_Data.zip"
download.file(dataURL, destfile=tmp_file, mode='wb')

# extract file
tmp_folder <- tempdir()
utils::unzip(tmp_file, exdir = tmp_folder)

# check files and folders
#list.files(tmp_folder)

# read raw faostat data with noflag
tmp_tm <- read.csv(file.path(tmp_folder, "Trade_DetailedTradeMatrix_E_All_Data_NOFLAG.csv"), check.names = F)
# get only $ value
tmp_keyVars <- tmp_tm %>% filter(Element %in% c("Export value", "Import value"))
# fix commodity names
## add cpc_code2 to FAO data
tmp_cpcRev <- tmp_keyVars %>% mutate(CPCrev = gsub("'", "", `Item Code (CPC)`, fixed=T)) %>% left_join(CPCv21, by = c("CPCrev"="CPC Ver. 2.1 Code")) %>% mutate(cpc_code2 = substr(CPCrev, 1, 4)) %>% left_join(cpc_group2, by = "cpc_code2") %>% filter(!is.na(cpc_name1)) ## only items with CPC group retained
# select key columns
## find max year
maxYear <- names(tmp_cpcRev %>% select(starts_with("Y"))) %>% gsub("Y", "", .) %>% as.integer() %>% max(na.rm=T)
## get cols
tmp_keyCols <- tmp_cpcRev %>% select(`Reporter Country Code (M49)`, `Partner Country Code (M49)`, Item, cpc_name1, Element, Unit, Y1992:paste0("Y",maxYear))
# transpose year and element
tmp0_long <- tmp_keyCols %>% tidyr::pivot_longer(Y1992:paste0("Y",maxYear), names_to = "Year", values_to = "value") %>% mutate(Year = as.integer(gsub("Y", "", Year)))
# fix Item names
## remove indigenous categories
tmp_long <- tmp0_long %>% mutate(revItem = gsub("\\s*\\([^)]*\\)", "", Item)) %>% mutate(revItem = gsub(", fresh or chilled", "", revItem)) %>% mutate(revItem = gsub(", fresh", "", revItem)) %>% mutate(revItem = gsub(", chilled or frozen", "", revItem)) %>% mutate(revItem = gsub(", dry", "", revItem)) %>% mutate(revItem = gsub(", raw", "", revItem)) %>% mutate(revItem = gsub(", green", "", revItem)) %>% mutate(revItem = gsub(", in shell", "", revItem)) %>% mutate(revItem = trimws(revItem)) %>% group_by(`Reporter Country Code (M49)`, `Partner Country Code (M49)`, revItem, cpc_name1, Element, Unit, Year) %>% summarise(value = sum(value, na.rm = T)) %>% ungroup() %>% rename(Item = revItem)

# clean data
reporter <- iso3 %>% select(m49cd, ISO3, Area)
provider <- iso3 %>% select(m49cd, Area) %>% rename(Partner = Area)

out_data0 <- tmp_long %>% left_join(reporter, by = c("Reporter Country Code (M49)" = "m49cd")) %>% relocate(ISO3) %>% filter(!is.na(Area)) %>% left_join(provider, by = c("Partner Country Code (M49)" = "m49cd")) %>% filter(!is.na(Partner)) %>% select(ISO3, Area, Partner, cpc_name1, Item, Element, Unit, Year, value) %>% mutate(grouped = "no")
## get grouped stats
out_data_grouped <- out_data0 %>% group_by(ISO3, Area, Partner, cpc_name1, Element, Unit, Year) %>% summarise(value = sum(value, na.rm=T)) %>% ungroup() %>% mutate(Item = NA, grouped = "yes")

## final data
out_data <- bind_rows(out_data_grouped, out_data0)

# # saving as csv
# readr::write_excel_csv(out_data_grouped, paste0("../data/", "faostatqv_grouped_", thisYear, thisMonth, ".csv"))

##split into multiple csv
for (iso3 in unique(out_data_grouped$ISO3)){
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
  
  readr::write_excel_csv(thisData, paste0("../data/faostat_tm_by_country/", iso3, "_faostattm.csv"))
}




### EBRD analysis
ebrd_countries <- read.csv("../EBRD_countries.csv", check.names = F)
# ebrdOp <- ebrd_countries %>% filter(OPERATION == 1)
# ebrdRegions <- ebrd_countries %>% left_join(cty_code %>% select(`ISO-alpha3 Code`, `Country or Area`, `Sub-region Name`), by = c("ISO3"="ISO-alpha3 Code"))
# readr::write_excel_csv(ebrdRegions, "../EBRD_countries.csv")

# stats for the region
ebrdQv <- out_data %>% filter(ISO3 %in% ebrd_countries$ISO3, Year %in% 2014:2023)


