#' Script for downloading and processing FAOSTAT Crop and Livestock data
#' Developed by Andry Rajaoberison
#' Contributors: ...
#' 
#' Necessary packages: utils, dplyr, tidyr, readr
#' 
#' Data source: https://www.fao.org/faostat/en/#data/QCL
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
dataURL <- "https://bulks-faostat.fao.org/production/Production_Crops_Livestock_E_All_Data.zip"
download.file(dataURL, destfile=tmp_file, mode='wb')

# extract file
tmp_folder <- tempdir()
utils::unzip(tmp_file, exdir = tmp_folder)

# check files and folders
#list.files(tmp_folder)

# read raw faostat data with noflag
tmp_qcl <- read.csv(file.path(tmp_folder, "Production_Crops_Livestock_E_All_Data_NOFLAG.csv"), check.names = F)
# get only Area harvested, Yield, Production, Stocks
tmp_keyVars <- tmp_qcl %>% filter(Element %in% c("Area harvested", "Yield", "Production", "Stocks"))
# fix commodity names
## add cpc_code2 to FAO data
tmp_cpcRev <- tmp_keyVars %>% mutate(CPCrev = gsub("'", "", `Item Code (CPC)`, fixed=T)) %>% left_join(CPCv21, by = c("CPCrev"="CPC Ver. 2.1 Code")) %>% mutate(cpc_code2 = substr(CPCrev, 1, 4)) %>% left_join(cpc_group2, by = "cpc_code2") %>% filter(!is.na(cpc_name1))
# select key columns
## find max year
maxYear <- names(tmp_keyVars %>% select(starts_with("Y"))) %>% gsub("Y", "", .) %>% as.integer() %>% max(na.rm=T)
## get cols
tmp_keyCols <- tmp_cpcRev %>% select(`Area Code (M49)`, Item, cpc_name1, Element, Unit, Y1992:paste0("Y",maxYear))
# transpose year and element
tmp_long <- tmp_keyCols %>% tidyr::pivot_longer(Y1992:paste0("Y",maxYear), names_to = "Year", values_to = "value") %>% mutate(Year = as.integer(gsub("Y", "", Year)))

## faostat regions
faoreg_list <- c("Australia and New Zealand", "Caribbean", "Central America", "Central Asia", "Eastern Africa", "Eastern Asia", "Eastern Europe", "Melanesia", "Micronesia", "Middle Africa", "Northern Africa", "Northern America", "Northern Europe", "Polynesia", "South America", "South-eastern Asia", "Southern Africa", "Southern Asia", "Southern Europe", "Western Africa", "Western Asia", "Western Europe")
fao_country_group <- read.csv("../faostat_region.csv", check.names = F)
faoreg_data <- fao_country_group %>% filter(`ISO3 Code` %in% iso3$ISO3, `Country Group` %in% faoreg_list) %>% select(`ISO3 Code`, `Country Group`)
iso3wReg <- iso3 %>% left_join(faoreg_data, by = c("ISO3" = "ISO3 Code"))

# clean data
out_data0 <- tmp_long %>% left_join(iso3wReg, by = c("Area Code (M49)" = "m49cd")) %>% relocate(ISO3) %>% select(-`Area Code (M49)`) %>% filter(!is.na(Area)) %>% select(ISO3, Area, `Country Group`, `UN Region`, `EBRD Region`, cpc_name1, Item, Element, Unit, Year, value) %>% mutate(grouped = "no")
## get grouped stats
out_data_grouped <- out_data0 %>% group_by(ISO3, Area, `Country Group`, `UN Region`, `EBRD Region`, cpc_name1, Element, Unit, Year) %>% summarise(value = sum(value, na.rm=T)) %>% ungroup() %>% mutate(Item = NA, grouped = "yes")

## region average
out_unregion <- out_data0 %>% group_by(`UN Region`, cpc_name1, Item, Element, Unit, Year) %>%  summarise(value = mean(value, na.rm=T)) %>% filter(!is.na(`UN Region`)) %>% mutate(grouped = "no")
out_unregion_grouped <- out_unregion %>% group_by(`UN Region`, cpc_name1, Element, Unit, Year) %>% summarise(value = mean(value, na.rm=T)) %>% ungroup() %>% mutate(grouped = "yes")

out_ebrdregion <- out_data0 %>% group_by(`EBRD Region`, cpc_name1, Item, Element, Unit, Year) %>%  summarise(value = mean(value, na.rm=T)) %>% filter(!is.na(`EBRD Region`)) %>% mutate(grouped = "no")
out_ebrdregion_grouped <- out_ebrdregion %>% group_by(`EBRD Region`, cpc_name1, Element, Unit, Year) %>% summarise(value = mean(value, na.rm=T)) %>% ungroup() %>% mutate(grouped = "yes")

## final data
out_data <- bind_rows(out_data_grouped, out_data0, out_unregion, out_unregion_grouped, out_ebrdregion, out_ebrdregion_grouped)

# # saving as csv
# readr::write_excel_csv(out_data_grouped, paste0("../data/", "faostatqcl_grouped_", thisYear, thisMonth, ".csv"))

##split into multiple csv
# for (iso3 in unique(out_data$ISO3)){
#   readr::write_excel_csv(out_data %>% filter(ISO3 == iso3 | Area == "World"), paste0("../data/faostat_qcl_by_country/", iso3, "_faostatqcl.csv"))
# }

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
  
  readr::write_excel_csv(thisData, paste0("../data/faostat_qcl_by_country/", iso3, "_faostatqcl.csv"))
}



