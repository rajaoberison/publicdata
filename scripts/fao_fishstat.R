#' Script for downloading and processing FAO fisheries data 
#' Developed by Andry Rajaoberison
#' Contributors: ...
#' 
#' Necessary packages: utils, dplyr, tidyr, readr
#' 
#' Data source: https://www.fao.org/fishery/en/fishstat/collections
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
dataURL <- "https://www.fao.org/fishery/static/Data/GlobalProduction_2024.1.0.zip"
download.file(dataURL, destfile=tmp_file, mode='wb')

# extract file
tmp_folder <- tempdir()
utils::unzip(tmp_file, exdir = tmp_folder)

# check files and folders
#list.files(tmp_folder)

# read raw data
tmp_fishstat <- read.csv(file.path(tmp_folder, "Global_production_quantity.csv"), check.names = F)
tmp_countries <- read.csv(file.path(tmp_folder, "CL_FI_COUNTRY_GROUPS.csv"), check.names = F) %>% select(UN_Code, ISO3_Code)
tmp_species <- read.csv(file.path(tmp_folder, "CL_FI_SPECIES_GROUPS.csv"), check.names = F) %>% select(`3A_Code`, Major_Group, ISSCAAP_Group_En, CPC_Group_En) %>% mutate(Major_Group = case_when(Major_Group == "PISCES" ~ "Fish", Major_Group == "CRUSTACEA" ~ "Crustaceans", Major_Group == "MOLLUSCA" ~ "Molluscs", Major_Group == "MAMMALIA" ~ "Mammals", Major_Group == "AMPHIBIA, REPTILIA" ~ "Amphibians, Reptiles", Major_Group == "INVERTEBRATA AQUATICA" ~ "Aquatic invertebrates", Major_Group == "PLANTAE AQUATICAE" ~ "Aquatic plants", T ~ NA))
tmp_merge <- tmp_fishstat %>% left_join(tmp_countries, by = c("COUNTRY.UN_CODE"="UN_Code")) %>% left_join(tmp_species, by = c("SPECIES.ALPHA_3_CODE"="3A_Code"))
# fix CPC group names
tmp_cpcFix <- tmp_merge %>% mutate(cpcRev = trimws(gsub(", live, fresh or chilled|live, fresh or chilled|live, fresh or chilled for human consumption", "", CPC_Group_En))) %>% mutate(cpcRev = if_else(cpcRev == "", "Not classified", cpcRev))

## aggregate
tmp_agg <- tmp_cpcFix %>% filter(ISO3_Code != "") %>% group_by(ISO3_Code, PRODUCTION_SOURCE_DET.CODE, Major_Group, cpcRev, ISSCAAP_Group_En, MEASURE, PERIOD) %>% summarise(value = sum(VALUE, na.rm = T)) %>% ungroup()
## since 1992
tmp_latest <- tmp_agg %>% filter(PERIOD >= 1992) %>% rename(ISO3 = ISO3_Code, type = PRODUCTION_SOURCE_DET.CODE, Item = Major_Group, cpc_group = cpcRev, isscaap = ISSCAAP_Group_En, unit = MEASURE, year = PERIOD)

## faostat regions
faoreg_list <- c("Australia and New Zealand", "Caribbean", "Central America", "Central Asia", "Eastern Africa", "Eastern Asia", "Eastern Europe", "Melanesia", "Micronesia", "Middle Africa", "Northern Africa", "Northern America", "Northern Europe", "Polynesia", "South America", "South-eastern Asia", "Southern Africa", "Southern Asia", "Southern Europe", "Western Africa", "Western Asia", "Western Europe")
fao_country_group <- read.csv("../faostat_region.csv", check.names = F)
faoreg_data <- fao_country_group %>% filter(`ISO3 Code` %in% iso3$ISO3, `Country Group` %in% faoreg_list) %>% select(`ISO3 Code`, `Country Group`)
iso3wReg <- iso3 %>% left_join(faoreg_data, by = c("ISO3" = "ISO3 Code"))

# final data
out_data0 <- tmp_latest %>% left_join(iso3wReg, by = "ISO3") %>% filter(!is.na(Area)) %>% select(ISO3, Area, `Country Group`, `UN Region`, `EBRD Region`, type, Item, cpc_group, isscaap, unit, year, value) %>% mutate(grouped = "no")
## get grouped stats (by Item)
out_data0_grouped <- out_data0 %>% group_by(ISO3, Area, `Country Group`, `UN Region`, `EBRD Region`, type, Item, unit, year) %>% summarise(value = sum(value, na.rm=T)) %>% ungroup() %>% mutate(cpc_group = NA, isscaap = NA, grouped = "yes")
# world
out_world <- tmp_latest %>% group_by(type, Item, cpc_group, isscaap, unit, year) %>% summarise(value = sum(value, na.rm=T)) %>% ungroup() %>% mutate(ISO3 = NA, Area = "World", `Country Group` = NA, grouped = "no")
out_world_grouped <- tmp_latest %>% group_by(type, Item, unit, year) %>% summarise(value = sum(value, na.rm=T)) %>% ungroup() %>% mutate(ISO3 = NA, Area = "World", `Country Group` = NA, cpc_group = NA, isscaap = NA, grouped = "yes")

## region average
out_unregion <- out_data0 %>% group_by(`UN Region`, type, Item, cpc_group, isscaap, unit, year) %>%  summarise(value = mean(value, na.rm=T)) %>% filter(!is.na(`UN Region`)) %>% mutate(grouped = "no")
out_unregion_grouped <- out_unregion %>% group_by(`UN Region`, type, Item, unit, year) %>% summarise(value = mean(value, na.rm=T)) %>% ungroup() %>% mutate(grouped = "yes")

out_ebrdregion <- out_data0 %>% group_by(`EBRD Region`, type, Item, cpc_group, isscaap, unit, year) %>%  summarise(value = mean(value, na.rm=T)) %>% filter(!is.na(`EBRD Region`)) %>% mutate(grouped = "no")
out_ebrdregion_grouped <- out_ebrdregion %>% group_by(`EBRD Region`, type, Item, unit, year) %>% summarise(value = mean(value, na.rm=T)) %>% ungroup() %>% mutate(grouped = "yes")

## final the data
#out_data_grouped <- bind_rows(out_data0_grouped, out_data0)
out_data <- bind_rows(out_data0_grouped, out_data0, out_world, out_world_grouped, out_unregion, out_unregion_grouped, out_ebrdregion, out_ebrdregion_grouped)

## saving as csv
# readr::write_excel_csv(out_data_grouped, paste0("../data/", "faofishstat_grouped_", thisYear, thisMonth, ".csv"))

##split into multiple csv
# for (iso3 in unique(out_data$ISO3)){
#   readr::write_excel_csv(out_data %>% filter(ISO3 == iso3 | Area == "World"), paste0("../data/faofishstat_by_country/", iso3, "_faofishstat.csv"))
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
  
  readr::write_excel_csv(thisData, paste0("../data/faofishstat_by_country/", iso3, "_faofishstat.csv"))
}





