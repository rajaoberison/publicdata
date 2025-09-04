#' Script for downloading and processing FAOSTAT Macro indicators
#' Developed by Andry Rajaoberison
#' Last updated: August 12, 2025
#' Contributors: ...
#' 
#' Necessary packages: utils, dplyr, tidyr, readr
#' 
#' Data source: https://www.fao.org/faostat/en/#data/MK
#' Output format: ISO3, Area, cpc_name1, Item, Element, Unit, Year, value, grouped
#' @source: \url{https://github.com/rajaoberison/publicdata}
library(dplyr)

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

# get iso3 code
cty_code <- read.csv("../country_codes.csv", check.names = F)
iso3 <- cty_code %>% mutate(m49cd = paste0("'", sprintf("%03d", `M49 Code`))) %>% rename(ISO3 = `ISO-alpha3 Code`, Area = `Country or Area`) %>% select(m49cd, ISO3, Area)

# final data
out_data <- tmp_long %>% left_join(iso3, by = c("Area Code (M49)" = "m49cd")) %>% relocate(ISO3) %>% select(-`Area Code (M49)`) %>% filter(!is.na(Area)) %>% select(ISO3, Area, Item, Element, Unit, Year, value)

# saving as csv
readr::write_excel_csv(out_data, paste0("../data/", "faostatqv_grouped_", thisYear, thisMonth, ".csv"))

##split into multiple csv
for (iso3 in unique(out_data$ISO3)){
  readr::write_excel_csv(out_data %>% filter(ISO3 == iso3 | Area == "World"), paste0("../data/faostat_mk_by_country/", iso3, "_faostatmk_", thisYear, thisMonth, ".csv"))
}


### EBRD analysis
ebrd_countries <- read.csv("../EBRD_countries.csv", check.names = F)
# stats for the region
ebrdMk <- out_data %>% filter(ISO3 %in% ebrd_countries$ISO3, Year %in% 2014:2023)



