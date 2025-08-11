#' Script for downloading and processing FAOSTAT Crop and Livestock data
#' Developed by Andry Rajaoberison
#' Last updated: August 09, 2025
#' Contributors: ...
#' 
#' Necessary packages: utils, dplyr, tidyr, readr
#' 
#' Data source: https://www.fao.org/faostat/en/#data/QV
#' Output format: ISO3, Area, cpc_name1, Item, Element, Unit, Year, value, grouped
#' @source: \url{https://github.com/rajaoberison/publicdata}
library(dplyr)

thisYear <- as.numeric(format(Sys.Date(), "%Y"))
thisMonth <- format(Sys.Date(), "%m")

## CPC names
CPCv21 <- readxl::read_xlsx("../CPC_Ver_2.1_Exp_Notes_Updated_3Apr2025.xlsx", sheet = 1)
cpc_group1 <- CPCv21 %>% filter(nchar(`CPC Ver. 2.1 Code`) == 3) %>% rename(cpc_code1 = 1, cpc_name1 = 2) %>% select(cpc_code1, cpc_name1)
cpc_group2 <- CPCv21 %>% filter(nchar(`CPC Ver. 2.1 Code`) == 4) %>% rename(cpc_code2 = 1, cpc_name2 = 2) %>% mutate(cpc_code1 = substr(cpc_code2, 1, 3)) %>% select(cpc_code1, cpc_code2, cpc_name2) %>% left_join(cpc_group1, by = "cpc_code1")

# download file
tmp_file <- tempfile(fileext = ".zip")
dataURL <- "https://bulks-faostat.fao.org/production/Value_of_Production_E_All_Data.zip"
download.file(dataURL, destfile=tmp_file, mode='wb')

# extract file
tmp_folder <- tempdir()
utils::unzip(tmp_file, exdir = tmp_folder)

# check files and folders
#list.files(tmp_folder)

# read raw faostat data with noflag
tmp_qv <- read.csv(file.path(tmp_folder, "Value_of_Production_E_All_Data_NOFLAG.csv"), check.names = F)
# get only current and constant US$
tmp_keyVars <- tmp_qv %>% filter(Element %in% c("Gross Production Value (current thousand US$)", "Gross Production Value (constant 2014-2016 thousand US$)"))
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

# get iso3 code
cty_code <- read.csv("../country_codes.csv", check.names = F)
iso3 <- cty_code %>% mutate(m49cd = paste0("'", sprintf("%03d", `M49 Code`))) %>% rename(ISO3 = `ISO-alpha3 Code`, Area = `Country or Area`) %>% select(m49cd, ISO3, Area)

# final data
out_data0 <- tmp_long %>% left_join(iso3, by = c("Area Code (M49)" = "m49cd")) %>% relocate(ISO3) %>% select(-`Area Code (M49)`) %>% filter(!is.na(Area)) %>% select(ISO3, Area, cpc_name1, Item, Element, Unit, Year, value) %>% mutate(grouped = "no")
## get grouped stats
out_data_grouped <- out_data0 %>% group_by(ISO3, Area, cpc_name1, Element, Unit, Year) %>% summarise(value = sum(value, na.rm=T)) %>% ungroup() %>% mutate(Item = NA, grouped = "yes")
## combine the two data
out_data <- bind_rows(out_data_grouped, out_data0)

# saving as csv
readr::write_excel_csv(out_data_grouped, paste0("../data/", "faostatqv_grouped_", thisYear, thisMonth, ".csv"))

##split into multiple csv
for (iso3 in unique(out_data$ISO3)){
  readr::write_excel_csv(out_data %>% filter(ISO3 == iso3 | Area == "World"), paste0("../data/faostat_qv_by_country/", iso3, "_faostatqv_", thisYear, thisMonth, ".csv"))
}





