#' Script for processing UN COUNTRY CODES
#' Developed by Andry Rajaoberison
#' Last updated: March 31, 2025
#' Contributors: ...
#' 
#' Necessary packages: utils, dplyr, readr
#' 
#' Data source: https://www.fao.org/faostat/en/#data/GT
#' Output format: Global Code, Global Name, Region Code, Region Name, Sub-region Code, Sub-region Name, Intermediate Region Code, Intermediate Region Name, Country or Area, M49 Code, ISO-alpha2 Code, ISO-alpha3 Code, Least Developed Countries (LDC), Land Locked Developing Countries (LLDC), Small Island Developing States (SIDS)
#' @source: \url{https://github.com/rajaoberison/publicdata}
library(dplyr)

# full data
cty_code <- read.csv("~/Library/CloudStorage/Dropbox/datasets/country_codes.csv", check.names = F)

# add regions
global_code <- cty_code %>% select(`Global Code`, `Global Name`) %>% distinct() %>% mutate(`Country or Area` = `Global Name`, `M49 Code` = `Global Code`)
region_code <- cty_code %>% select(`Global Code`, `Global Name`, `Region Code`, `Region Name`) %>% distinct() %>% mutate(`Country or Area` = `Region Name`, `M49 Code` = `Region Code`) %>% filter(!is.na(`Region Code`))
subregion_code <- cty_code %>% select(`Global Code`, `Global Name`, `Region Code`, `Region Name`, `Sub-region Code`, `Sub-region Name`) %>% distinct() %>% mutate(`Country or Area` = `Sub-region Name`, `M49 Code` = `Sub-region Code`) %>% filter(!is.na(`Region Code`) & !is.na(`Sub-region Code`))
intregion_code <- cty_code %>% select(`Global Code`, `Global Name`, `Region Code`, `Region Name`, `Sub-region Code`, `Sub-region Name`, `Intermediate Region Code`, `Intermediate Region Name`) %>% distinct() %>% mutate(`Country or Area` = `Intermediate Region Name`, `M49 Code` = `Intermediate Region Code`) %>% filter(!is.na(`Region Code`) & !is.na(`Sub-region Code`) & !is.na(`Intermediate Region Code`))

# final data
out_data <- cty_code %>% bind_rows(global_code, region_code, subregion_code, intregion_code)
# saving as csv
readr::write_excel_csv(out_data, paste0("../", "country_codes.csv"))

