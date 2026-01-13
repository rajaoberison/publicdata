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
cpc_group0 <- CPCv21 %>% filter(nchar(`CPC Ver. 2.1 Code`) == 2) %>% rename(cpc_code0 = 1, cpc_name0 = 2) %>% select(cpc_code0, cpc_name0)
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
tmp_keyCols <- tmp_cpcRev %>% select(`Reporter Country Code (M49)`, `Partner Country Code (M49)`, Item, cpc_name1, cpc_code1, Element, Unit, Y1992:paste0("Y",maxYear))

## remove non-partners
tmp_partners <- tmp_keyCols %>% mutate(Total = rowSums(across(Y1992:paste0("Y",maxYear)), na.rm=T)) %>% filter(Total>0) %>% select(-Total)

## country trade partners
reporter <- iso3 %>% select(m49cd, ISO3, Area)
provider <- iso3 %>% select(m49cd, ISO3, Area) %>% rename(ISOP = ISO3, Partner = Area)

## loop by country
for (m49 in unique(tmp_partners$`Reporter Country Code (M49)`)){
  # transpose year and element
  tmp0_long <- tmp_partners %>% filter(`Reporter Country Code (M49)` == m49) %>% tidyr::pivot_longer(Y1992:paste0("Y",maxYear), names_to = "Year", values_to = "value") %>% mutate(Year = as.integer(gsub("Y", "", Year)))
  ## get only agriculture and food products
  tmp1_long <- tmp0_long %>% filter(cpc_name1 %in% c(pull(filter(cpc_group1, as.integer(cpc_code1) < 50), cpc_name1), pull(filter(cpc_group1, between(as.integer(cpc_code1), 200, 300)), cpc_name1)))
  ## group stats for non-primary products
  tmp_long <- tmp1_long %>% 
    mutate(cpc_code0 = substr(cpc_code1, 1, 2)) %>% 
    left_join(cpc_group0, by = "cpc_code0") %>% 
    mutate(Item = case_when(cpc_name1 %in% pull(filter(cpc_group1, between(as.integer(cpc_code1), 200, 300)), cpc_name1) ~ cpc_name0, cpc_name1 %in% pull(filter(cpc_group1, between(as.integer(cpc_code1), 30, 40)), cpc_name1) ~ cpc_name1, T ~ Item)) %>% 
    mutate(cpc_name1 = case_when(cpc_name1 %in% pull(filter(cpc_group1, as.integer(cpc_code1) < 30), cpc_name1) ~ cpc_name1, T ~ "Other groups")) %>% 
    mutate(cpc_name1 = case_when(grepl("Forage products; fibre crops; plants used in", cpc_name1, fixed=T) ~ "Forage, fibers, and other crops (CPC code 019)", T ~ cpc_name1)) %>% 
    group_by(`Reporter Country Code (M49)`, `Partner Country Code (M49)`, cpc_name1, Item, Element, Unit, Year) %>% 
    summarise(value = sum(value, na.rm=T), .groups = "drop")
  ## get top partners
  ## pct>5% 
  tmp_top <- tmp_long %>% group_by(`Reporter Country Code (M49)`, cpc_name1, Item, Element, Unit, Year) %>% mutate(pct = value*100/sum(value, na.rm = T)) %>% ungroup() %>% filter(pct >= 5) %>% select(-pct)
  
  # clean data
  out_data <- tmp_top %>% left_join(reporter, by = c("Reporter Country Code (M49)" = "m49cd")) %>% relocate(ISO3) %>% filter(!is.na(Area)) %>% left_join(provider, by = c("Partner Country Code (M49)" = "m49cd")) %>% filter(!is.na(Partner)) %>% select(ISO3, Area, ISOP, Partner, cpc_name1, Item, Element, Unit, Year, value)
  
  # saving as csv
  readr::write_excel_csv(out_data, paste0("../data/faostat_tm_by_country/", unique(out_data$ISO3), "_faostattm.csv"))
}


