#' Script for downloading and processing USDA PSD data
#' Developed by Andry Rajaoberison
#' Contributors: ...
#' 
#' Necessary packages: utils, dplyr, tidyr, readr
#' 
#' Data source: https://apps.fas.usda.gov/psdonline/app/index.html#/app/downloads
#' Output format: ISO3, Area, Item, Year, gas, tCO2eq
#' @source: \url{https://github.com/rajaoberison/publicdata}
library(dplyr)

## geo data
# get iso2 code
cty_code <- read.csv("../country_codes.csv", check.names = F)
unregion <- cty_code %>% mutate(`UN Region` = case_when(trimws(`Intermediate Region Name`) == "" ~ `Sub-region Name`, T ~ trimws(`Intermediate Region Name`))) %>% rename(ISO3 = `ISO-alpha3 Code`, Area = `Country or Area`) %>% filter(!is.na(ISO3), `UN Region` != "") %>% select(ISO3, `UN Region`)

ebrd_countries <- read.csv("../EBRD_countries.csv", check.names = F)
ebrdregion <- ebrd_countries %>% filter(`EBRD region` != "n/a") %>% rename(`EBRD Region` = `EBRD region`) %>% select(ISO3, `EBRD Region`)

iso2 <- cty_code %>% mutate(m49cd = paste0("'", sprintf("%03d", `M49 Code`))) %>% rename(ISO2 = `ISO-alpha2 Code`, ISO3 = `ISO-alpha3 Code`, Area = `Country or Area`) %>% select(m49cd, ISO2, ISO3, Area) %>% left_join(unregion, by = "ISO3") %>% left_join(ebrdregion, by = "ISO3")

# readr::write_excel_csv(iso2, paste0("../", "iso2_regions.csv"))

## idx data
thisYear <- as.numeric(format(Sys.Date(), "%Y"))
thisMonth <- format(Sys.Date(), "%m")

# download file
tmp_file <- tempfile(fileext = ".zip")
dataURL <- "https://apps.fas.usda.gov/psdonline/downloads/psd_alldata_csv.zip"
download.file(dataURL, destfile=tmp_file, mode='wb')

# extract file
tmp_folder <- tempdir()
utils::unzip(tmp_file, exdir = tmp_folder)

# check files and folders
#list.files(tmp_folder)

# read raw faostat data with noflag
tmp_psd <- read.csv(file.path(tmp_folder, "psd_alldata.csv"), check.names = F)
##could come from FAOSTAT

# get key vars
tmp_keyVars <- tmp_em %>% filter(Element %in% c("Emissions per capita", "Emissions per value of agricultural production", "Emissions per area of agricultural land"))

# select key columns + convert to tCO2eq
maxYear <- names(tmp_keyVars %>% select(starts_with("Y"))) %>% gsub("Y", "", .) %>% as.integer() %>% max(na.rm=T)
tmp_keyCols <- tmp_keyVars %>% select(`Area Code (M49)`, Item, Element, Unit, Y1992:paste0("Y",maxYear))

# transpose year and element
tmp_long <- tmp_keyCols %>% tidyr::pivot_longer(Y1992:paste0("Y",maxYear), names_to = "Year", values_to = "value") %>% mutate(Year = as.integer(gsub("Y", "", Year)))
# get only aggregate values
FAO_agrifood <- c("Farm gate", "Land Use change", "Pre- and Post- Production")
FAO_agg <- c("Emissions on agricultural land", "Emissions from crops", "Emissions from livestock", "Agrifood systems")
IPCC_agg <- c("IPCC Agriculture", "Agricultural Soils", "LULUCF", "AFOLU")
non_agrifood <- c("Energy", "IPPU", "Waste", "International bunkers", "Other")
Total_agg <- c("All sectors with LULUCF", "All sectors without LULUCF")

## fix na values (data-specific)
tmp_noNA <- tmp_long %>% filter(!is.na(value))
#anyNA(tmp_noNA)

# add item groups
tmp_grouped <- tmp_noNA %>% mutate(Group = case_when(Item %in% FAO_agrifood ~ "Agrifood systems", Item %in% FAO_agg ~ "FAO aggregates", Item %in% IPCC_agg ~ "IPCC aggregates", T ~ "Not agri-food")) %>% mutate(sector = case_when(Item %in% c("IPCC Agriculture", "LULUCF", non_agrifood) ~ "yes", T ~ "no"), agrifood = case_when(Item %in% FAO_agrifood ~ "yes", T ~ "no")) %>% mutate(Item = case_when(Item == "IPCC Agriculture" ~ "Agriculture", Item == "Emissions from crops" ~ "Crops", Item == "Emissions from livestock" ~ "Livestock", T ~ Item))


# clean data
clean_data <- tmp_grouped %>% right_join(iso3, by = c("Area Code (M49)" = "m49cd")) %>% relocate(ISO3) %>% select(-`Area Code (M49)`) %>% select(ISO3, Area, `UN Region`, `EBRD Region`, Element, Item, Unit, Group, sector, agrifood, Year, value)

## region average
out_unregion <- clean_data %>% group_by(`UN Region`, Element, Item, Unit, Group, sector, agrifood, Year) %>% summarise(value = mean(value)) %>% ungroup() %>% filter(!is.na(`UN Region`))
out_ebrdregion <- clean_data %>% group_by(`EBRD Region`, Element, Item, Unit, Group, sector, agrifood, Year) %>% summarise(value = mean(value)) %>% ungroup() %>% filter(!is.na(`EBRD Region`))

## final data
out_data <- bind_rows(clean_data, out_unregion, out_ebrdregion)

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
  
  readr::write_excel_csv(thisData, paste0("../data/faostat_em_by_country/", iso3, "_faostatem.csv"))
}



