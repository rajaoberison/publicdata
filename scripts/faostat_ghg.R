#' Script for downloading and processing FAOSTAT GHG data
#' Developed by Andry Rajaoberison
#' Contributors: ...
#' 
#' Necessary packages: utils, dplyr, tidyr, readr
#' 
#' Data source: https://www.fao.org/faostat/en/#data/GT
#' Output format: ISO3, Area, Item, Year, gas, tCO2eq
#' @source: \url{https://github.com/rajaoberison/publicdata}
library(dplyr)

## geo data
# get iso3 code
cty_code <- read.csv("../country_codes.csv", check.names = F)
unregion <- cty_code %>% mutate(`UN Region` = case_when(trimws(`Intermediate Region Name`) == "" ~ `Sub-region Name`, T ~ trimws(`Intermediate Region Name`))) %>% rename(ISO3 = `ISO-alpha3 Code`, Area = `Country or Area`) %>% filter(!is.na(ISO3), `UN Region` != "") %>% select(ISO3, `UN Region`)

ebrd_countries <- read.csv("../EBRD_countries.csv", check.names = F)
ebrdregion <- ebrd_countries %>% filter(`EBRD region` != "n/a") %>% rename(`EBRD Region` = `EBRD region`) %>% select(ISO3, `EBRD Region`)

iso3 <- cty_code %>% mutate(m49cd = paste0("'", sprintf("%03d", `M49 Code`))) %>% rename(ISO3 = `ISO-alpha3 Code`, Area = `Country or Area`) %>% select(m49cd, ISO3, Area) %>% left_join(unregion, by = "ISO3") %>% left_join(ebrdregion, by = "ISO3")

# readr::write_excel_csv(ebrdregion %>% group_by(`EBRD Region`) %>% summarise(`N countries` = n()) %>% ungroup(), paste0("../", "ebrd_regions.csv"))

## idx data
thisYear <- as.numeric(format(Sys.Date(), "%Y"))
thisMonth <- format(Sys.Date(), "%m")

# download file
tmp_file <- tempfile(fileext = ".zip")
dataURL <- "https://bulks-faostat.fao.org/production/Emissions_Totals_E_All_Data.zip"
download.file(dataURL, destfile=tmp_file, mode='wb')

# extract file
tmp_folder <- tempdir()
utils::unzip(tmp_file, exdir = tmp_folder)

# check files and folders
#list.files(tmp_folder)

# read raw faostat data with noflag
tmp_ghg <- read.csv(file.path(tmp_folder, "Emissions_Totals_E_All_Data_NOFLAG.csv"), check.names = F)
# get only co2eq emission totals from FAO TIER 1
tmp_co2eq <- tmp_ghg %>% filter(grepl("CO2eq", Element) & Source == "FAO TIER 1")
# select key columns + convert to tCO2eq
maxYear <- names(tmp_co2eq %>% select(starts_with("Y"))) %>% gsub("Y", "", .) %>% as.integer() %>% max(na.rm=T)
tmp_tCo2eq <- tmp_co2eq %>% select(`Area Code (M49)`, Item, Element, Y1992:paste0("Y",maxYear)) %>% mutate(across(Y1992:paste0("Y",maxYear), \(x) x*1e3))
# transpose year and element
tmp_long0 <- tmp_tCo2eq %>% tidyr::pivot_longer(Y1992:paste0("Y",maxYear), names_to = "Year", values_to = "tCO2eq") %>% mutate(Year = as.integer(gsub("Y", "", Year)))
tmp_long <- tmp_long0 %>% tidyr::pivot_wider(names_from = Element, values_from = "tCO2eq", values_fill = NA) %>% rename(Total = `Emissions (CO2eq) (AR5)`, N2O = `Emissions (CO2eq) from N2O (AR5)`, CH4 = `Emissions (CO2eq) from CH4 (AR5)`, `F-gases` = `Emissions (CO2eq) from F-gases (AR5)`)
# get only aggregate values
FAO_agrifood <- c("Farm gate", "Land Use change", "Pre- and Post- Production")
FAO_agg <- c("Emissions on agricultural land", "Emissions from crops", "Emissions from livestock", "Agrifood systems")
IPCC_agg <- c("IPCC Agriculture", "Agricultural Soils", "LULUCF", "AFOLU")
non_agrifood <- c("Energy", "IPPU", "Waste", "International bunkers", "Others")
# get selected aggregates
tmp_agg <- tmp_long %>% filter(Item %in% c(FAO_agrifood, FAO_agg, IPCC_agg, non_agrifood))

## fix na values (data-specific)
tmp_noNA <- tmp_agg %>% filter(!is.na(Total)) %>% tidyr::replace_na(list(N2O = 0, CH4 = 0, `F-gases` = 0))
#anyNA(tmp_noNA)

# add item groups
tmp_grouped <- tmp_noNA %>% mutate(Group = case_when(Item %in% FAO_agrifood ~ "Agrifood systems", Item %in% FAO_agg ~ "FAO aggregates", Item %in% IPCC_agg ~ "IPCC aggregates", T ~ "Not agri-food")) %>% mutate(sector = case_when(Item %in% c("IPCC Agriculture", "LULUCF", non_agrifood) ~ "yes", T ~ "no"), agrifood = case_when(Item %in% FAO_agrifood ~ "yes", T ~ "no")) %>% mutate(Item = case_when(Item == "IPCC Agriculture" ~ "Agriculture", Item == "Emissions from crops" ~ "Crops", Item == "Emissions from livestock" ~ "Livestock", T ~ Item))

tmp_wCO2 <- tmp_grouped %>% mutate(CO2 = Total - rowSums(across(c(N2O, CH4, `F-gases`)), na.rm=T))

# clean data
clean_data <- tmp_wCO2 %>% right_join(iso3, by = c("Area Code (M49)" = "m49cd")) %>% relocate(ISO3) %>% select(-`Area Code (M49)`) %>% select(ISO3, Area, `UN Region`, `EBRD Region`, Item, Group, sector, agrifood, Year, Total, CO2, N2O, CH4, `F-gases`)

## region average
out_unregion <- clean_data %>% group_by(`UN Region`, Item, Group, sector, agrifood, Year) %>% summarise(across(Total:`F-gases`, \(x) mean(x))) %>% ungroup() %>% filter(!is.na(`UN Region`))
out_ebrdregion <- clean_data %>% group_by(`EBRD Region`, Item, Group, sector, agrifood, Year) %>% summarise(across(Total:`F-gases`, \(x) mean(x))) %>% ungroup() %>% filter(!is.na(`EBRD Region`))

## final data
out_data <- clean_data %>% bind_rows(out_ebrdregion) %>% bind_rows(out_unregion)
out_data_long <- out_data %>% tidyr::pivot_longer(Total:`F-gases`, names_to = "gas", values_to = "tCO2eq")

# # saving as csv
# readr::write_excel_csv(out_data_long, paste0("../data/", "faostatghg_", thisYear, thisMonth, ".csv"))
# readr::write_excel_csv(out_data_long %>% filter(sector == "yes"), paste0("../data/", "faostatghg_sector_", thisYear, thisMonth, ".csv"))

##split into multiple csv
for (iso3 in unique(out_data_long$ISO3)){
  thisData <- out_data_long %>% filter(ISO3 == iso3 | Area == "World")
  thisUnReg <- thisData %>% filter(!is.na(`UN Region`)) %>% pull(`UN Region`) %>% unique()
  thisEbrdReg <- thisData %>% filter(!is.na(`EBRD Region`)) %>% pull(`EBRD Region`) %>% unique()

  if(length(thisUnReg) > 0){
    thisUnRegAvg <- out_data_long %>% filter(is.na(Area), `UN Region` == thisUnReg)
    thisData <- thisData %>% bind_rows(thisUnRegAvg)
  }
  
  if(length(thisEbrdReg) > 0){
    thisEbrdRegAvg <- out_data_long %>% filter(is.na(Area), `EBRD Region` == thisEbrdReg)
    thisData <- thisData %>% bind_rows(thisEbrdRegAvg)
  }
  
  readr::write_excel_csv(thisData, paste0("../data/faostat_ghg_by_country/", iso3, "_faostatghg.csv"))
}



