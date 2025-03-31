#' Script for downloading and processing FAOSTAT GHG data
#' Developed by Andry Rajaoberison
#' Last updated: March 31, 2025
#' Contributors: ...
#' 
#' Necessary packages: utils, dplyr, tidyr, readr
#' 
#' Data source: https://www.fao.org/faostat/en/#data/GT
#' Output format: ISO3, Area, Item, Year, tCO2eq_N2O, tCO2eq_total, tCO2eq_CH4, tCO2eq_Fgases, Group, charted
#' @source: \url{https://github.com/rajaoberison/publicdata}
library(dplyr)

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
tmp_tCo2eq <- tmp_co2eq %>% select(`Area Code (M49)`, Item, Element, Y1992:Y2050) %>% mutate(across(Y1992:Y2050, \(x) x*1e3))
# transpose year and element
tmp_long0 <- tmp_tCo2eq %>% tidyr::pivot_longer(Y1992:Y2050, names_to = "Year", values_to = "tCO2eq") %>% mutate(Year = as.integer(gsub("Y", "", Year)))
tmp_long <- tmp_long0 %>% tidyr::pivot_wider(names_from = Element, values_from = "tCO2eq", values_fill = NA) %>% rename(tCO2eq_total = `Emissions (CO2eq) (AR5)`, tCO2eq_N2O = `Emissions (CO2eq) from N2O (AR5)`, tCO2eq_CH4 = `Emissions (CO2eq) from CH4 (AR5)`, tCO2eq_Fgases = `Emissions (CO2eq) from F-gases (AR5)`)
# get only aggregate values
FAO_agrifood <- c("Farm gate", "Land Use change", "Pre- and Post- Production")
FAO_agg <- c("Emissions on agricultural land", "Emissions from crops", "Emissions from livestock", "Agrifood systems")
IPCC_agg <- c("IPCC Agriculture", "Agricultural Soils", "LULUCF", "AFOLU")
non_agrifood <- c("Energy", "IPPU", "Waste", "International bunkers", "Others")
# get selected aggregates
tmp_agg <- tmp_long %>% filter(Item %in% c(FAO_agrifood, FAO_agg, IPCC_agg, non_agrifood))
# add item groups
tmp_grouped <- tmp_agg %>% mutate(Group = case_when(Item %in% FAO_agrifood ~ "Agrifood systems", Item %in% FAO_agg ~ "FAO aggregates", Item %in% IPCC_agg ~ "FAO aggregates", T ~ "Not agri-food")) %>% mutate(charted = case_when(Item %in% c("IPCC Agriculture", "LULUCF", non_agrifood) ~ "yes", T ~ "no"))

# get iso3 code
cty_code <- read.csv("../country_codes.csv", check.names = F)
iso3 <- cty_code %>% mutate(m49cd = paste0("'", sprintf("%03d", `M49 Code`))) %>% rename(ISO3 = `ISO-alpha3 Code`, Area = `Country or Area`) %>% select(m49cd, ISO3, Area)

# final data
out_data <- tmp_grouped %>% left_join(iso3, by = c("Area Code (M49)" = "m49cd")) %>% relocate(ISO3) %>% select(-`Area Code (M49)`) %>% select(ISO3, Area, Item, Group, charted, Year, tCO2eq_total, tCO2eq_N2O, tCO2eq_CH4, tCO2eq_Fgases)
# saving as csv
readr::write_excel_csv(out_data, paste0("../data/", "faostatghg_", thisYear, thisMonth, ".csv"))
readr::write_excel_csv(out_data %>% filter(charted == "yes"), paste0("../data/", "faostatghg_chart_", thisYear, thisMonth, ".csv"))

