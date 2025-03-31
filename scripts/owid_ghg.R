#' Script for downloading and processing OWID GHG data
#' Developed by Andry Rajaoberison
#' Last updated: March 31, 2025
#' Contributors: ...
#' 
#' Necessary packages: utils, dplyr, tidyr, stringr, readr
#' 
#' Data source: https://www.fao.org/faostat/en/#data/GT
#' Output format: ISO3, Area, Item, Year, tCO2eq_N2O, tCO2eq_total, tCO2eq_CH4, tCO2eq_Fgases, Group, charted
#' @source: \url{https://github.com/rajaoberison/publicdata}
library(dplyr)

thisYear <- as.numeric(format(Sys.Date(), "%Y"))
thisMonth <- format(Sys.Date(), "%m")

# download file
tmp_file <- tempfile(fileext = ".csv")
dataURL <- "https://ourworldindata.org/grapher/ghg-emissions-by-sector.csv?v=1&csvType=full&useColumnShortNames=true"
download.file(dataURL, destfile=tmp_file, mode='wb')

# read raw owid data with noflag
tmp_ghg <- read.csv(tmp_file, check.names = F)

# inspec
#names(tmp_ghg)

# rename columns + pivot longer
tmp_long <- tmp_ghg %>% rename(Area = Entity, ISO3 = Code) %>% tidyr::pivot_longer(agriculture_ghg_emissions:aviation_and_shipping_ghg_emissions, names_to = "Sector", values_to = "tCO2eq")

# rename sectors
tmp_renamed <- tmp_long %>% mutate(Sector = gsub("_ghg_emissions", "", Sector)) %>% mutate(Sector = stringr::str_to_sentence(gsub("_", " ", Sector)))

# final data
out_data <- tmp_renamed %>% relocate(ISO3)
# saving as csv
readr::write_excel_csv(out_data, paste0("../data/", "owidghg_", thisYear, thisMonth, ".csv"))


