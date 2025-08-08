#' Script for downloading and processing OECD CRS data
#' Developed by Andry Rajaoberison
#' Last updated: July 28, 2025
#' Contributors: ...
#' 
#' Necessary packages: utils, dplyr, tidyr, readr, nanoparquet
#' 
#' Data source: https://data-explorer.oecd.org/vis?tm=crs&pg=0&fc=Topic&snb=26&df[ds]=dsDisseminateFinalDMZ&df[id]=DSD_CRS%40DF_CRS&df[ag]=OECD.DCD.FSD&df[vs]=1.4&dq=........_T..&pd=%2C&to[TIME_PERIOD]=false
#' Output format: ISO3, Area, Item, Year, tCO2eq_N2O, tCO2eq_total, tCO2eq_CH4, tCO2eq_Fgases, Group, charted
#' @source: \url{https://github.com/rajaoberison/publicdata}
library(dplyr)
library(nanoparquet)

thisYear <- as.numeric(format(Sys.Date(), "%Y"))
thisMonth <- format(Sys.Date(), "%m")

# download file
tmp_file <- tempfile(fileext = ".zip")
dataURL <- "https://stats.oecd.org/wbos/fileview2.aspx?IDFile=50f0355e-8f61-4230-85f3-90b4db45bfc9"
# parquet file is heavy ~1Gb, so update timeout
options(timeout=2048)
# start download
download.file(dataURL, destfile=tmp_file, mode='wb')

# extract file
tmp_folder <- tempdir()
utils::unzip(tmp_file, exdir = tmp_folder)

# check files and folders
#list.files(tmp_folder)

# read raw CRS data (reduced version)
tmp_crs <- read_parquet(file.path(tmp_folder, "CRS.parquet"))
# get only data from 1992
tmp_recent <- tmp_crs %>% 
  filter(year >= 1992) %>% 
  # add sector code groups
  mutate(sector_group = case_when(
    startsWith(sector_name, "I.1.") ~ "Education",
    startsWith(sector_name, "I.2.") ~ "Health",
    startsWith(sector_name, "I.3.") ~ "Population Policies/Programmes",
    startsWith(sector_name, "I.4.") ~ "Water Supply & Sanitation",
    startsWith(sector_name, "I.5.") ~ "Government & Civil Society",
    startsWith(sector_name, "I.6.") ~ "Other Social Infrastructure & Services",
    sector_name == "I. Social Infrastructure & Services" ~ "Other Social Infrastructure & Services",
    startsWith(sector_name, "II.1.") ~ "Transport & Storage",
    startsWith(sector_name, "II.2.") ~ "Communications",
    startsWith(sector_name, "II.3.") ~ "Energy",
    startsWith(sector_name, "II.4.") ~ "Banking & Financial Services",
    startsWith(sector_name, "II.5.") ~ "Business & Other Services",
    sector_name == "II. Economic Infrastructure & Services" ~ "Other Economic Infrastructure & Services",
    startsWith(sector_name, "III.1.") ~ "Agriculture, Forestry, Fishing",
    startsWith(sector_name, "III.2.") ~ "Industry, Mining, Construction",
    sector_name == "III.3.a. Trade Policies & Regulations" ~ "Trade Policies & Regulations",
    sector_name == "III.3.b. Tourism" ~ "Tourism",
    startsWith(sector_name, "IV.1.") ~ "General Environment Protection",
    startsWith(sector_name, "IV.2.") ~ "Other Multisector",
    sector_name == "IV. Multi-Sector / Cross-Cutting" ~ "Other Multisector",
    sector_name == "IX. Unallocated / Unspecified" ~ "Unallocated / Unspecified",
    sector_name == "Sectors not specified" ~ "Unallocated / Unspecified",
    startsWith(sector_name, "VI.1.") ~ "General Budget Support",
    startsWith(sector_name, "VI.2.") ~ "Development Food Assistance",
    startsWith(sector_name, "VI.3.") ~ "Other Commodity Assistance",
    sector_name == "VI. Commodity Aid / General Programme Assistance" ~ "Other Commodity Assistance",
    sector_name == "VII. Action Relating to Debt" ~ "Action Relating to Debt",
    startsWith(sector_name, "VIII.1.") ~ "Emergency Response",
    startsWith(sector_name, "VIII.2.") ~ "Reconstruction Relief & Rehabilitation",
    startsWith(sector_name, "VIII.3.") ~ "Disaster Prevention & Preparedness",
    startsWith(sector_name, "VIII.1.") ~ "Other Humanitarian Aid",
    T ~ sector_name
  ))

## get only recipient countries
cty_codes <- read.csv("../country_codes.csv", check.names = F) %>% pull("ISO-alpha3 Code")
tmp_cty <- tmp_recent %>% filter(de_recipientcode %in% cty_codes)

## aggregate values
## ignoring "sector_name" and "purpose_name"
tmp_agg <- tmp_cty %>% select(de_recipientcode, recipient_name, de_donorcode, donor_name, flow_name, sector_group, sector_name, purpose_name, climate_adaptation, climate_mitigation, biodiversity, desertification, year, usd_disbursement, usd_disbursement_defl) %>% group_by(de_recipientcode, recipient_name, de_donorcode, donor_name, flow_name, sector_group, climate_adaptation, climate_mitigation, biodiversity, desertification, year) %>% summarise(across(usd_disbursement:usd_disbursement_defl, \(x) sum(x, na.rm=T))) %>% ungroup()

# final data
out_data <- tmp_agg %>% mutate(across(climate_adaptation:desertification, \(x) case_when(x == 0 ~ "not targeted", x == 0 ~ "significant objective", x == 2 ~ "principal objective", T ~ "not screened")))
## climate finance
out_data_climate <- out_data %>% group_by(de_recipientcode, donor_name, flow_name, sector_group, year) %>% summarise(across(usd_disbursement:usd_disbursement_defl, \(x) sum(x, na.rm=T))) %>% ungroup()
## only by donor
out_data_donor <- out_data %>% group_by(de_recipientcode, recipient_name, de_donorcode, donor_name, year) %>% summarise(across(usd_disbursement:usd_disbursement_defl, \(x) sum(x, na.rm=T))) %>% ungroup()
## only by flow type
out_data_flowtype <- out_data %>% group_by(de_recipientcode, recipient_name, flow_name, year) %>% summarise(across(usd_disbursement:usd_disbursement_defl, \(x) sum(x, na.rm=T))) %>% ungroup()
## only by sector
out_data_sector <- out_data %>% group_by(de_recipientcode, recipient_name, sector_group, year) %>% summarise(across(usd_disbursement:usd_disbursement_defl, \(x) sum(x, na.rm=T))) %>% ungroup()

# saving as csv
#readr::write_excel_csv(out_data, paste0("../data/", "oecdcrs_", thisYear, thisMonth, ".csv"))
readr::write_excel_csv(out_data_climate, paste0("../data/", "oecdcrs_climate_", thisYear, thisMonth, ".csv"))
readr::write_excel_csv(out_data_donor, paste0("../data/", "oecdcrs_donors_", thisYear, thisMonth, ".csv"))
readr::write_excel_csv(out_data_flowtype, paste0("../data/", "oecdcrs_flowtype_", thisYear, thisMonth, ".csv"))
readr::write_excel_csv(out_data_sector, paste0("../data/", "oecdcrs_sectors_", thisYear, thisMonth, ".csv"))

## for climate data
##split into multiple csv
for (iso3 in unique(out_data_climate$de_recipientcode)){
  readr::write_excel_csv(out_data_climate %>% filter(de_recipientcode == iso3), paste0("../data/oecd_crs_by_country/", iso3, "_oecdcrs_climate_", thisYear, thisMonth, ".csv"))
}

# 
# ### analysis for Central Asia
# tmp_crs <- read_parquet("~/Downloads/oecd/CRS.parquet")
# ##
# dt1 <- tmp_crs %>% 
#   filter(flow_name %in% c("ODA Loans", "ODA Grants")) %>% 
#   filter(recipient_code %in% 613:617) %>% 
#   select(recipient_name, donor_name, flow_name, sector_name, purpose_name, year, usd_disbursement_defl) %>% mutate(sector_group = case_when(
#     startsWith(sector_name, "I.1.") ~ "Education",
#     startsWith(sector_name, "I.2.") ~ "Health",
#     startsWith(sector_name, "I.3.") ~ "Population Policies/Programmes",
#     startsWith(sector_name, "I.4.") ~ "Water Supply & Sanitation",
#     startsWith(sector_name, "I.5.") ~ "Government & Civil Society",
#     startsWith(sector_name, "I.6.") ~ "Other Social Infrastructure & Services",
#     sector_name == "I. Social Infrastructure & Services" ~ "Other Social Infrastructure & Services",
#     startsWith(sector_name, "II.1.") ~ "Transport & Storage",
#     startsWith(sector_name, "II.2.") ~ "Communications",
#     startsWith(sector_name, "II.3.") ~ "Energy",
#     startsWith(sector_name, "II.4.") ~ "Banking & Financial Services",
#     startsWith(sector_name, "II.5.") ~ "Business & Other Services",
#     sector_name == "II. Economic Infrastructure & Services" ~ "Other Economic Infrastructure & Services",
#     startsWith(sector_name, "III.1.") ~ "Agriculture, Forestry, Fishing",
#     startsWith(sector_name, "III.2.") ~ "Industry, Mining, Construction",
#     sector_name == "III.3.a. Trade Policies & Regulations" ~ "Trade Policies & Regulations",
#     sector_name == "III.3.b. Tourism" ~ "Tourism",
#     startsWith(sector_name, "IV.1.") ~ "General Environment Protection",
#     startsWith(sector_name, "IV.2.") ~ "Other Multisector",
#     sector_name == "IV. Multi-Sector / Cross-Cutting" ~ "Other Multisector",
#     sector_name == "IX. Unallocated / Unspecified" ~ "Unallocated / Unspecified",
#     sector_name == "Sectors not specified" ~ "Unallocated / Unspecified",
#     startsWith(sector_name, "VI.1.") ~ "General Budget Support",
#     startsWith(sector_name, "VI.2.") ~ "Development Food Assistance",
#     startsWith(sector_name, "VI.3.") ~ "Other Commodity Assistance",
#     sector_name == "VI. Commodity Aid / General Programme Assistance" ~ "Other Commodity Assistance",
#     sector_name == "VII. Action Relating to Debt" ~ "Action Relating to Debt",
#     startsWith(sector_name, "VIII.1.") ~ "Emergency Response",
#     startsWith(sector_name, "VIII.2.") ~ "Reconstruction Relief & Rehabilitation",
#     startsWith(sector_name, "VIII.3.") ~ "Disaster Prevention & Preparedness",
#     startsWith(sector_name, "VIII.1.") ~ "Other Humanitarian Aid",
#     T ~ sector_name
#   ))
# ##
# #summary by country
# ex1 <- dt1 %>% 
#   filter(year %in% 1991:2018) %>% 
#   group_by(recipient_name) %>% 
#   summarise(ODA = sum(usd_disbursement_defl, na.rm=T)) %>% 
#   ungroup()
# ex2 <- dt1 %>% 
#   filter(year %in% 2019:2023) %>% 
#   group_by(recipient_name) %>% 
#   summarise(ODA = sum(usd_disbursement_defl, na.rm=T)) %>% 
#   ungroup()
# 
# # oda by year
# ex3 <- dt1 %>% 
#   group_by(recipient_name, year) %>% 
#   summarise(ODA = sum(usd_disbursement_defl, na.rm=T)) %>% 
#   ungroup() %>% 
#   tidyr::pivot_wider(names_from = "recipient_name", values_from = "ODA")
# 
# # ODA type
# ex4 <- dt1 %>% 
#   filter(year %in% 1991:2018) %>% 
#   group_by(recipient_name, flow_name) %>% 
#   summarise(ODA = sum(usd_disbursement_defl, na.rm=T)) %>% 
#   ungroup() %>% 
#   tidyr::pivot_wider(names_from = "flow_name", values_from = "ODA")
# 
# ex5 <- dt1 %>% 
#   filter(year %in% 2019:2023) %>% 
#   group_by(recipient_name, flow_name) %>% 
#   summarise(ODA = sum(usd_disbursement_defl, na.rm=T)) %>% 
#   ungroup() %>% 
#   tidyr::pivot_wider(names_from = "flow_name", values_from = "ODA")
# 
# # top providers
# ex6 <- dt1 %>% 
#   filter(year %in% 1991:2018) %>% 
#   group_by(recipient_name, donor_name) %>% 
#   summarise(ODA = sum(usd_disbursement_defl, na.rm=T)) %>% 
#   slice_max(order_by = ODA, n = 10) %>% 
#   ungroup()# %>% 
# #filter("Kazakhstan") %>% 
# #tidyr::pivot_wider(names_from = "flow_name", values_from = "ODA")
# 
# ex7 <- dt1 %>% 
#   filter(year %in% 2019:2023) %>% 
#   group_by(recipient_name, donor_name) %>% 
#   summarise(ODA = sum(usd_disbursement_defl, na.rm=T)) %>% 
#   slice_max(order_by = ODA, n = 10) %>% 
#   ungroup()
# 
# # top sectors
# ex8 <- dt1 %>% 
#   filter(year %in% 1991:2018) %>% 
#   group_by(recipient_name, sector_group) %>% 
#   summarise(ODA = sum(usd_disbursement_defl, na.rm=T)) %>% 
#   slice_max(order_by = ODA, n = 20) %>% 
#   ungroup()
# 
# ex9 <- dt1 %>% 
#   filter(year %in% 2019:2023) %>% 
#   group_by(recipient_name, sector_group) %>% 
#   summarise(ODA = sum(usd_disbursement_defl, na.rm=T)) %>% 
#   slice_max(order_by = ODA, n = 20) %>% 
#   ungroup()
# 
# ##


