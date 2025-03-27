#' Script for downloading and processing INFORM RISK data
#' Developed by Andry Rajaoberison
#' Last updated: March 27, 2025
#' Contributors: ...
#' 
#' Necessary packages: readxl, dplyr
#' 
#' Data source: https://drmkc.jrc.ec.europa.eu/inform-index/INFORM-Risk
#' Output format: ISO3, INFORM RISK, RISK CLASS, Rank, HAZARD & EXPOSURE, VULNERABILITY, LACK OF COPING CAPACITY
#' @source: \url{https://github.com/rajaoberison/publicdata}
library(dplyr)

thisYear <- format(Sys.Date(), "%Y")
thisMonth <- format(Sys.Date(), "%m")

# download file
tmp_file <- tempfile(fileext = ".xlsx")
dataURL <- paste0("https://drmkc.jrc.ec.europa.eu/inform-index/Portals/0/InfoRM/2025/INFORM_Risk_", thisYear, "_v069.xlsx")
download.file(dataURL, destfile=tmp_file, mode='wb')

# check tabs
# readxl::excel_sheets(tmp_file)

# read main tab
tmp_data <- readxl::read_xlsx(tmp_file, sheet = "INFORM Risk 2025 (a-z)")
# remove first row
tmp_data <- tmp_data[-1,]
# final data
out_data <- tmp_data %>% select(ISO3, `INFORM RISK`, `RISK CLASS`, Rank, `HAZARD & EXPOSURE`, VULNERABILITY, `LACK OF COPING CAPACITY`)
# saving as csv
readr::write_excel_csv(out_data, paste0("../data/", "informrisk_", thisYear, thisMonth, ".csv"))

