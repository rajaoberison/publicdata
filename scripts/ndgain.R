#' Script for downloading and processing ND-GAIN data
#' Developed by Andry Rajaoberison
#' Last updated: March 27, 2025
#' Contributors: ...
#' 
#' Necessary packages: utils, dplyr
#' 
#' Data source: https://gain.nd.edu/our-work/country-index/download-data/
#' Output format: ISO3, index, rank, vulnerability, readiness
#' @source: \url{https://github.com/rajaoberison/publicdata}
library(dplyr)

thisYear <- as.numeric(format(Sys.Date(), "%Y"))
thisMonth <- format(Sys.Date(), "%m")

# download file
tmp_file <- tempfile(fileext = ".zip")
dataURL <- paste0("https://gain.nd.edu/assets/581929/nd_gain_countryindex_", thisYear-1, ".zip")
download.file(dataURL, destfile=tmp_file, mode='wb')

# extract file
tmp_folder <- tempdir()
utils::unzip(tmp_file, exdir = tmp_folder)

# check files and folders
# list.files(tmp_folder)

# read index data and choose latest value, and rank
tmp_index <- read.csv(file.path(tmp_folder, "resources", "gain", "gain.csv")) %>% select(1, tail(names(.), 1)) %>% rename(index = 2) %>% mutate(rank = rank(desc(round(index, 1)), ties.method = "min"))
# read vulnerability data and choose latest value
tmp_vuln <- read.csv(file.path(tmp_folder, "resources", "vulnerability", "vulnerability.csv")) %>% select(1, tail(names(.), 1)) %>% rename(vulnerability = 2)
# read readiness data and choose latest value
tmp_read <- read.csv(file.path(tmp_folder, "resources", "readiness", "readiness.csv")) %>% select(1, tail(names(.), 1)) %>% rename(readiness = 2)

# final data
out_data <- tmp_index %>% left_join(tmp_vuln, by = "ISO3") %>% left_join(tmp_read, by = "ISO3")
# saving as csv
readr::write_excel_csv(out_data, paste0("../data/", "ndgain_", thisYear, thisMonth, ".csv"))

