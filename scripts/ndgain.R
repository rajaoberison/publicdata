#' Script for downloading and processing ND-GAIN data
#' Developed by Andry Rajaoberison
#' Last updated: March 27, 2025
#' Contributors: ...
#' 
#' Necessary packages: utils, dplyr, rvest, readr
#' 
#' Data source: https://gain.nd.edu/our-work/country-index/download-data/
#' Output format: ISO3, index, rank, vulnerability, readiness
#' @source: \url{https://github.com/rajaoberison/publicdata}
library(dplyr)
library(rvest)

thisYear <- as.numeric(format(Sys.Date(), "%Y"))
thisMonth <- format(Sys.Date(), "%m")

# download file
tmp_file <- tempfile(fileext = ".zip")
## find url using rvest
ndgain_downloadPage <- read_html("https://gain.nd.edu/our-work/country-index/download-data/")
buttons <- ndgain_downloadPage %>% html_elements(".btn")
buttonTexts <- buttons %>% html_text()
downloadButton <- buttons[which(grepl("download", buttonTexts, T))[1]]
dataURL <- paste0("https://gain.nd.edu", downloadButton %>% html_attr("href"))
download.file(dataURL, destfile=tmp_file, mode='wb')

# extract file
tmp_folder <- tempdir()
utils::unzip(tmp_file, exdir = tmp_folder)

# check files and folders
# list.files(tmp_folder)

# read index data and choose latest value, and rank
tmp_index <- read.csv(file.path(tmp_folder, "resources", "gain", "gain.csv")) %>% select(1, 2, tail(names(.), 1)) %>% rename(index = 3) %>% mutate(rank = min_rank(desc(round(index, 1))))
q25 <- quantile(tmp_index$index, probs = 0.25, na.rm = T)
q75 <- quantile(tmp_index$index, probs = 0.75, na.rm = T)
tmp_class <- tmp_index %>% mutate(resilience = case_when(index < q25 ~ "Low", between(index, q25, q75) ~ "Moderate", index > q75 ~ "High", T ~ NA))
# read vulnerability data and choose latest value, then add rank
tmp_vuln <- read.csv(file.path(tmp_folder, "resources", "vulnerability", "vulnerability.csv")) %>% select(1, tail(names(.), 1)) %>% rename(vulnerability = 2) %>% mutate(rankV = min_rank(round(vulnerability, 3)))
# read readiness data and choose latest value
tmp_read <- read.csv(file.path(tmp_folder, "resources", "readiness", "readiness.csv")) %>% select(1, tail(names(.), 1)) %>% rename(readiness = 2) %>% mutate(rankR = min_rank(round(readiness, 3)))

# final data
out_data <- tmp_class %>% left_join(tmp_vuln, by = "ISO3") %>% left_join(tmp_read, by = "ISO3")
# saving as csv
readr::write_excel_csv(out_data, paste0("../data/", "ndgain_", thisYear, thisMonth, ".csv"))

