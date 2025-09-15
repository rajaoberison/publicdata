#' Script for getting wb indicators list
#' Developed by Andry Rajaoberison
#' Last updated: Sept 10, 2025
#' Contributors: ...
#' 
#' Necessary packages: utils, dplyr, readr
#' 
#' Data source: https://api.worldbank.org/
#' Output format: 
#' @source: \url{https://github.com/rajaoberison/publicdata}
library(dplyr)
library(rvest)

# full data
wbjson <- read_html("https://api.worldbank.org/v2/source/2/indicator?per_page=5000&format=json") %>% html_text()

# saving as json
writeLines(wbjson, "../wbIdxList.json")

