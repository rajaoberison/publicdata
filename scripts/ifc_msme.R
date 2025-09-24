#' Script for downloading and processing MSME indicators from IFC
#' Developed by Andry Rajaoberison
#' Last updated: September 15, 2025
#' Contributors: ...
#' 
#' Necessary packages: utils, dplyr, rvest, readr
#' 
#' Data source: https://www.smefinanceforum.org/data-sites/msme-country-indicators
#' Output format: ISO3, ...
#' @source: \url{https://github.com/rajaoberison/publicdata}
library(dplyr)
library(rvest)

thisYear <- as.numeric(format(Sys.Date(), "%Y"))
thisMonth <- format(Sys.Date(), "%m")

# download file
tmp_file <- tempfile(fileext = ".xlsx")
## find url using rvest
msme_downloadPage <- read_html("https://www.smefinanceforum.org/data-sites/msme-country-indicators")
clicks <- msme_downloadPage %>% html_elements("a")
clickRefs <- clicks %>% html_attr("href")
dbRef <- clickRefs[which(grepl("xlsx", clickRefs, T))[1]]
dataURL <- paste0("https://www.smefinanceforum.org", dbRef)
download.file(dataURL, destfile=tmp_file, mode='wb')

readxl::excel_sheets(tmp_file)
# read data
tmp_msme <- readxl::read_xlsx(tmp_file, sheet = "Latest Year Available", col_names = F)
## get colnames
colnames_df <- tmp_msme[1:2,] %>% t() %>% as.data.frame() %>% tibble::remove_rownames() %>% mutate(across(everything(), \(x) gsub("\r", "", x, fixed=T))) %>% mutate(across(everything(), \(x) gsub("\n", "", x, fixed=T))) %>% mutate(across(everything(), \(x) gsub(", ", ",", x, fixed=T))) %>% mutate(across(everything(), \(x) gsub(",", ", ", x, fixed=T)))
filledColnames <- colnames_df %>% tidyr::fill(V1, .direction = "down") %>% mutate(fixedColnames = paste(V1, "|", V2))
filledColnames[10:20,]
## fix colnames
tmp_fixed <- tmp_msme[3:nrow(tmp_msme),]
names(tmp_fixed) <- filledColnames %>% pull(fixedColnames)
## subset relevant data: # of msme
relVars <- c("Country Code | NA", "Year | NA", "Population, total | NA", "Number of Enterprises | Micro per 1, 000 people", "Number of Enterprises | SMEs per 1, 000 people", "Number of Enterprises | Large Ent. per 1, 000 people")
tmp_subset <- tmp_fixed %>% as_tibble(.name_repair = "unique") %>% filter(!is.na(`Country | NA`)) %>% select(all_of(relVars))
names(tmp_subset) <- c("ISO3", "year", "pop", "micro per 1000 people", "sme per 1000 people", "large per 1000 people")
# clean
tmp_clean <- tmp_subset %>% mutate(across(year:pop, \(x) as.integer(x))) %>% mutate(across(`micro per 1000 people`:`large per 1000 people`, \(x) as.numeric(x))) %>% rowwise() %>% mutate(`total per 1000 people` = sum(c(`micro per 1000 people`, `sme per 1000 people`, `large per 1000 people`), na.rm=T)) %>% filter(`total per 1000 people`>0)
tmp_long <- tmp_clean %>% tidyr::pivot_longer(`micro per 1000 people`:`total per 1000 people`, names_to = "type", values_to = "value")


# final data
out_data <- tmp_long

# saving as csv
readr::write_excel_csv(out_data, paste0("../data/", "ifc_msme_data.csv"))

