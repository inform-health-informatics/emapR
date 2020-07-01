# Steve Harris
# 2020-06-30
# Test file to load and build bed moves

# library(tidyverse)
library(lubridate)
library(data.table)

# Grab user name and password. I store my 'secrets' in an environment file that
# remains out of version control (.Renviron). You can see an example in
# 'example-config-files'. The .Renviron should be at the root of your project or
# in your 'home'.
ctn <- DBI::dbConnect(RPostgres::Postgres(),
                      host = Sys.getenv("UDS_HOST"),
                      port = 5432,
                      user = Sys.getenv("UDS_USER"),
                      password = Sys.getenv("UDS_PWD"),
                      dbname = "uds")


# Load bed moves
# ==============
# THis query returns approx 1e6 rows so it takes a minute or so
query <- "SELECT * FROM uds.star.bed_moves"
wdt <- DBI::dbGetQuery(ctn, query)
setDT(wdt)

# First find all MRNs that have been to a critical care area
critical_care_departments <- c(
  "UCH T03 INTENSIVE CARE",
  "UCH P03 CV",
  "UCH T07 HDRU",
  "WMS W01 CRITICAL CARE"
  )

wdt[, critcare := department %in% critical_care_departments]
mrncc <- unique(wdt[critcare == TRUE][,.(mrn)])

# then select all bedmoves related to those patients
wdt <- wdt[mrncc, on="mrn"][order(mrn,admission)]
head(wdt)


# Now collapse by department to appropriately define department level moves
wdt <- guidEHR::collapse_over(wdt,
                              col='department',
                              in_time='admission',
                              out_time='discharge',
                              order_vars=c('mrn','admission'),
                              group='mrn')

head(wdt)

# Better: write this back to the icu_audit schema (rather than saving locally)
table_path <- DBI::Id(schema="icu_audit", table="bed_moves")
DBI::dbWriteTable(ctn, name=table_path, value=wdt, overwrite=TRUE)


