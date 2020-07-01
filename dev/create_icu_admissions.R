# Steve Harris
# 2020-07-01
# Create one row per admission

debug <- FALSE
library(tidyverse)
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
query <- "SELECT * FROM uds.icu_audit.bed_moves"
wdt <- DBI::dbGetQuery(ctn, query)
setDT(wdt)
colnames(wdt)

# Capture prev and subsequent departments
wdt[order(mrn,csn,bed_admission),department_prev := shift(department,1,type="lag"),by=.(mrn,csn)]
wdt[order(mrn,csn,bed_admission),department_next := shift(department,1,type="lead"),by=.(mrn,csn)]

setkey(wdt,mrn,csn,bed_admission)

# Drop bed level columns
tdt <- wdt[critcare==TRUE, .(
  mrn,
  csn,
  visit_occurrence_i,
  department,
  critcare_i,
  hospital_admission=csn_admission,
  hospital_discharge=csn_discharge,
  admission_icu=department_admission,
  discharge_icu=department_discharge
)]
# drop internal bed moves within critical care
tdt <- unique(tdt)
nrow(tdt)
setkey(tdt,admission_icu)

if (TRUE)  View(tdt[department == 'UCH T03 INTENSIVE CARE'])

# Better: write this back to the icu_audit schema (rather than saving locally)
table_path <- DBI::Id(schema="icu_audit", table="icu_admissions")
DBI::dbWriteTable(ctn, name=table_path, value=tdt, overwrite=TRUE)
DBI::dbDisconnect(ctn)
