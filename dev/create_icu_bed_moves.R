# Steve Harris
# 2020-06-30
# Test file to load and build bed moves
# Work specifically with census moves so drop non-census beds

debug <- FALSE

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
rdt <- DBI::dbGetQuery(ctn, query)
setDT(rdt)
wdt <- data.table::copy(rdt)

# drop all non-census steps from this Hand crafted list
non_census <- c(
  "PATIENT OFFSITE",
  "HOLDING BAY",
  "POOL ROOM",
  "NONE",
  "WAIT",
  "THR",
  "ENDO",
  "ARRIVED",
  "DISCHARGE",
  "READY",
  "HOME",
  "VIRTUAL"
)
# Drop non-census areas
wdt <- wdt[!is.na(bed)]
wdt <- wdt[!(room %in% non_census) & !(bed %in% non_census)]

# define the discharge / end of observation for each mrn,csn; store
wdt[, csn_admission := min(discharge),by=.(mrn,csn)]
wdt[, csn_discharge := max(discharge),by=.(mrn,csn)]

# delete all discharges
wdt[, discharge := NULL]

# define jumps and delete everything else
wdt[, location := paste(department, room, bed, sep='^')]
wdt[, location_jump := NULL]
wdt <- guidEHR::column_jump(wdt, 'location',
                     order_vars=c('mrn', 'csn', 'admission'),
                     group=c('mrn', 'csn'),
                     col_jump = 'location_jump')
wdt <- wdt[is.na(location_jump) | location_jump == TRUE]
wdt[, location_jump := NULL]
wdt
# create a discharge time from the next admission
setkey(wdt,mrn,csn,admission)
wdt[, discharge := shift(admission, type='lead'),by=.(mrn,csn)]

# use the stored end of observation to complete the discharge for the last step
wdt[is.na(discharge), discharge := csn_discharge]
setcolorder(wdt, c('mrn', 'csn', 'admission', 'discharge'))
wdt[, location := NULL]


if (debug) wdt[mrn == '40991395']
if (debug) View(wdt)


# First find all MRNs that have been to a critical care area
critical_care_departments <- c(
  "UCH T03 INTENSIVE CARE",
  "UCH P03 CV",
  "UCH T07 HDRU",
  "WMS W01 CRITICAL CARE"
  )

wdt[, critcare := department %in% critical_care_departments]
mrn_csn <- unique(wdt[critcare == TRUE][,.(mrn,csn)])
mrn_csn[order(mrn,csn), visit_occurrence_i := seq_len(.N), by=mrn]


# then select all bedmoves related to those patients
if (debug) {
  tdt <- wdt[mrn_csn, on=c("mrn==mrn", "csn==csn")][order(mrn,admission)]
  tdt[order(mrn,csn,admission),
      discharge_delta := admission - shift(discharge, type='lag'),
      by=.(mrn,csn)]
  # inspect cases where there are gaps or overlaps
  tdt[mrn %in% tdt[discharge_delta > 0][,mrn]]
  # TODO: report: there seem to be cases where there is an overlap and the patient skips forward/back
  tdt[mrn == '21203433']

}
wdt <- wdt[mrn_csn, on=c("mrn==mrn", "csn==csn")][order(mrn,admission)]
head(wdt)



# Now collapse by department to appropriately define department level moves
# FIXME: this seems to require me to reload / rebuild the package each time
tdt <- guidEHR::collapse_over(wdt,
                              col='department',
                              in_time='admission',
                              out_time='discharge',
                              order_vars=c('mrn','admission'),
                              group='mrn',
                              time_jump_window=dhours(8) # Arbitrary but join stays where less than 8 hours
                              )

# Now label up separate critical care stays
udt <- unique(tdt[critcare == TRUE, .(mrn,csn,department,department_i)])
udt[order(mrn,csn,department_i), critcare_i := seq_len(.N), by=.(mrn,csn)]
udt
tdt <- udt[,.(mrn,csn,critcare_i,department_i)][tdt, on=c("mrn", "csn", "department_i")]

if (debug) View(tdt)

colnames(tdt)
setnames(tdt, 'admission', 'bed_admission')
setnames(tdt, 'discharge', 'bed_discharge')
setcolorder(tdt, c(
            "mrn",
            "csn",
            "visit_occurrence_i",
            "department_i",
            "critcare_i",
            "department_admission",
            "department_discharge",
            "bed_admission",
            "bed_discharge",
            "critcare",
            "department"
            ))
tdt

# Better: write this back to the icu_audit schema (rather than saving locally)
table_path <- DBI::Id(schema="icu_audit", table="bed_moves")
DBI::dbWriteTable(ctn, name=table_path, value=tdt, overwrite=TRUE)
DBI::dbDisconnect(ctn)


stop()

# now test and check
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
# THis query returns approx 1e6 rows so it takes a minute or so
query <- "SELECT * FROM uds.icu_audit.bed_moves"
wdt <- DBI::dbGetQuery(ctn, query)
setDT(wdt)

# manual check current
wdt[critcare==TRUE & is.na(bed_discharge)]
wdt[critcare==TRUE & is.na(bed_discharge) & department == "UCH T03 INTENSIVE CARE"][order(bed)]

# look for repeated admissions
setkey(wdt, mrn, admission)
wdt[critcare==TRUE & is.na(discharge) & department == "UCH T03 INTENSIVE CARE"][order(bed)]
wdt[mrn == '21203433']
tdt[mrn == '21203433']


# Compare vs Epic's ICU stay table
query <- "SELECT * FROM uds.icu_audit.icu_stay"
xdt <- DBI::dbGetQuery(ctn, query)
setDT(xdt)
xdt[, pat_enc_csn_id := as.character(pat_enc_csn_id)]

ydt <- unique(wdt[critcare==TRUE, .(mrn,csn,department_admission,department_discharge)])
ydt <- xdt[ydt, on="pat_enc_csn_id==csn"]
# ydt <- xdt[ydt, on="pat_enc_csn_id==csn", nomatch=0]
ydt[, discharge_diff := department_discharge - icu_stay_end_dttm]
ydt[, admission_diff := department_admission - icu_stay_start_dttm]

openxlsx::write.xlsx(ydt, file='dev/tmp/star_vs_caboodle_bed_moves.xlsx')
