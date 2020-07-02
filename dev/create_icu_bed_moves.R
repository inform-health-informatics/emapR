# Steve Harris
# 2020-06-30
# Test file to load and build bed moves
# Work specifically with census moves so drop non-census beds

debug <- FALSE
debug <- TRUE

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
setkey(rdt, mrn, admission)

wdt <- data.table::copy(rdt)
wdt[, hl7_location := NULL]
# FIXME: drop missing mrn
if (nrow(wdt[is.na(csn) | is.na(mrn)])) rlang::warn('!!! missing mrn or csn in bed_moves')
wdt <- wdt[!is.na(mrn) & !is.na(csn) ]

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
tdt <- wdt[!is.na(bed) & !(room %in% non_census) & !(bed %in% non_census)]

# define the discharge / end of observation for each mrn,csn; store
setkey(tdt, mrn, csn)
tdt[, csn_admission := min(admission),by=.(mrn,csn)]
tdt[, csn_discharge := max(discharge),by=.(mrn,csn)]
tdt[order(mrn,admission), census_moves_i := seq_len(.N), by=.(mrn,csn)]
tdt[, census_moves_N := .N, by=.(mrn,csn)]

# census bed moves
csn_bm <- tdt
# census visit occurrence
csn_vo <- tdt[admission == csn_admission | discharge == csn_discharge]


# # delete all discharges
# wdt[, discharge := NULL]
#
# # define jumps and delete everything else
# wdt[, location := paste(department, room, bed, sep='^')]
# wdt[, location_jump := NULL]
# wdt <- guidEHR::column_jump(wdt, 'location',
#                      order_vars=c('mrn', 'csn', 'admission'),
#                      group=c('mrn', 'csn'),
#                      col_jump = 'location_jump')
# wdt <- wdt[is.na(location_jump) | location_jump == TRUE]
# wdt[, location_jump := NULL]
# wdt
# # create a discharge time from the next admission
# setkey(wdt,mrn,csn,admission)
# wdt[, discharge := shift(admission, type='lead'),by=.(mrn,csn)]
#
# # use the stored end of observation to complete the discharge for the last step
# wdt[is.na(discharge), discharge := csn_discharge]
# setcolorder(wdt, c('mrn', 'csn', 'admission', 'discharge'))
# wdt[, location := NULL]


setkey(wdt, admission)
if (debug) wdt[mrn == '40991395']
if (debug) View(wdt[mrn == '40991395'])
if (debug) bm_csn[mrn == '40991395']
if (debug) View(wdt)

# First find all MRNs that have been to a critical care area
critical_care_departments <- c(
  "UCH T03 INTENSIVE CARE",
  "UCH P03 CV",
  "UCH T07 HDRU",
  "WMS W01 CRITICAL CARE"
  )

wdt[, critcare := department %in% critical_care_departments]
wdt[, census := TRUE]
wdt[is.na(bed) | (room %in% non_census) | (bed %in% non_census), census := FALSE]


# List of mrns and csns that have been through critical care
mrn_csn <- unique(wdt[critcare == TRUE][,.(mrn,csn)])
# mrn_csn[order(mrn,csn), visit_occurrence_i := seq_len(.N), by=mrn]


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
tdt <- wdt[mrn_csn, on=c("mrn==mrn", "csn==csn")][order(mrn,admission)]
head(tdt)

# NOTE: switched to using CSN for grouping to permit department to reset between admissions
if (debug) {
  xdt <- tdt[mrn == '03036594'] # two hospital admissions, three critcare admissions
  # column_jump(xdt[census==TRUE],col='department', order_vars = c('mrn', 'admission'), group = c('mrn', 'csn'), col_jump='foo')
  xdt <- guidEHR::collapse_over(xdt[census==TRUE],col='department',
                in_time = 'admission',
                out_time = 'discharge',
                order_vars = c('mrn', 'admission'),
                group = c('csn'))
  print(xdt)
}

# Now collapse by department to appropriately define department level moves
# FIXME: this seems to require me to reload / rebuild the package each time
udt <- guidEHR::collapse_over(tdt[census == TRUE],
                              col='department',
                              in_time='admission',
                              out_time='discharge',
                              order_vars=c('mrn','admission'),
                              group='csn',
                              time_jump_window=dhours(8) # Arbitrary but join stays where less than 8 hours
                              )


if (debug) udt[mrn == '40991395'] # one critcare admission with a trip to theatre during
if (debug) udt[mrn == '00054561'] # two critcare admissions with a short discharge to the ward
if (debug) udt[mrn == '40966136'] # two critcare admissions with a trip to endoscopy during the 2nd
if (debug) udt[mrn == '03036594'] # two hospital admissions, three critcare admissions

# Now label up separate critical care stays
vdt <- unique(udt[critcare == TRUE, .(mrn,csn,department,department_i)])
vdt[order(mrn,csn,department_i), critcare_i := seq_len(.N), by=.(mrn,csn)]
vdt <- vdt[,.(mrn,csn,critcare_i,department_i)][udt, on=c("mrn", "csn", "department_i")]

if (debug) vdt[mrn == '03036594']

# Now join back on to tdt
# str(tdt)
# str(vdt)
tdt <- vdt[tdt, on=.NATURAL]
if (debug) tdt[mrn == '03036594']

# Now join on CSN dates etc
if (debug) csn_vo[mrn == '03036594']
tdt <- csn_bm[tdt, on=.NATURAL]
if (debug) tdt[mrn == '03036594']


setnames(tdt, 'admission', 'bed_admission')
setnames(tdt, 'discharge', 'bed_discharge')

# Better: write this back to the icu_audit schema (rather than saving locally)
table_path <- DBI::Id(schema="icu_audit", table="bed_moves")
DBI::dbWriteTable(ctn, name=table_path, value=tdt, overwrite=TRUE)

# DBI::dbDisconnect(ctn)


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
