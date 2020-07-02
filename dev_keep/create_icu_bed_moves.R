# Steve Harris
# 2020-07-02

# *************
# Configuration
# *************

# debug <- TRUE
debug <- FALSE

# Script extracts all bed movements for patients moving through a particular ward
# the example below is for critical care areas but it can be adapted

# Input: uds.star.bed_moves (custom helper view)
input_table <- 'uds.star.bed_moves'

# Output: uds.icu_audit.bed_moves
target_schema <- 'icu_audit'
target_table <- 'bed_moves'
target_table_path <- DBI::Id(schema=target_schema, table=target_table)

# Period of interest
period_min <- ymd_hms('2020-02-01 00:00:00')
period_max <- now()

# List the departments or wards that you wish to extract data for
wards_of_interest <- c(
  "UCH T03 INTENSIVE CARE",
  "UCH P03 CV",
  "UCH T07 HDRU",
  "WMS W01 CRITICAL CARE"
)

# Beds and Rooms that are non-census (i.e. patient doesn't actually occupy a bed)
# Hand crafted: needs verification
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
query <- paste("SELECT * FROM", input_table)
rlang::warn('--- slow query; be patient')
rdt <- DBI::dbGetQuery(ctn, query)
setDT(rdt)
setkey(rdt, mrn, admission)

# make a copy of the data so you can debug without having to re-run the query
wdt <- data.table::copy(rdt)
wdt <- wdt[admission > period_min & admission < period_max]
wdt[, hl7_location := NULL]

# FIXME: drop missing mrn
if (nrow(wdt[is.na(csn) | is.na(mrn)])) rlang::warn('!!! missing mrn or csn in bed_moves')
wdt <- wdt[!is.na(mrn) & !is.na(csn) ]

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

# Set-up census variable
setkey(wdt, admission)
wdt[, critcare := department %in% wards_of_interest]
wdt[, census := TRUE]
wdt[is.na(bed) | (room %in% non_census) | (bed %in% non_census), census := FALSE]

# First find all MRNs that have been to a critical care area
mrn_csn <- unique(wdt[critcare == TRUE][,.(mrn,csn)])
# Now use that list to filter *all* bed moves involving those patients
tdt <- wdt[mrn_csn, on=c("mrn==mrn", "csn==csn")][order(mrn,admission)]

# Now collapse by department to appropriately define department level moves
# FIXME: this seems to require me to reload / rebuild the package each time
# manually get data.table imported into the package
notfixed <- TRUE
if (notfixed) {
  library(devtools)
  document()
  build()
  install()
}
udt <- guidEHR::collapse_over(tdt[census == TRUE],
                              col='department',
                              in_time='admission',
                              out_time='discharge',
                              order_vars=c('mrn','admission'),
                              group='csn',
                              time_jump_window=dhours(8) # Arbitrary but join stays where less than 8 hours
)

# Now label up separate critical care stays
vdt <- unique(udt[critcare == TRUE, .(mrn,csn,department,department_i)])
vdt[order(mrn,csn,department_i), critcare_i := seq_len(.N), by=.(mrn,csn)]
vdt <- vdt[,.(mrn,csn,critcare_i,department_i)][udt, on=c("mrn", "csn", "department_i")]

# Now join back on to tdt
tdt <- vdt[tdt, on=.NATURAL]

# Now join on CSN dates etc
tdt <- csn_bm[tdt, on=.NATURAL]

setnames(tdt, 'admission', 'bed_admission')
setnames(tdt, 'discharge', 'bed_discharge')

# number bed moves
tdt[order(mrn,bed_admission), bed_i := seq_len(.N), by=.(mrn)]
setkey(tdt,bed_admission)

# Write this back to the icu_audit schema (rather than saving locally)
DBI::dbWriteTable(ctn, name=target_table_path, value=tdt, overwrite=TRUE)
DBI::dbDisconnect(ctn)

