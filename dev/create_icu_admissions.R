# Steve Harris
# 2020-07-01
# Create one row per admission

debug <- FALSE

library(tidyverse)
library(lubridate)
library(data.table)
library(guidEHR)

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

# number bed moves
wdt[order(mrn,bed_admission), bed_i := seq_len(.N), by=.(mrn)]

# Capture prev and subsequent (census) departments
# 1 row per department
udt <- unique(wdt[census==TRUE,.(mrn,csn,department,department_i)])
udt[,department_prev := shift(department,1,type="lag"),by=.(mrn,csn)]
udt[,department_next := shift(department,1,type="lead"),by=.(mrn,csn)]
udt <- udt[wdt, on=.NATURAL]


if (TRUE) udt[mrn == '03036594'] # two hospital admissions, three critcare admissions

tdt <- udt[order(bed_admission)][critcare_i == 1, .SD[1] , by=.(mrn,csn)]
tdt <- tdt[,.(mrn,csn,
       hospital_admission=csn_admission,
       hospital_discharge=csn_discharge,
       icu_admission=department_admission,
       icu_discharge=department_discharge,
       department,
       department_prev,
       department_next
       )]
# drop those patients that had admissions before the database started
tdt <- tdt[!is.na(icu_admission)]
tdt

# JOIN ONTO CABOODLE VERSION
# Compare vs Epic's ICU stay table
query <- "SELECT * FROM uds.icu_audit.icu_stay"
caboodle_icu <- DBI::dbGetQuery(ctn, query)
setDT(caboodle_icu)
caboodle_icu[, pat_enc_csn_id := as.character(pat_enc_csn_id)]
setnames(caboodle_icu, 'pat_enc_csn_id', 'csn')
str(caboodle_icu)
janitor::tabyl(caboodle_icu$icu_department)
# drop T07S
caboodle_icu <- caboodle_icu[icu_department != 'UCH T07S']

# you need to join on csn==csn department_admission~=icu_stay_start_dttm
caboodle_icu[, icu_start := lubridate::round_date(icu_stay_start_dttm, unit="hour")]
tdt[critcare == TRUE, icu_start := lubridate::round_date(department_admission, unit="hour")]

tdt <- caboodle_icu[tdt, on=.NATURAL]

tdt[, discharge_diff := as.integer(icu_discharge - icu_stay_end_dttm)]
tdt[, admission_diff := as.integer(icu_admission - icu_stay_start_dttm)]

# DROP this column: was only there for the join
tdt[, icu_start := NULL]

# FLAG matches
tdt[, emap_caboodle_match_ok := FALSE]
tdt[!is.na(icu_stay_block_id) & (abs(discharge_diff) < 300 | is.na(discharge_diff)) & abs(admission_diff) < 300, emap_caboodle_match_ok := TRUE ]


tdt
tdt[department == 'UCH T03 INTENSIVE CARE']
tdt[department == 'UCH T03 INTENSIVE CARE' & emap_caboodle_match_ok]

# Better: write this back to the icu_audit schema (rather than saving locally)
table_path <- DBI::Id(schema="icu_audit", table="icu_admissions")
DBI::dbWriteTable(ctn, name=table_path, value=tdt, overwrite=TRUE)

DBI::dbDisconnect(ctn)

stop()

# Compare vs Epic's ICU stay table
query <- "SELECT * FROM uds.icu_audit.icu_stay"
xdt <- DBI::dbGetQuery(ctn, query)
setDT(xdt)
xdt[, pat_enc_csn_id := as.character(pat_enc_csn_id)]

xdt <- xdt[tdt, on="pat_enc_csn_id==csn"]
# ydt <- xdt[ydt, on="pat_enc_csn_id==csn", nomatch=0]
xdt[, discharge_diff := discharge_icu - icu_stay_end_dttm]
xdt[, admission_diff := admission_icu - icu_stay_start_dttm]

openxlsx::write.xlsx(xdt, file='dev/tmp/star_vs_caboodle_bed_moves.xlsx')
