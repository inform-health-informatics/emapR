#' @title Extract & Reshape Data from EMAP
#'
#'   This is the workhorse function that transcribes data from EMAP OPS from
#'   OMOP CDM 5.3.1 to a standard rectangular table with 1 column per dataitem
#'   and 1 row per time per patient.
#'
#'   The time unit is user definable, and set by the "cadence" argument. The
#'   default behaviour is to produce a table with 1 row per hour per patient. If
#'   there are duplicates/conflicts (e.g more than 1 event for a given hour),
#'   then the default behaviour is that only the first result for that hour is
#'   returned. One can override this behvaiour by supplying a vector of summary
#'   functions directly to the 'coalesce_rows' argument. This could also include
#'   any custom function written by the end user, so long as it takes a vector
#'   of length n, and returns a vector of length 1, of the original data type.
#'
#'   Many events inside EMAP occur on a greater than hourly basis. Depending
#'   upon the chosen analysis, you may which to increase the cadence. 0.5 for
#'   example will produce a table with 1 row per 30 minutes per patient. Counter
#'   to this, 24 would produce 1 row per 24 hours.
#'
#'   Choose what variables you want to pull out wisely. This function is quite
#'   efficient considering what it needs to do, but it can take a very long time
#'   if extracting lots of data, and doing so repeatedly. It is a strong
#'   recomendation that you run your extraction on a small subset of patients
#'   first and check that you are happy with the result, before moving to a
#'   larger extraction.
#'
#'   The current implementation is focussed on in-patients only. And as such,
#'   all dataitems are referenced to the visit_start_datetime of the
#'   visit_occurrence. Thus, observations and measurements recorded outside the
#'   boudaries of the visit_occurrence are automatically removed. This is - at
#'   this stage - intensional behaviour.
#'
#' @param connection a EMAP database connection
#' @param target_schema the target database schema
#' @param visit_occurrence_ids an integer vector of episode_ids or NULL. If NULL
#'   (the default) then all visits are extracted.
#' @param anchor_timestamps an timestamp vector or NULL of the same length as
#'   visit_occurence_ids. If NULL (the default) then visit_start_datetime (from
#'   visit_occurrence) is used.
#' @param concept_ids a vector of OMOP concept_ids to be extracted
#' @param concept_short_names a character vector of names you want to relabel
#'   OMOP codes as, or NULL (the default) if you do not want to relabel. Given
#'   in the same order as \code{concept_names}
#' @param coalesce_rows a vector of summary functions that you want to summarise
#'   data that is contributed higher than your set cadence. Given in the same
#'   order as \code{concept_names}
#' @param chunk_size a chunking parameter to help speed up the function and
#'   manage memory constaints. The defaults work well for most desktop
#'   computers.
#' @param cadence a numerical scalar >= 0. Describes the base time unit to build
#'   each row, in divisions of an hour. For example: 1 = 1 hour, 0.5 = 30 mins,
#'   2 = 2 hourly. If cadence = 0, then the pricise datetime will be used to
#'   generate the time column. This is likely to generate a large table, so use
#'   cautiously.
#'
#' @return sparse tibble with hourly cadence as rows, and unique OMOP concepts
#'   as columns.
#'
#' @export
#'
#' @import data.table
#'
#' @importFrom purrr map imap
#' @importFrom lubridate now
#' @importFrom rlang inform
#' @importFrom dplyr first
#'
#' @export
extract <- function(connection,                   # via DBI; database connection
                    target_schema,                # schema that holds the OMOP tables
                    visit_occurrence_ids = NULL,  # visit ids to extract
                    anchor_timestamps = NULL,     # timestamps to define relative dates
                    concept_ids = NULL,           # concept ids
                    concept_short_names = NULL,   # friendly names for variables
                    coalesce_rows = NULL,         # function for collapsing data
                    # chunk_size = 5000,          # deprecated
                    cadence = 1                   # cadence for collapsing time series
                    ) {

  starting <- now()

  # check the connection
  assertthat::assert_that(DBI::dbIsValid(ctn))

  # checks for at least one of the measurement and observation table
  # and if the visit_occurrence table if visit_occurence IDs are not provided
  tables <- data.table(table = c('observation', 'measurement'),
                       path = '')
  if (is.null(visit_occurrence_ids)) tables <- c(tables, 'visit_occurrence')
  for (i in 1:nrow(tables)) {
    t <- DBI::Id(schema=target_schema, table=tables[i]$table)
    chk <- DBI::dbExistsTable( connection,t)
    assertthat::assert_that(chk, msg=paste('!!! unable to find table', tables[i]$table, 'in connection'))
    tables[i]$path <- paste(target_schema, tables[i]$table, sep = '.')
  }

  # make sure we have one timestamp for each visit_occurence ID
  if (!is.null(visit_occurrence_ids)) {
    assertthat::assert_that(checkmate::check_posixct(anchor_times))
    assertthat::assert_that(length(visit_occurrence_ids) == length(anchor_timestamps))
  }

  # cadence checks
  cadence_pos_num <- class(cadence) == "numeric" && cadence >= 0
  cadence_timestamp <- cadence == "timestamp"
  if (!(cadence_pos_num || cadence_timestamp)) {
    rlang::abort(
      "`cadence` must be given as a numeric scalar >= 0
       or the string 'timestamp'")
  }

  # if coalesce rows is NULL then use 'first'
  if (is.null(coalesce_rows)) {
    rlang::inform("--- Using first to select values where more than one available")
    # TODO: how to provide the package e.g. data.table::first
    coalesce_rows <- data.table::first
    coalesce_rows <- first
  } else {
    # check that coalesce rows contains functions
    coalesce_rows <- parse_coalesce_functions(coalesce_rows)
  }

  # check that we've either got one function to recycle or one function per variable
  chk1 <- length(coalesce_rows) > 0
  chk2 <- length(coalesce_rows) == length(concept_short_names)
  assertthat::assert_that(any(chk1 | chk2 ))

  # now practically check the functions evaluate a simple numerical list
  # before you start loading data
  tryCatch(
    expr = {
      sapply(coalesce_rows, do.call, list(1:3))
    },
    error = function(e){
      print('!!! Functions passed in coalesce rows failed on simple vector 1:3, please check')
      print(e)
    }
  )

  # define anchore times for date offsets
  if (!is.null(visit_occurrence_ids) & !is.null(anchor_timestamps)) {
    # use an arbitrary set of ids and anchor times
    assertthat::assert_that(length(visit_occurrence_ids) == length(anchor_timestamps))
    vo <- data.table(
        visit_occurrence_id = visit_occurrence_ids,
        anchor_time = anchor_timestamps
    )
  } else {
    # load the visit occurrence table
    vo <- select_from(ctn,
                      target_schema,
                      'visit_occurrence',
                      cols=c('visit_occurrence_id', 'visit_start_datetime'))
  }
  if (is.null(visit_occurrence_ids)) {
    # use all visit_start_datetimes from visit_occurrence table
    vo <- vo[,.(visit_occurrence_id, anchor_time = visit_start_datetime)]
  }
  if (!is.null(visit_occurrence_ids) & is.null(anchor_timestamps)) {
    # use selected visit_start_datetime from visit_occurrence table
    vo <- vo[visit_occurrence_id %in% visit_occurrence_ids,
             .(visit_occurrence_id, anchor_time = visit_start_datetime)]
  }
  assertthat::assert_that(nrow(vo) > 0)
  # FIXME?
  # assertthat::assert_that(uniqueN(vo) == nrow(vo))
  vo <- unique(vo)

  # If no friendly names provided then string(ify) the concept_id
  if (is.null(concept_short_names)) rename <- as.character(concept_ids)

  # Expand functions as needed or raise an error
  # If one function provided then replicate for each param
  if (length(coalesce_rows) == 1) {
    coalesce_rows <- rep(coalesce_rows, length(concept_ids))
  }
  # if one function per parameter then no further action
  if (length(coalesce_rows) == length(concept_ids)) {
    # pass
  }
  # if functions and parameters differ then expand (with informational msg)
  if (length(coalesce_rows) != length(concept_ids)) {
    n_concept_ids <- length(concept_ids)
    n_coalesce_rows <- length(coalesce_rows)
    # Now expand up for each function
    coalesce_rows <- rep(coalesce_rows, n_concept_ids )
    concept_ids <- rep(concept_ids, each=n_coalesce_rows)
    concept_short_names <- rep(concept_short_names, each=n_coalesce_rows)
  }

  # Prepare data.table to hold
  # - concepts_ids
  # - proposed friendly names for concept_ids
  # - functions names (characters) to coalesce each data item
  params <- data.table(
    concept_ids = concept_ids,
    short_name = concept_short_names,
    func = coalesce_rows
  )

  assertthat::assert_that(!anyDuplicated(params))
  assertthat::assert_that(!anyDuplicated(params[,.(short_name,func)]))

  # if more than one summary function provided for a variable then append that to the column name
  if (anyDuplicated(params$short_name)) {
    params[, col_name := paste(short_name, func, sep='_')]
  } else {
    params[, col_name := short_name]
  }

  rlang::inform('\n--- BEGIN: parameters to be processed\n')
  print(params)
  rlang::inform('\n--- END: parameters to be processed')

  # Now start loading data
  obs <- data.table()
  mes <- data.table()

  # Load observations
  obs <- omop_select_from(connection,
                          target_schema,
                          'observation',
                          concepts = params$concept_ids,
                          chunk_size = 1e4)
  if (nrow(obs)) {
    setnames(obs, 'observation_concept_id', 'concept_id')
    setnames(obs, 'observation_id', 'property_id')
    setnames(obs, 'observation_datetime', 'datetime')
  }

  # Load measurements
  mes <- omop_select_from(connection,
                          target_schema,
                          'measurement',
                          concepts = params$concept_ids,
                          chunk_size = 1e4)
  if (nrow(mes)) {
    setnames(mes, 'measurement_concept_id', 'concept_id')
    setnames(mes, 'measurement_id', 'property_id')
    setnames(mes, 'measurement_datetime', 'datetime')
  }

  # Union observations and measurements
  dt <- list(obs, mes)
  dt <- rbindlist(dt, fill=TRUE)
  rm(obs,mes)

  # Filter to just the relevant concepts for the relevant patients
  tdt <- dt[visit_occurrence_id %in% visit_occurrence_ids]
  rlang::inform(paste('--- NOTE: Loaded', nrow(tdt), 'observations'))

  # Make times relative
  tdt <- make_times_relative(tdt,vo)
  tdt

  # https://stackoverflow.com/questions/26508519/how-to-add-elements-to-a-list-in-r-loop
  tdts <- vector("list", nrow(params))

  # Coalesce each parameter
  for (i in 1:length(tdts)) {

    param <- params[i,]
    udt <- tdt[concept_id == param$concept_id]
    if (nrow(udt) == 0) {
      rlang::inform(paste('--- No data found for', param$short_name, "(skipping)"))
      next()
    }

    tryCatch(
    expr = {
      value_from_NAs<- udt[
        ,lapply(.SD, is.na),
        .SDcols=c('value_as_concept_id',
                  'value_as_datetime',
                  'value_as_number',
                  'value_as_string')]
      col <- names(which.min( colSums(value_from_NAs) ))
      assertthat::assert_that(length(col) == 1)
      rlang::inform(paste('*** Using', col, 'to coalesce', param$col_name))
    },
    error = function(e){
      rlang::warn(
        paste('!!! Unable to coalesce',
              param$short_name,
              '(could not identify value_from column)'))
      next()
    } )
    udt <- coalesce_over(udt, value_as=col, coalesce = param$func, cadence=cadence)
    udt[, col_name := param$col_name]
    rlang::inform(paste('*** Coalesced', param$short_name, "from", nrow(tdt),
                "rows to", nrow(udt), "rows at a", cadence, "hourly cadence using", param$func))
    tdts[[i]] <- udt

  }

  res <- rbindlist(tdts, fill=TRUE)
  res

  elapsed_time <- signif(
    as.numeric(
      difftime(
        lubridate::now(), starting, units = "secs")), 2)
  rlang::inform(paste('\n', elapsed_time, "seconds to process"))

  if (requireNamespace("praise", quietly = TRUE)) {
    well_done <-
      praise::praise(
        "${EXCLAMATION}! How ${adjective} was that?!"
      )
    rlang::inform(well_done)
  }

  return(res)
}


# Helper functions
# ================
# Not exported

coalesce_over <- function(dt, value_as='value_as_number', coalesce=NULL, cadence=1) {
  'given dt with diff times, collapse using function over cadence'

  # TODO: where value_as_string/datetime etc. then build in supporting logic

  cols <- paste(c('visit_occurrence_id', 'diff_time', value_as))

  if (is.null(coalesce)) coalesce <- "first"
  if (value_as != 'value_as_number' & coalesce != 'first') {
    rlang::warn(paste('!!! non-numeric parameter so forcing to first despite request'), coalesce)
    coalesce <- 'first'
  }

  dt <- dt[,..cols,with=TRUE]
  dt[, diff_time := round_any(diff_time, cadence)]
  dt[, (value_as) := do.call(get(coalesce), list(get(value_as))), by=.(visit_occurrence_id, diff_time)]
  return(unique(dt))
}

filter_obs <- function(dt, concept_ids, these_ids=NULL){
  'filter observations by concept_id and episode (aka visit_detail)'
  tdt <- data.table::copy(dt)
  tdt <- tdt[concept_id %in% concept_ids]
  if (!is.null(these_ids)) {
    tdt <- tdt[visit_occurrence_id %in% these_ids]
  }
  return(tdt)
}

make_times_relative <- function(dt, vdt, units = "hours", debug=FALSE) {
  'given a timeseries keyed by an id, and a start time for each id'
  'vd = visit_detail with visit_detail_start_datetime'
  'dt = obs or similar with visit_occurrence_id and datetime'
  'NOTE: returns time diff in hours'
  # TODO: convert this to work with any pair of tables
  # where one contains timeseries EAV and the other has an 'offset' date against an ID
  tdt <- data.table::copy(dt)
  assertthat::assert_that(uniqueN(vdt) == nrow(vdt))

  tdt <- vdt[tdt, on=c('visit_occurrence_id')]
  tdt[, diff_time := as.numeric(difftime(datetime, anchor_time, units = units))]
  tdt <- tdt[order(visit_occurrence_id, diff_time)]
  if (!debug) {
    tdt[, anchor_time := NULL]
    tdt[, datetime := NULL]
  }
  if (NA %in% tdt$diff_time){
    rlang::warn('--- Lossy: NAs generated when calculating diff, missing either visit_detail_start_datetime or similar')
    # Not necessarily a problem since there will be observations for visits that you're not interested in
    # remember that the join is on the specific visit detail time
    tdt <- tdt[!is.na(diff_time)]
  }
  setcolorder(tdt, c('person_id', 'visit_occurrence_id', 'diff_time', 'concept_id'))
  return(tdt)
}

parse_coalesce_functions <- function(funs=c('first')){
  #' return a character vector of function names
  #' expects either alist (or) a character vector
  print(funs)
  if (is.atomic(is.character(funs))) {
    return(funs)
  } else {
    # FIXME: Need to work out how to do this cleanly
    stop("!!! Please pass function names not objects e.g. c('sum') not c(sum)" )
    return( sapply(funs, function(x) as.character(substitute(x))) )
  }

}

#' Fill in 2d Table to make a Sparse Table
#'
#' The extract_timevarying returns a non-sparse table (i.e. rows/hours with
#' no recorded information for a patient are not presented in the table)
#' This function serves to expand the table and fill missing rows with NAs.
#' This is useful when working with most time-series aware stats packages
#' that expect a regular cadence to the table.
#'
#' @param df a dense time series table produced from extract_timevarying
#' @param cadence the cadence by which you want to expand the table
#'   (default = 1 hour)
#'
#' @return a sparse time series table
#' @export
expand_missing <- function(df, cadence = 1) {
  df %>%
    select(episode_id, time) %>%
    split(., .$episode_id) %>%
    imap(function(base_table, epi_id) {
      tibble(
        episode_id = as.numeric(epi_id),
        time = seq(
          min(base_table$time, 0),
          max(base_table$time, 0),
          by = cadence
        )
      )
    }) %>%
    bind_rows() %>%
    left_join(df, by = c("episode_id", "time"))
}
