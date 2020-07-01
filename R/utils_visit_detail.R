#' @title foo
#'
#' @description
#' foobar
#'
#' @import data.table
#' @importFrom lubridate dhours
#'
#' @param dt data.table
#' @param col output column
#' @param group grouping variable
#' @param order_vars vars for sorting
#' @param col_jump column where discontinuity will be flagged
#'


column_jump <- function(dt, col, order_vars, group, col_jump) {
  # helper function for collapse_over
  # Input: a data.table
  # Output: a data.table with a column (col) that flags 'steps'

  # define updates where the location changes; then keep those and the NAs
  # checks to see if the department has changed compared to the prev entry

  dt[order(get(order_vars)),
     (col_jump) := shift(.SD, type="lag"),
     by=list(get(group)),
     .SDcols=col][
       , (col_jump) :=  get(col_jump) != get(col)]
  return(dt)

}

time_jump <- function(dt, in_time, out_time, order_vars, group) {
  # helper function for collapse_over
  # Input: a data.table with cols for in and out times (e.g. admission and discharge)
  # Output: a data.table with a column that flags 'steps' in a time-series

  # now need logic to check whether the gap between times is too big (i.e. a re-admission or not)
  # Specify gap between times for a new location to be considered a 're-admission'
  col_jump <- 'out2in_jump'

  dt[order(get(order_vars)),
     (col_jump) := shift(.SD, type="lag"),
     by=list(get(group)),
     .SDcols=out_time][
       , (col_jump) :=  get(in_time) - get(col_jump)]
  return(dt)

}


#' @export
collapse_over <- function(dt, col, in_time, out_time, order_vars, group, time_jump_window=dhours(6)) {
  # defines ward transitions based on a time series in of ward names
  # and a gap of sufficent size in the time that a re-admission is likely
  tdt <- data.table::copy(dt)
  col_jump <- paste0(col, '_jump')

  tdt <- column_jump(tdt, col, order_vars, group, col_jump)
  tdt <- time_jump(tdt, in_time, out_time, order_vars, group)

  # Now to collapse over ward episodes, just keep where either ward_jump | time_jump is true
  # keep all time_jumps > a window
  temp <- tdt[get(col_jump) | abs(out2in_jump) > time_jump_window | is.na(get(col_jump)) | is.na(out2in_jump)]
  # now number the department moves: department_i
  col_i <- paste0(col, '_i')
  temp[order(get(order_vars)), (col_i) := seq_len(.N), by=.(get(group))][]

  # finally join this back on to dt
  tdt <- temp[tdt, on=.NATURAL]

  # then roll the department variable forwards and back
  tdt[order(get(order_vars)), (col_i) := nafill(get(col_i), type='locf'), by=.(get(group))][]

  # drop intermediate columns
  tdt[, (c(col_jump, "out2in_jump")) := NULL] # drop these intermediate columns

  # department admission
  col_in <- paste0(col,'_',in_time)
  tdt[order(get(order_vars)), (col_in) := min(get(in_time)), by=.(get(group),get(col_i)) ]

  # department discharge
  col_out <- paste0(col,'_',out_time)
  tdt[order(get(order_vars)), (col_out) := max(get(out_time)), by=.(get(group),get(col_i)) ]

  return(tdt)
}
