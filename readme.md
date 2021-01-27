# Utilities for working with EHR data

```
remotes::install_github('inform-health-informatics/guidEHR', upgrade=FALSE)
```

## Notes

I had trouble getting Rcmd.exe to run on the datascience desktop form Rstudio during development.
The following works for me from within Rstudio

```R
library(devtools)
document()
build()
install()
```

This package builds out from Ed Palmer's **inspectEHR** work, and provides a series of tools for working with EMAP.
