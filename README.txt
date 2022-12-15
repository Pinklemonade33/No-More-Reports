 To create a report: import report_objects file, and then refer to its classes for what report you want and then initialize that class.
A data-frame is stored in each report instance as "df". For Single File Reports this is usually just the raw data pulled from its source.
For Cross File Reports it is a combination of at least two files in a configuration that most often needed.
if there are multiple configurations needed then multiple dataframes will exist within the class instance.





