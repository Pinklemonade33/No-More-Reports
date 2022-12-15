 To create a report: import report_objects file, and then refer to its classes for what report you want and then initialize that class.
A data-frame is stored in each report instance as "df". For Single File Reports this is usually just the raw data pulled from its source.
For Cross File Reports it is a combination of at least two files in a configuration that most often needed.
if there are multiple configurations needed then multiple dataframes will exist within the class instance.

This directory should include the following files:

* data_file_objects.py
    - Contains classes that are specifically tailored to each data file based on column values, these are subclassed based on overall row values
    - Contains functions that are used to search locally and identify the desired data file for the selected report along with a file detection class that
    stores information of all desired files that are not found
    - Works in conjunction with report_objects module to provide identified files for it to select from
* datafiletypes.pl
    - used by data_file_objects module to store data used for file identification
* list_file_methods.py
    - Methods used to create and manipulate listfiles.pl
* listfiles.pl
    - Categories of items needed for certain reports
* report_objects.py
    - Contains classes that utilize a DataFile instance in the form of ones of its subclasses to create a pandas
    data frame
    - Column reports are composited into single file reports and single file reports are composited into
    cross file reports





