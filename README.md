# No-More-Reports

No-More-Reports is Python application I created at my old Job
to help me automate many of the reports myself and co-workers were
expected to do.

## Design

No-More-Reports is built with pandas and returns data frames
so that more precise results can be acquired if needed. I designed 
No-More-Reports to be very simple to use on the user side, and
scalable on the developer side in a way that once any kind of code
is written to further analyze data, it can be easily copied and pasted
as a method to the report class it was built off of. report classes
are also designed to be composited by other report classes for code
reuse. 

The user only needs to initialize a report object to receive a 
data frame, there are no other steps required as long as the 
required files are inside the main directory. The required files are
automatically gathered when the data_file_objects module is loaded.

## Structure

There are two different types of classes that make up the structure
of my application: data file classes and report classes. data file 
classes give an identity to a downloaded file and report classes
use that identity locate their required files.

### Data file classes

The data file classes exist in a hierarchy with the DataFile class
at the top level. Data files are mainly identified by the column header
values in a pandas data frame by the higher level classes, lower
level classes are mainly identified by specific column values

### Report classes

There are three different types of report classes: single column 
reports, single file reports, and cross file reports.

- **Single column reports** are bundled methods
- **Single file reports** require one data file and  can composite
single column reports
- **Cross file reports** can require multiple data files and/or
 composite multiple single file reports.

Each report class contains a method for initializing its data frame,
it also contains methods to further narrow down that data into 
somthing the user may want.














