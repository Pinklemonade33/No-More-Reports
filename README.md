# No-More-Reports

No-More-Reports is a Python application I created at my old Job
to help me automate many of the reports myself and co-workers were
expected to do.

## Design

No-More-Reports is built with pandas and returns data frames
so that more precise results can be acquired if needed. I designed 
No-More-Reports to be very simple to use, the user only needs to initialize a report object to receive a 
data frame, there are no other steps required as long as the 
required files are somewhere inside the main directory. The required files are
automatically gathered when the data_file_objects module is loaded.


## Top level structure and how that relates to scaling

We have two classes: data file classes and report classes. data file 
classes give an identity to a downloaded file and report classes
use that identity to locate their required files. 

### Data file classes

The data file classes exist in a hierarchy with the DataFile class
at the top level. Data files are mainly identified by the column header
values in a pandas data frame by the higher level classes, lower
level classes are mainly identified by specific column values. 

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

### Scaling

This purpose behind data file objects and report objects is to scale this application
with new reports that I believed that I would one day have to do. Any time I would need further
analysis, I could do the work then copy and paste the code I used into the report object as a
method I could use another time if I needed to. If I want to create a new report and that 
report requires the results of a few other reports, I can very simply just composite those needed
reports into my report.

## Important note

For multiple reasons some of them obvious, I am unable to demonstrate this project with the
original data files that it was meant to be used for. I have had to generate data files
for a demo and while those data files can show that this application works, there are complications
that these demo files allow my application to leap over. I've also left out most of the data
that I didn't think was important for demonstration purposes, it also may be possible that 
some of that data that I left out may be used for something I have forgotten about.


## How to use

1. Clone this repo
2. Open Python console
3. Import report_objects
4. Initialize a report object
   - **Example:** `ro = MaterialForcast()`
     - Each report object contains at least one data frame
     - All report objects are located in report_objects.py
     - Data frames inside report objects always start with df