# Milestone 3 README


## Initialization

### Our dag script first starts with creating the path and datetime variables to save our csv files

### Then we create the default_arguments and dag scheduled to run once



## Pipeline Methods

### We have 3 methods read_olympic, read_noc, read_medals which are responsible for reading the csv files and saving them in kwargs for the other tasks to use

### The clean_data is responsible for the data cleaning process for the olympic dataset

### The integrate_data is responsible integrating the noc column and creating the new medals dataset 

### The feature_data is resposible for adding the BMI and all time total to our datasets

### Finally the load_data stores the output of each of the tasks in the path using a folder name created by the date and time of today

### We added an example output of the folder with the saved csv files created from running the dags


## Tasks dependency

### At the end of the file we give the dependency order depending on what tasks need the input of another

### We provided a picture of the dependency graph of the tasks from airflow




















