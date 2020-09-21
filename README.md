<span id="anchor"></span>Intro

Create a spark application which will :

-   Read a csv file from local filesystem
-   Remove rows where any string column is an empty string or just
    spaces (“ ”)\
    Note : empty string is not same as null
-   Convert data-type and names of the columns as per user’s choice
-   For a given column, provide profiling information: total number of
    unique values, count of each unique value. Exclude nulls

The application should be generic, so that it can be used for any csv
file, on the basis of arguments provided by the user. There should not
be any hard-coding of column names. The sample.csv provided below is
just an example.

Source code should be placed in an open repository (e.g. github.com), no
zip archives. Prepare this task as you do with features for your
production (abstractions, code style, etc).

Focus on business logic, not on parsing command-line arguments. If input
params can be set only via modifying several first lines of the source
code of your app - it will be fine.

<span id="anchor-1"></span>Step 1

Load a csv file from the local filesystem to spark dataframe. First row
in the file is header information, which should become the column’s for
the dataframe.

<span id="anchor-2"></span>Sample.csv

‘name’, ‘age’, ‘birthday’, ‘gender’

‘John’, ‘26’, ‘26-01-1995’, ‘male’

‘Lisa’, ‘xyz’, ‘26-01-1996’, ‘female’

, ‘26’, ‘26-01-1995’, ‘male’

‘Julia’, ‘ ’, ‘26-01-1995’, ‘female’

‘ ’ , , ‘26-01-1995’,

‘Pete’, ‘ ’, ‘26-01-1995’, ‘ ’

<span id="anchor-3"></span>Dataframe

Columns - name (string), age (string), birthday (string), gender
(string)

name+++age+++birthday+++gender

‘John’+++‘26’+++‘26-01-1995’+++‘male‘

‘Lisa’+++‘xyz’+++‘26-01-1996’+++‘female‘

null+++‘26’+++‘26-01-1995’+++‘male‘

‘Julia’+++‘ ‘+++‘26-01-1995’+++‘female‘

‘ ’+++null+++‘26-01-1995’+++‘male‘

‘Pete’+++‘ ‘+++‘26-01-1995’+++‘ ‘

<span id="anchor-4"></span>Step 2

Filter data and remove rows from the dataframe, where the column is of
string type and has empty value or just spaces. Do not remove rows where
the value for the column is null

<span id="anchor-5"></span>Dataframe

Columns - name (string), age (string), birthday (string), gender
(string)

name+++age+++birthday+++gender

‘John’+++‘26’+++‘26-01-1995’+++‘male‘

‘Lisa’+++‘xyz’+++‘26-01-1995’+++‘female‘

null+++‘26’+++‘26-01-1995’+++‘male‘

<span id="anchor-6"></span>Step 3

The system should take a input’s from the user which is a list of :

- existing\_col\_name, new\_col\_name, new\_data\_type, date\_expression
(required only if the new\_data\_type is of DateType

On the basis of the above inputs, the system should convert existing
columns in the dataframe to new columns with the new data type. If there
are column values in a row which cannot be converted, we should replace
it with null.

Example : 2nd row, 2nd column i.e. age = “xyz”. This cannot be converted
to integer. So it should be replaced with null

If an existing column in dataframe is not mentioned in the list, it
should be omitted from the dataframe

Example : gender hasn’t been mentioned. So it’s removed from the
dataframe generated.

The valid data\_types supported for this task are string, integer, date,
boolean. However please note - Ensure code modularity so that the system
can handle new types like decimal, float, etc.

<span id="anchor-7"></span>Arguments:

\[

{"existing\_col\_name" : "name", "new\_col\_name" : "first\_name",
"new\_data\_type" : "string"},

{"existing\_col\_name" : "age", “new\_col\_name" : "total\_years",
"new\_data\_type" : "integer"},

{"existing\_col\_name" : "birthday" "new\_col\_name" : "d\_o\_b",
"new\_data\_type" : "date", "date\_expression" : "dd-MM-yyyy"}

\]

<span id="anchor-8"></span>Dataframe

Columns - first\_name (string), total\_years (integer), d\_o\_b (date)

first\_name+++total\_years+++d\_o\_b

‘John’+++26+++26-01-1995

‘Lisa’+++null+++26-01-1995

null+++26+++26-01-1995

<span id="anchor-9"></span>Step 4

Provide profiling on all columns.

For the column provided, the system should give a count of the total
number of unique values and count of each unique value. null values
should be ignored.

<span id="anchor-10"></span>Output :

\[

 {

 "Column":"name",

 "Unique\_values":2,

 "Values":\[

 { "John":1 }, { "Lisa":1 }

 \]

 },

 {

 "Column":"age",

 "Unique\_values":1,

 "Values":\[

 { "26":2 }

 \]

 },

 {

 "Column":"birthday",

 "Unique\_values":1,

 "Values":\[

 { "26-01-1995":3 }

 \]

 }

\]
