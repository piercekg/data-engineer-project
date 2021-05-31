The goal of the design task is to see how you would scale and maintain the system.

* Propose solutions for an 100x increase in data volume, and an hourly update cadence
* Propose ideas for data reprocessing:
    * How would you go about backfilling 1 year worth of data?
    * How would you avoid impact on the production flow (e.g. concurrent job runs)?
* What kind of error handling would you put in place?

Be sure to discuss issues and trade-offs around scaling, monitoring, failure recovery, authentication, etc...

## Design
If I were going to continue improving and optimizing my ETL process, I would move away from using `csv.DictReader` to read the input file. While it seened useful early in the process to be able to access values in a row by field name, my sense is that mapping each row to a dictionary is very time consuming and can lead to an unnecessarily high error rate as one missing or incorrect character can cause values to be associated with the wrong field name. I would instead move to reading each row as a string and refining my grok patterns and regular expressions to extract the relevant values, instead of just as an extra validation step. While I am not very familiar with the time complexity of grok and regex, I think that at least removing the step of mapping the string to a dictionary would streamline the process. I imagine pattern matching to be time intensive itself, so I would continue looking for ways to optimize those expressions once I was extracting all of the values in that way.

I would also ask about how to handle rows with incomplete data or `0` and `[]` values for relevant fields. Should those rows be included in the results? This leads me to how I would continue to build and optimize error handling. Currently, if the value for any field in a row is returned as `None` from the grok or regex validation, that row is written to an error log and left out of the data model. Should rows with values of `0` for budget or revenue, or lists of length `0` for production companies and genres cause rows to be left out of the data model? Or should all incomplete data be included in the data model anyway? I think my next step would be to examine the rows being written to the error log and identify ways to improve my pattern matching expressions, or perhaps if a second level of pattern matching could be used to extract data from rows that fail at the first level, and thereby bring down the rate of rows being written to the error log and increase the amount of data going into the data model.