The goal of the design task is to see how you would scale and maintain the system.

* Propose solutions for an 100x increase in data volume, and an hourly update cadence
* Propose ideas for data reprocessing:
    * How would you go about backfilling 1 year worth of data?
    * How would you avoid impact on the production flow (e.g. concurrent job runs)?
* What kind of error handling would you put in place?

Be sure to discuss issues and trade-offs around scaling, monitoring, failure recovery, authentication, etc...