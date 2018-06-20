# gradlewdc
Tableau Web Data Connector for Gradle

How to use this connector:

1. Load the connector in Tableau Desktop.
2. Enter in your Gradle Enterprise URL
3. Two ways to load data:
- Custom Time Windows - Specify a start and end time in PST
- Setup for incremental refreshes - Specify a start time and leave the end time empty. Fill in the refresh interval ( how many minutes of data to load since the last refresh )
