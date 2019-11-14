--this is crime rate table for assault. we're interested in the number of assault crime in 2000. 
SELECT * FROM crime_rates.crime_assaults 
WHERE report_year = 2000
ORDER BY agency_jurisdiction

--this is crime rate table for homicide. we're interested in the number of homicide crime in 2000. 
SELECT * FROM crime_rates.crime_homicide 
WHERE report_year = 2000
ORDER BY agency_jurisdiction

--this is crime rate table for rapes. we're interested in the number of rapes crime in 2000. 
SELECT * FROM crime_rates.crime_rapes
WHERE report_year = 2000
ORDER BY agency_jurisdiction

--this is crime rate table for robberies. we're interested in the number of robberies crime in 2000. 
SELECT * FROM crime_rates.crime_robberies
WHERE report_year = 2000
ORDER BY agency_jurisdiction

--this is crime rate table for violent cirme. we're interested in the number of violent crime in 2000. 
SELECT * FROM crime_rates.crime_violentcrime 
WHERE report_year = 2000
ORDER BY agency_jurisdiction

--this is unemployment rate table for national unemployment rate. we're interested in the number of unemployment rate in 2000. 
SELECT * FROM Unemployment.unemployment_rate
WHERE year = 2000
ORDER BY State
