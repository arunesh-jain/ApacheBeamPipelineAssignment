# apache-beam-basics

Problem 1-
Given CSV file of sales data(find attached SalesJan2009.csv file)


Write a Beam pipeline to read the given CSV file of sales data and Compute which state of every country has the maximum sales(sum of all products sold price within every state then find maximum of sold price and which state) in 2009. Save the output in another CSV file with headers [State, city, max_price].
output Sample file-
State, city, max_price
United state,NY,49200
United Kingdom,England,54600
Switzerland,Vaud,35100

----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
Problem 4 -
Given CSV file of  mall customers data(find attached Mall_Customers_Income.csv file)


Write a Beam pipeline to read the given Mall_Customers_Income.csv  file and compute the average spending score and average annual income of male and female customers. Save the output in another file with Header [gendre,avg_annual_income(k$),avg_spending_score]
Output sample file-

gendre,avg_annual_income(k$),avg_spending_score

male,21,65
female,25,70
