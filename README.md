Project Synopsis-Group 1




Texas Lottery Data Analysis and Prediction
Project Title:Statistical Modeling of Texas Lottery Draw Outcomes.
________________________________________
Problem Statement:The Texas Lottery Commission seeks to enhance revenue planning by forecasting lottery sales and segmenting retailers based on their sales patterns and game preferences. Leveraging historical sales data can provide insights to optimize inventory distribution and tailor promotional strategies
________________________________________
Objective:
This project aims to perform a comprehensive data-driven analysis of Texas Lottery outcomes and sales patterns. It includes identifying statistical trends, retailer performance, and consumer game preferences. The project will also segment retailers based on behavioral patterns to guide strategy, marketing, and logistics.
1.	Sales Forecasting
2.	Retailer Segmentation
3.	Trend Analysis
________________________________________
Expanded Scope:

●	Study sales growth trends over time (monthly, quarterly, yearly).

●	Identify top-performing retailers (parent/chain and independent) in terms of volume and frequency of sales.

●	Segment retailers based on:

○	Sales volume

○	Preferred game types sold

○	Customer buying patterns (scratch-off vs draw games)

●	Perform time series forecasting for total sales, broken down by:

○	Region
○	Retailer type
○	Game category

________________________________________
Methodology:
1.	Data Collection:

○	Pull multi-year draw and sales data from Texas Lottery Commission databases and public records.

2.	Data Cleaning & Preprocessing:

○	Merge and clean datasets on draws, sales, and retailers.

○	Create derived columns: Year-Over-Year growth, retailer type indicators, game popularity indexes.

3.	Exploratory Data Analysis (EDA):

○	Visualizing data for sales growth by game and region.

○	Clustering of retailers based on sales patterns. 

4.	Advanced Analysis:

○	Identify and rank top parent retailers with consistently high sales.

○	Conduct market basket analysis to see co-purchase trends (e.g. scratch-off + Powerball).

5.	Modeling:
○	Forecasting Modeling:
■	Event based modelling and revenue planning.

○	Predictive Modeling:
■	Predict high-sales days based on historical data.
■	Classification for predicting popular game types at a retailer.
________________________________________
Expected Outcomes:
●	A visual dashboard tracking sales growth, game popularity, and top-performing retailers.

●	Segmentation of retailers to optimize Sales patterns,Game preferences.

________________________________________
Tools & Technologies:Proposed Tech Stack
●	Data Ingestion & Storage:
○	AWS S3
○	AWS Glue
●	Data Processing & Analysis:
○	Apache Spark (via AWS Glue or EMR)
○	Python (Pandas, NumPy,PySpark)
●	Visualization & Reporting:
○	Power BI
●	Deployment & Monitoring:
○	AWS Lambda
○	Github and Terraform
________________________________________
Conclusion:
This project looks at lottery draw results, sales patterns, and retailer performance to give a complete picture of how the Texas Lottery works. By using data and prediction tools, it helps decision-makers understand what’s happening and plan better.
Studying Texas Lottery sales data can show useful trends—like which games sell the most, which stores do well, and what customers like to play. Using these insights and the right tools can help the Lottery Commission make smarter choices, improve how they plan for future sales, and create better marketing strategies.
________________________________________
References:
https://catalog.data.gov/dataset/texas-lottery-sales-by-fiscal-month-year-game-and-retailer
