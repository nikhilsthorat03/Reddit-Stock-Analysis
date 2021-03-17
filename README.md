## Finding out about stocks before they make the news using Reddit Data , Nasdaq Data &amp; Cloud Analytics Services from Amazon Web Services.

Lately, a group of online traders collaborated on Reddit & took down the hedge fund by bidding up the stock price of GameStop.This unprecedented move by the online traders took the market by storm with continuous ups and downs in the stock value. Therefore, we decided to come up with a project that can analyze the discussions on Reddit for “/r/WallStreetBets” and gives the users an idea about what are some of the stocks that are being discussed, sentiment of their discussion and further allowing the user to view the additional details about the required stock over an interactive dashboard.

- /src directory 
  - Includes the python script for the two lambda functions that were created for the project execution
 
- Try our Application here : http://redstocks.com.s3-website-us-east-1.amazonaws.com/
  - Amazon Lex : Chatbot user interface to easy access.
  - AWS Lambda : Lambda Triggers to collect data from Reddit upon specific Lex Intent.
  - Amazon S3 : Storage of the Output files [reddit scrapped data & nasdaq stock data]
  - AWS Glue & AWS Athena : Athena Table for the Nasdaq Stocks Data
  - Amazon Comprehend : Text Analytics [Detect Entities & Detect Sentiment ]
  - Amazon QuickSight : Interactive dashboard allowing users to visualize the stock data attributes.

- Benefits
  - Chatbot easy interaction, no need to read the reddit discussions.
  - We analyse the redditors posts and do all the heavy lifting.
  - Provides an interactive dashboard where the user can visualize the stock attributes.
- Challenges
  - Limitations in the size of data that can be analyzed at a time by AWS Comprehend
  - Limitations on the execution time for lambda functions used in fulfilling an intent.
  - Publicly shareable quicksight dashboard links come with a premium subscription.
- Assumptions
  - Redditors on the channel “/WallStreetBets” discuss only about stocks and option trading.
  - Top 100 trending reddit posts data is relevant enough for analysis.
  - As fast-growing stocks are listed on Nasdaq, we have considered Nasdaq stocks data.

## Architecture
![Architecture](https://github.com/aashish-bidap/Reddit-Data-Analysis/blob/main/Architecture.png)

