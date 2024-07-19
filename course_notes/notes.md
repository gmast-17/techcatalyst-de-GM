## Week 1 - Data Engineering on the Cloud

Data Engineering
* What is it?
    - Making data useable and accessible
    - Movement of data (raw to usable and clean)
    - Preparing data for use at scale. 
* Top skills required (2-3) - Professional Skills 
    - Collaboration within team (solve issues together) 
    - Adaptable (learn/pickup new tech; not just be a - master at one skill)
     - Understanding the business need
* Most common tools used
     - SQL, python, Snowflake, AWS

    We learned about tableau/ thought spot and I don’t really think about that as 
        Medium to communicate things better/ tell a story
        Effective communication (issue or solution) 



Ways an org can utilize the different types of analytics
    - Descriptive: Use historical data to figure out what happened. 
        ○ Find patterns and relationships that could help describe why a success/failure occurred
        ○ Better understand changes in a business and if we are implementing changes we can use this data to feel more secure/confident in our decision, due diligence with our decision/ numerical evidence with our decision
        ○ Ex: profits month over month

    - Diagnostic
        ○ Eval overall performance from descriptive analytics, now we can see which metrics aren't up to standard and undercover those problems 
        ○ Bench mark for metrics and identify the cause of trends
        ○ Figuring out if its external or internal (not a lot of clicks on website and find out there is a problem in the code) 
        ○ Ex: Learn that our technology is out of date or Finding that problem in the code 
        ○ Ex: jan/feb/march profits are down in ice cream and diagnostic we can see temp is low 
        ○ Defect analysis 
        
    - Predictive:
        ○ Combine external factors and existing data to identify a trend
        ○ Predict how data should move
        ○ Help in making financial goals/kpis 
        ○ Ex; look at trend from prev year and based on how data is moving this year we can tweak the model and have better prediction for this current year 
        
    - Prescriptive:
        ○ Use visualizations to show what's going on and tell story and use that to guide recommendations 
        ○ Use the predictive analytics and take our model and apply it to a different location/apply elsewhere and use this as our recommendation 
        
        

 









Big Data
    - Massive data that can be processed using traditional methods
    - 3 V's: 
        ○ Volume
            - Huge amounts of data
        ○ Variety
            - Comes from lots of difference sources
        ○ Velocity
            - Batch Processing: a set time for data update/refresh (ex: bus waits for everyone to get on the bus to leave) 
            - Real Time: data is updated continuously (ex: escalator brings people upstairs constantly) 
    - Latency: data can come at different speeds
    - Important to undercover hidden patterns and correlations, deeper insights
    - Importance lies in how it is used 
    - Challenge is in the storage, processing and analysis 


Data Sources:
    - On premises
    - Cloud
- 
    

    - Types
        ○ Data bases (relations, non-relational)
        ○ File based (excel, csv, Json, xml) 
Forms
    - Structed: has a fixed style/organization (ex: excel sheet, sql)
        ○ Easily queryable
        ○ Has defined manner or schema, typically found in relational db
        ○ Organized in rows and columns, type is well defined 
    - Unstructured: no predefined structure (ex; email or social media post)
        ○ Not easily queryable without preprocessing
        ○ Various formats
        ○ Does not have a specific schema
        ○ Tends to be text heavy 
        ○ Ex: video, audio files. Images, text files without a fixed format 
    - Semi structured: a hybrid (ex: Json, XML)
        ○ Elements might be tagged or categorized in some way
        ○ More flexible but still a bit chaotic
        ○ Neither raw or fitted to a conventional database system 
        ○ Ex: xml or Json
        ○ Ex: json files for policy information, XML files for claim processing 



Modern Data Architecture
    - Data lakes, lake houses, cloud 
    - In order to have chat gpt, chat bots, ai we need clean and trusted data that has been integrated from multiple sources 
        ○ Goal is to bring all this data into one place that is integrated (snowflake is where we store that clean and trusted data) 
    

- One source of truth, homogenous data (don’t need to connect to each source every time), don’t want to touch the source directly (decoupling),
- Data ware house - Snowflake
    - Pay for storage (cheap) it is the engine that is expensive bc running 24/7
    - Sql query engine
- Data lake - S3
    - Just paying for storage not the engine 
- Lakehouse
    - Lower storage cost, less data movement 
    - Simplified schema
    - Can be expensive and time consuming to build and transfer to this 
    - Data lake but with additional functionality 

















 










