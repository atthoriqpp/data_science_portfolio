# Data Science Portfolio
A repository for data science and analytics projects. Portfolios are presented as Jupyter Notebooks/SQL Query Files, Dashboards, and/or Reports (PDF). For Jupyter Notebooks, use the `requirements.txt` file in each project folder to run the notebooks in your environment.

## About me
A driven business administration graduate with a passion for data science. Recently completed Rakamin Academy's Data Science Bootcamp and was awarded the runner-up spot for Best Final Project Team. Proficient in Python, SQL, and Looker Studio, he is always eager to expand his knowledge in the dynamic field of data science.

Before his data science journey, he demonstrated his entrepreneurial skills by launching his coffee shop business in 2019. He later contributed to a 20% increase in clients at GOODVIBES Creative.

Now, he seeks opportunities to apply his data science expertise to real-world scenarios, empowering companies to make informed decisions driven by data insights.

## Certifications and Trainings
1. [Rakamin Academy Data Science Bootcamp](https://drive.google.com/file/d/1zNqwX1lKTFjfao6_X8dMunKTsYbvaIDx/view?usp=sharing) (Rakamin Academy | 2023)
2. [Data Analyst in SQL](https://www.datacamp.com/statement-of-accomplishment/track/5c6499cc62e40dc7db3fd07e68aa9820c70a46be) (DataCamp | 2023)
3. [Data Analyst with R](https://www.datacamp.com/statement-of-accomplishment/track/17b96eb5916fe0df8a09d43c715309af60832dc4) (DataCamp | 2023)
4. [Google Data Analytics Certificate](https://www.credly.com/badges/f1abe8c6-fe17-4214-b05d-0ab5914fa323/linked_in_profile) (Coursera | 2023)

## Projects
- ### Exploratory Analysis and Visualization
  - **Analyzing E-Commerce Business Performance with SQL**
    
     An exploratory data visualization study delved into the core metrics driving an e-commerce's success. Identified actionable insights by analyzing customer activity, revenue, product performance, and payment types and designed a custom dashboard for stakeholders. Additionally, the project showcased PgAdmin (PostgreSQL) for efficient database management.
    
    [Query Files](https://github.com/atthoriqpp/data_science_portfolio/tree/main/analyzing_e-commerce_business_performance_with_sql/query_files) | [Report (PDF)](https://github.com/atthoriqpp/data_science_portfolio/blob/main/analyzing_e-commerce_business_performance_with_sql/Final%20Report_Analyzing%20eCommerce%20Business%20Performance%20with%20SQL_Atthoriq%20Putra%20Pangestu.pdf) | [Dashboard (Looker Studio)](https://lookerstudio.google.com/reporting/cac6363e-b9f8-4dee-a528-de7dd58ba502) | [Technical Documentation (PDF)](https://github.com/atthoriqpp/data_science_portfolio/blob/main/analyzing_e-commerce_business_performance_with_sql/Documentation_Analyzing%20eCommerce%20Business%20Performance%20with%20SQL_Atthoriq%20Putra%20Pangestu.pdf)

  - **Project Based Virtual Intern: Bank Muamalat x Rakamin Academy Database and Dashboard Building**
    
     A project that produced a database and dashboard from synthetic client transaction data (Jan 2020 - Dec 2021). Processed 4 raw transaction data from CSV to Google BigQuery's database (aided by initial Excel work.) Finally, a Looker Studio dashboard summarized key sales trends: total sales, order quantity, transaction count, average sales trends, and product/city breakdowns. This provided valuable insights for the bank client to monitor their overall transaction performance at any given time.
    
    [Project Explanation Video (Indonesian)](https://drive.google.com/file/d/1RRCVNeQjQvHZ8JG3PPdnWw4UrJKR28Nf/view?usp=sharing) | [Query File 1: Data Definition](https://console.cloud.google.com/bigquery?sq=861814418732:01fbfdae05f84426b4ecd8c373ddf4a3) | [Query File 2: Master Table Creation](https://console.cloud.google.com/bigquery?sq=861814418732:14ae8be03926430cbde287b244a71995) | [Dashboard (Looker Studio)](https://lookerstudio.google.com/reporting/baaf9894-26cf-4d87-b60a-253f1b1f964d) | [Data Source](https://github.com/atthoriqpp/data_science_portfolio/tree/794d4a363faf92549eff1b6c8721f5807f0bafe6/project_based_virtual_intern_bank_muamalat_business_intelligence_analyst/dataset)

  - **Project Based Virtual Intern: Kimia Farma x Rakamin Academy Datamart and Dashboard Building**
    
     A project that produced a database, datamart, and built a dashboard for Kimia Farma, an Indonesian pharmaceutical producer and distributor, covering sales data from January 2022 to June 2022. This involved creating a database from raw data and processing sales information using PostgreSQL. As the final step, I developed a sales dashboard using Looker Studio, highlighting key metrics like total sales, average sales, total transactions, top sales by location, top-selling brands, top-selling products, and top customers by store type.

    *Note: Access to the data source is restricted to participants in this program.*
    
    [Query File](https://github.com/atthoriqpp/data_science_portfolio/blob/4638a40903fd7f4d6eb2cb28c211c1638f6deca1/project_based_virtual_intern_kimia_farma_x_rakamin_academy_datamart_and_dashboard_building/query_file/KimiaFarma_SalesQuery.sql) | [Dashboard (Looker Studio)](https://lookerstudio.google.com/reporting/abbcaf17-8598-415d-8988-15aea76160fc) | [Data Source](https://www.rakamin.com/virtual-internship-experience/kimiafarma-big-data-analytics-virtual-internship-program)

  - **Top Universities for International Students**
    
    An informative analysis of universities around the world, tailored specifically for international students seeking useful insights and recommendations. Additionally, this project identifies potential problems in universities, which will be further investigated in future analyses with more relevant data and potential solutions.
    
    [Notebook](https://github.com/atthoriqpp/data_science_portfolio/blob/main/global_university_rankings_2023/top-universities-for-international-students.ipynb) | [Report (PDF)](https://github.com/atthoriqpp/data_analytics_portfolios/blob/main/global_university_rankings_2023/Global%20University%20Rankings%202023%20Analysis.pdf)
    
- ### Machine Learning
  - #### Supervised Learning
    - **E-Commerce Shipping Perspective & Prediction (Classification)**
    
       Analysis of an e-commerce shipments dataset where the company is facing late shipments problem. Produced a classification model using the random forest algorithm and identified the factors that are associated with late deliveries.

      [Notebook](https://github.com/atthoriqpp/data_science_portfolio/blob/main/e-commerce_shipping_prediction/e-commerce-shipment-perspective-prediction.ipynb) | [Report (PDF)](https://github.com/atthoriqpp/data_analytics_portfolios/blob/main/e-commerce_shipping_prediction/E-Commerce%20Shipment%20Prediction.pdf)

  - #### Unsupervised Learning
    - **E-commerce Transaction: RFM Clustering with K-Means (Clustering)**
    
       Analysis of sales data (Nov 2018-Nov 2019 period) from a UK online gift & homeware store (est. 2007) to identify the most promising customer segment for targeted marketing. Used Exploratory Data Analysis (EDA), K-Means Clustering, Principal Component Analysis (PCA), and Recency Frequency Monetary (RFM) analysis to segment and profile the most valuable group, tailoring recommendations to their unique characteristics.

      [Notebook](https://github.com/atthoriqpp/data_science_portfolio/blob/fe2e388ea66389bf17e9735e73b726f44d12d493/e_commerce_transaction_rfm_clustering/e-commerce-analysis-rfm-clustering-with-k-means.ipynb) | [Report (PDF)](https://github.com/atthoriqpp/data_science_portfolio/blob/fe2e388ea66389bf17e9735e73b726f44d12d493/e_commerce_transaction_rfm_clustering/Report_E-Commerce%20Transaction%20RFM%20Clustering.pdf)
   
If you have questions or are interested in knowing more about the portfolio, please feel free to reach out to me either by:
- Email: atthoriqpp.student@gmail.com
- LinkedIn: [Atthoriq Putra Pangestu](https://www.linkedin.com/in/atthoriqputra/)
