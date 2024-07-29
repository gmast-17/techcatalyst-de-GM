import configparser
import pandas as pd
import streamlit as st
import boto3

#streamlit run streamlit_main.py in terminal
config = configparser.ConfigParser()
config.read('aws2.cfg')
AWS_ACCESS_KEY = config['AWS']['aws_access_key']
AWS_SECRET_KEY = config['AWS']['aws_secret_key']

s3_client = boto3.client('s3', aws_access_key_id = AWS_ACCESS_KEY, aws_secret_access_key = AWS_SECRET_KEY)

st.title('NYC Taxi Data Analysis')

# output = s3.download_file(Bucket="capstone-techcatalyst-raw", Key='yellow_taxi/yellow_tripdata_2023-09.parquet', Filename='nyc.parquet')

df = pd.read_parquet('nyc.parquet')

cols = st.multiselect('Select Columns', df.columns)

if st.checkbox("Show Data"):
    st.dataframe(df[cols].head(20))


    # read CSV file
    file = st.file_uploader("Upload your CSV data", type=["csv"])
    st.write(file)

    ts = df[['tpep_pickup_datetime', 'total_amount']]
    ts['date'] = df['tpep_pickup_datetime'].dt.date
    ts_sum = ts.groupby('date')[['total_amount']].mean()
    ts_sum.reset_index(inplace=True)
    ts_sum['date'] = pd.to_datetime(ts_sum['date'])

    if st.checkbox('Show line chart'):
        st.line_chart(ts_sum.set_index('date'))


    st.write('Top 10 Pick up Locations')
    st.dataframe(df['PULocationID'].value_counts().head(10))

    st.bar_chart(df['PULocationID'].value_counts().head(10))

    # example using selectbox to filter data
    st.write('Filter data by payment type')
    payment_type = st.selectbox('Payment Type', df['payment_type'].unique())
    st.write(df[df['payment_type'] == payment_type])


    # add calendar widget to filter data by date
    st.write('Filter data by date')

    date = st.date_input('Date', df['tpep_pickup_datetime'].max())
    st.write(df[df['tpep_pickup_datetime'].dt.date == date])

st.divider()