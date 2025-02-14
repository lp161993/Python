import boto3
import pandas as pd
from io import BytesIO


s3_client = boto3.client('s3')

def lambda_handler(event, context):
    try:
      bucket_name = "mysource-lp-s3"                     #Source S3 bucket name
      s3_file_name = "Student performance.csv"           #CSV file to import
      resp = s3_client.get_object(Bucket=bucket_name, Key=s3_file_name)
      df_s3_data = pd.read_csv(resp['Body'], sep=',')
      print(df_s3_data.head(5))

      #Create a filtered dataframe with Student who slept more than 8, got good grades (> 75th percentile)
      grade_75th_percentile = df_s3_data['Grades'].quantile(0.75)
      df_filtered = df_s3_data[(df_s3_data['Sleep Hours'] >= 8) & (df_s3_data['Grades'] >= grade_75th_percentile)]
      print(df_filtered.head(5))
      #print("Total Percentage of well rested students, who scored good grades: ", (len(df_filtered)/len(df_s3_data))*100)

      #The output to the file containing the percentage of students who were well rested and performed well
      file_content = "Total Percentage of well rested students, who scored good grades: " + str((len(df_filtered)/len(df_s3_data))*100) 
      print(file_content)
      s3_path = "mydestination-lp-s3"
   
      # 1. Create an in-memory file-like object using io.BytesIO
      file_obj = BytesIO(file_content.encode())  # Encode the string to bytes

      # 2. Upload the file-like object to S3
      try:
         s3_client.upload_fileobj(
            file_obj,
            s3_path,
            'Report.txt',  
            ExtraArgs={
                  'ContentType': 'text/plain'  # Set the correct content type
            }
         )
         print("File uploaded successfully.")
      except Exception as e:
         print(f"Error uploading file: {e}")

    except Exception as err:
       print(err)

    return {
        'statusCode': 200,
        'body': "All okay"
        
    }



