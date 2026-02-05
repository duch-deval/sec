import csv
import os
import boto3 
from dotenv import load_dotenv

load_dotenv()

AWS_ACCESS_KEY_ID= os.getenv("AWS_ACCESS_KEY_ID")
AWS_SECRET_ACCESS_KEY = os.getenv("AWS_SECRET_ACCESS_KEY")
AWS_DEFAULT_REGION = os.getenv("AWS_DEFAULT_REGION")
AWS_S3_BUCKET= os.getenv("AWS_S3_BUCKET")



# Get s3 indenture files from a directory. Consider putting indenture files we want to test in a directory

s3 = boto3.client(
    "s3",
    aws_access_key_id=AWS_ACCESS_KEY_ID,
    aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
    region_name=AWS_DEFAULT_REGION
)



BUCKET = AWS_S3_BUCKET
PREFIX = "SampleIndentures/" # The directory the files are in

response = s3.list_objects_v2(
    Bucket=BUCKET,
    Prefix=PREFIX
)

files = [
    obj["Key"]
    for obj in response.get("Contents", [])
    if not obj["Key"].endswith("/")
] # Do not grab /BaseDirectory, only grab the files inside of it

print(files)

# This is how you get the contents of the html files
key = files[0]
obj = s3.get_object(Bucket=BUCKET, Key=key)
content = obj["Body"].read().decode("utf-8")



# To write list values (governing law(s), mapping(s), etc.), use ', '.join(list) for lists with string elements
# To write list values with non-string elements, use ', '.join(str(s) for s in [1,2,3])
data = [
    ["Berkshire Hathaway Inc.", "10/23/2024", "8K", "https://www.sec.gov/ix?doc=/Archives/edgar/data/0001067983/000119312524241941/d840653d8k.htm",
    "4.2", "Officer's certificate", "1.031% Senior Notes Due 2027",1.031 , '¥58,000,000,000', "2027", "084670 EJ3", "XS2919188834",
    ', '.join(str(s) for s in [4]), ', '.join(['“Business Day” means any day, other than a Saturday or Sunday, that is not a day on which banking institutions in the Borough of Manhattan, The City of '
    'New York or London or Tokyo are authorized or required by law, regulation or executive order to close and that is a day on which the Trans-European Automated '
    'Real-time Gross Settlement Express Transfer System (the TARGET system), or any successor or replacement for that system, operates.']),
    "Terms and Conditions", ', '.join(["New York", "London", "Tokyo", "TARGET"]), ', '.join(["US", "EN", "JN", "TE"])],
    ["Essential Utilities Inc", "8/7/2025", "8K", "https://www.sec.gov/Archives/edgar/data/78128/000155278125000248/e25281_ex4-3.htm",
    "4.3", "Indenture or Supp Indenture",  "5.250% Senior Notes due 2035", 5.250, "$500,000,000", "2035","29670G AK8", "US29670GAK85",
    ', '.join(str(s) for s in [10]), ', '.join(['“Business Day” means any day other than a Saturday, Sunday or any day on which banking institutions in New York, New York are authorized or obligated by '
    'applicable law or executive order to close or be closed.']), "Terms and conditions", ', '.join(["New York"]), ', '.join(["US"])]
]

fieldnames = ['Issuer', 'File Date', 'File Type', 'File Link', 'Exhibit', "Description of Exhibit", "Security Description",
    'Coupon Rate', 'Issue Size', 'Maturity Date', 'CUSIP', 'ISIN', 'Page Number(s)', 'Phrase(s)', 'Governing Law Type',
    'Governing Law(s)', 'Mapping(s)']
# Write to the csv file
with open('governing_law_extractions.csv', 'w', newline='', encoding='utf-8-sig') as csvfile: # encode with utf-8-sig to handle special characters properly
    
    writer = csv.writer(csvfile)
    writer.writerow(fieldnames)
    writer.writerows(data)

