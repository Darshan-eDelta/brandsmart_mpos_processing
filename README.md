
# direct_marketing_file_processing

direct_marketing_file_processing is a script that search and download file from files.com and it reads data files, validates and processes the data, inserts it into a database, generates quality reports, create postcard and upload postcard and unprocessed records into monday.com and add contact into mailchimp.

## 1. searchAndDownloadFils

This function is used to retrieve files from the remote server and save them locally for further processing. It can be part of an automated data retrieval process.

## 2. import_files

* `read_file`:

This function is designed to read structured text files, such as those with pipe-separated values, and convert them into a DataFrame, handling missing columns and logging any potential issues. It's a useful utility for preprocessing data from text files for further analysis or database insertion.

* `validate_file`:

This function is designed to ensure that the input DataFrame contains the expected columns defined in the header list. If any of the columns are missing, it logs an error message and returns False. Otherwise, it returns True to indicate that the DataFrame is valid and contains the necessary columns for further processing.

* `filter_invalid_plan_types`:

This function ensures that only rows with valid plan types are retained in the DataFrame by validating plan types against a database. Invalid rows are filtered out, and a quality report is updated with relevant information.

* `qualty_report`:

This function essentially computes and updates various quality metrics based on the input DataFrame, which can be useful for monitoring and reporting the quality of the data. The metrics include the count of unique email addresses, unique offer codes, and counts of rows with specific conditions.

* `insert_data`:

The code primarily focuses on inserting data from a DataFrame into a database and keeping track of inserted, unprocessed, and error-prone records. It also generates a quality report based on the insertion process. The SQL queries executed in this process should match the schema and database structure of the underlying database.

* `update_table`:

This function update specific columns in the `post_sale_marketing` table with values from other related tables (`contractprocessingmappedskus`, `gbs_warr_progs_reserves`, and `gbs_warranty_progs`) based on certain conditions. These updates are crucial for maintaining data consistency and integrity within the database. The actual SQL queries are dynamically generated and executed within the Python script.

* `import_quality_file`:

This code constructs an SQL query to insert quality-related data into a database table. It also integrates with Monday.com to track the imported data. The specific purpose of this code is to maintain a record of data quality metrics related to the import process.

* `import_to_monday`:

This code serves as an integration point with the Monday.com platform, allowing data from the obj dictionary to be structured and sent as an item to a specified board with associated column values. The code captures and returns the unique identifier of the created item for further reference or tracking purposes. It's a useful way to automate data reporting and tracking in Monday.com based on data processed in the Python script.

* `post_card`:

This code fetches data from the database, creates a CSV file with the specified data, updates a date field in the database for records related to the batch, and returns the name of the generated CSV file. This file appears to be a postcard file.

* `upload_file`:

This code uploads a `post card` to an item in a Monday.com board by making a GraphQL API request.

* `upload_file2`:

This code uploads a `unprocessed records` to an item in a Monday.com board by making a GraphQL API request.

* `add_contact`:

This function is add contact into `mailchimp` if environment is `prod`.

* `moveFile`:

This function is move file to destination in files.com.


## Where the program runs

currently, the program runs on a local system, not a remote server.

## What am I doing when you send a file to me/ticket

1. I will download the file to my local system
2. Then upload that file to files.com in the "/726-regency/direct_marketing_inbound/" path
3. Then I will run the program
4. The program will take some time to finish 2 to 3 hours
5. Once the program run is completed, I will check the following to verify the data:
   - files.com: The file should be moved to the process_files folder
   - Monday.com: Our file and records should be displayed on the board

### Step 1 to 3
[(Step 1 to 3[])](https://github.com/user-attachments/assets/67fcdfa2-1475-4adb-a635-e9dc8e8fe5c4)

### Step 5
[(Step 5[])](https://github.com/user-attachments/assets/1e4e19db-b045-4f4e-8fbf-02ae91868b28)

## LogDNA

https://app.mezmo.com/7d51ac61b1/logs/view/259c64cc9f?apps=direct-marketing

## Deploy

* This project is deploy on `AWS` fargarte using `CI-CD GitHub Action`.

* Build the Docker image using the `Dockerfile` in repository. This includes specifying the base Python image, installing dependencies, and copying your application code into the image.

* Create an ECR repository to store your Docker images

. ECR is a Docker image registry service provided by AWS.

* Create IAM user and group with filter policies : `amazon ECR full access`. Generate Access keys and save in `GitHub secrets` with all `ENV variables`.

* Create a GitHub Actions workflow file `(.github/workflows/master.yml)` in your GitHub repository to automate the CI/CD process.

* And Push the code to github That trigger workflow and push docker image to ECR and rest is managed by Liubomyr Rykhva.
