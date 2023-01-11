import sys
import time
import argparse
import json
import psycopg2
import requests
import numpy as np
from scipy.stats import t
import pandas as pd
from sklearn.preprocessing import StandardScaler
from utils.config import *


# List to store the issues detected
LIST_OF_ISSES = []
# Counter for the number of issues detected
ISSUES_DETECTED = 0


def create_log_into_newrelic(tablename, columnname, message, aniocampana, query, country, total_records, count_issues, issues_percentage, issuetype, createjiraticket="No"):
    """
        Send a log to New Relic with the details of an issue detected in the database.

        Parameters:
            tablename (str): name of the table where the issue was detected
            columnname (str): name of the column where the issue was detected
            message (str): description of the issue
            aniocampana (str): campaign year
            query (str): query used to detect the issue
            country (str): country where the issue was detected
            total_records (int): total number of records in the table
            count_issues (int): number of records with the issue
            issues_percentage (float): percentage of records with the issue
            issuetype (str): type of issue detected
            createjiraticket (str): whether to create a JIRA ticket for the issue (optional, default "No")
    """
    # Create a dictionary with the data to be sent to New Relic
    array_data = {
        "table_name": tablename,
        "column_name": columnname,
        "db_user": DB_USER,
        "create_jira_ticket": createjiraticket,
        "message": message,
        "campaign_year": aniocampana,
        "country": country,
        "query": query,
        "records_volume": total_records,
        "records_with_issues": count_issues,
        "accuracy": 100-issues_percentage,
        "issues_percentage": issues_percentage,
        "issue_type": issuetype
    }

    data = {
        "Masivo": {
            "DataQualityValidator": array_data
        }
    }

    # Create the payload to be sent to New Relic
    headers = {"Api-Key": NR_API_KEY}

    LIST_OF_ISSES.append(array_data)

    requests.post(url=NR_API_ENDPOINT, json=data, headers=headers)


class CheckDataBaseIssues:
    """
        Main class
    """
    def __init__(self, anio_campana=0):
        if len(anio_campana) == 2:
            self.aniocampana = anio_campana[1]
        else:
            self.aniocampana = '0'
    
        self.conn = None
        self.cursor = None

    def open_db_connection(self):
        """
        Open a connection to the database.
        """
        try:
            # Connect to the database using the provided credentials
            self.conn = psycopg2.connect(host=DB_HOST_PROD,
                                         port=DB_PORT,
                                         dbname=DB_NAME,
                                         user=DB_USER,
                                         password=str(DB_PASSWORD))

            # Enable autocommit mode
            self.conn.autocommit = True
            # Create a cursor for the connection
            self.cursor = self.conn.cursor()
        except psycopg2.InternalError as error:
            # Print an error message and exit if the connection fails
            print(f'Unable to connect! {error}')
            sys.exit(1)

    def close_connection(self):
        """
        Close the connection to the database.
        """
        self.cursor.close()
        self.conn.close()

    def __get_data_frame_from_db(self, query, columns):
        """
        Get a pandas DataFrame from the database using a given query and list of column names.

        Parameters:
            query (str): the query to execute
            columns (list): list of column names for the DataFrame
            batch_size (int, optional): the number of rows to fetch in each batch. Default is 10000.

        Returns:
            pandas.DataFrame: the resulting DataFrame
        """
        with self.conn, self.conn.cursor() as cursor:
            # Execute the query using the cursor
            cursor.execute(query)

            # Fetch the results
            results = cursor.fetchall()

            # Return a DataFrame of the results using the provided column names
            return pd.DataFrame(results, columns=columns)

    def validate_issues(self):
        """
        Validate for issues in the database.
        """
        # Initialize start and end times for performance tracking
        start_time = time.time()

        # Validate the campaign year to ensure it has a valid length of 6 characters
        if len(self.aniocampana) == 6:

            # Run the data rules
            self.__run_data_rules()
            end_time = time.time()

            # Print the results of the process
            print(
                f"Process complete, Problems found: {ISSUES_DETECTED}, Execution time: {end_time - start_time:.2f} seconds")

        else:
            print("Error: The campaign year must have a length of 6 characters")

    def __parse_detection_query(self, rule_item, excluded_countries):
        """
        Parse the detection query for a given rule.

        Parameters:
            rule_item (dict): dictionary containing the details of the rule
            excluded_countries (str): string of comma-separated country codes to exclude from the query

        Returns:
            tuple: a tuple containing the following elements:
                df_columns (list): list of column names for the dataframe
                query_without_rule (str): query without the rule applied
                query_with_rule (str): query with the rule applied
        """
        # Initialize variables for the select, join, group by, and child conditions clauses of the query
        select_clause, join_clause, groupby_clause, child_conditions = "", "", "", ""

        # Initialize the where clause with the campaign year and excluded countries
        where_clause = f" WHERE {rule_item['Table']}.aniocampana={self.aniocampana}"
        where_clause = f"{where_clause} AND {rule_item['Table']}.codpais NOT IN ({excluded_countries})"

        # Iterate through the join clauses and add them to the join variable
        for col in rule_item['Join']:
            join_clause = f"{join_clause} {col['Value']}"

        # Iterate through the where clauses and add them to the where and child_conditions variables
        for col in rule_item['Where']:
            where_clause = where_clause + " " + col['ParentFilter']
            child_conditions = child_conditions+col['CustomFilter']

        # Initialize the list of columns for the dataframe and the group by clause
        df_columns = []
        df_columns.clear()

        for col in rule_item['GroupBy']:
            groupby_clause = groupby_clause + \
                rule_item['Table']+"."+col['Column']
            df_columns.append(str(col['Column']))
            if col != rule_item['GroupBy'][len(rule_item['GroupBy']) - 1]:
                groupby_clause = groupby_clause + ","

        # Construct the select and group by clauses
        select_clause = f"SELECT {groupby_clause}, COUNT (*) as Results FROM {rule_item['Table']} WITH (NOLOCK)"
        groupby_clause = f"GROUP BY {groupby_clause}"

        # Construct the final queries with and without the rule applied
        query_without_rule = f"{select_clause} {join_clause} {where_clause} {groupby_clause}"
        query_with_rule = f"{select_clause} {join_clause} {where_clause} {child_conditions} {groupby_clause}"

        # Return the dataframe columns, and the queries with and without the rule applied
        return df_columns, query_without_rule, query_with_rule

    def __parse_evaluation_query(self, rule_item, excluded_countries):
        """
        Parse the evaluation query for a given rule.

        Parameters:
            rule_item (dict): dictionary containing the details of the rule
            excluded_countries (str): string of comma-separated country codes to exclude from the query

        Returns:
            tuple: a tuple containing the following elements:
                query (str): the evaluation query
                all_columns (list): list of all column names in the query
                groupby_columns (list): list of column names used in the group by clause
        """
        # Split the group by clause into a list of columns
        groupby_columns = rule_item['GroupBy'].split(',')

        # Initialize the default column and where clause
        default_column, where = "aniocampana", ""

        # Initialize the select clause with the default column
        select = f"SELECT {default_column},"

        # Add the group by columns to the select clause
        for grouped_column in groupby_columns:
            select += f"{grouped_column}, "

        # Split the columns to be evaluated into a list
        ci_columns = rule_item['Columns'].split(',')

        # Add the columns to be evaluated to the select clause
        for i, x in enumerate(ci_columns):
            select += f"{rule_item['ColumnFunction']} as Results"
            if i != len(ci_columns) - 1:
                select += ", "

        # Build the where clause
        for col in rule_item['Where']:
            where += f" {col['Value']}"

        # Append the excluded countries to the where clause
        where += f" AND {rule_item['Table']}.codpais NOT IN ({excluded_countries})"

        # Build the final query
        query = f"{select} FROM {rule_item['Table']} WITH (NOLOCK) WHERE aniocampana<={self.aniocampana} {where} GROUP BY aniocampana, {rule_item['GroupBy']} ORDER BY aniocampana"

        all_columns = [default_column] + groupby_columns + ['Results']

        return query, all_columns, groupby_columns

    def __get_excluded_countries(self, value):
        """
        Get a string of comma-separated country codes to be excluded from a query.

        Parameters:
            value (list): list of country codes to exclude

        Returns:
            str: a string of comma-separated country codes
        """
        # Initialize the string of excluded countries
        excludedcountries = ""
        # Iterate through the list of excluded countries and add them to the string
        for country_ex in value:
            excludedcountries = excludedcountries+"'"+country_ex+"'"
            if country_ex != value[len(value) - 1]:
                excludedcountries = excludedcountries + ","
        # Return the string of excluded countries
        return excludedcountries

    def __get_message_to_newrelic(self, percentage_issues, total_records, records_with_issues, issue_type, fields_combination, additional_message=""):
        """
        Generate a message to be logged in New Relic with details of an issue detected.

        Parameters:
            percentage_issues (str): percentage of records with issues
            total_records (str): total number of records
            records_with_issues (str): number of records with issues
            issue_type (str): type of issue detected
            fields_combination (str): combination of fields involved in the issue
            additional_message (str): additional message to include in the log (optional)

        Returns:
            str: the generated message
        """
        # Initialize the message with the issue type and fields combination
        message = f"Error: {issue_type} para la siguiente combinación de campos: {fields_combination}"

        # Add the percentage of errors and total number of records to the message
        message += f"Porcentaje de errores detectados: {percentage_issues}, Total de Datos: {total_records}"

        # Add the number of records with issues to the message
        message += f", Datos con errores: {records_with_issues}"

        # If an additional message was provided, append it to the message
        if additional_message:
            message = f"{message}, {additional_message}"

        # Return the generated message
        return message

    def __confidence_interval_calculator(self, data_frame, column_name, multiplier_factor):
        """
        Calculate the confidence interval for a given data frame and column.

        Parameters:
            data_frame (pandas.DataFrame): data frame with the data to calculate the confidence interval for
            column_name (str): name of the column in the data frame to calculate the confidence interval for
            multiplier_factor (float): multiplier factor to apply to the confidence interval

        Returns:
            int: 1 if the column values are outside the confidence interval, 0 otherwise
        """
        # Standardize the column data and calculate the mean and standard deviation
        data_frame[column_name] = StandardScaler(
        ).fit_transform(data_frame[[column_name]])
        m = data_frame[column_name].mean()
        s = data_frame[column_name].std()
        n = len(data_frame)
        confidence = 0.95

        # Calculate the critical value for the confidence interval
        t_crit = (np.abs(t.ppf((1-confidence)/2, n-1)))*multiplier_factor

        # Calculate the lower and upper bounds for the confidence interval
        lower_bound, upper_bound = m-s*t_crit/np.sqrt(n), m+s*t_crit/np.sqrt(n)

        # Add a column to the data frame indicating whether each value is outside the confidence interval
        data_frame['ci_results'] = np.where((data_frame[column_name] > upper_bound) | (
            data_frame[column_name] < lower_bound), 1, 0)

        # Return the value of the 'ci_results' column for the last row in the data frame
        return data_frame['ci_results'].iloc[-1]

    def __run_data_rules(self):
        print('Verifying the quality of the Forecast Masivo')

        self.open_db_connection()

        with open('rules.json', 'r') as rules_json:
            data = json.load(rules_json)

        global ISSUES_DETECTED
        excluded_countries = self.__get_excluded_countries(
            data['ExcludedCountries'])

        for rule_item in data['RulesEngine']:

            match rule_item['ValidationType']:
                case "Detection":

                    # Parse the detection query
                    parsed_query = self.__parse_detection_query(
                        rule_item, excluded_countries)

                    # Extract the data frame columns, merged columns index, and queries from the parsed query
                    df_columns = parsed_query[0]
                    df_merged_cols_index = df_columns.copy()
                    # Append the 'Results' column to the data frame columns
                    df_columns.append('Results')

                    # Retrieve the data frames for the queries with and without the rule
                    query_without_rule = parsed_query[1]
                    query_with_rule = parsed_query[2]

                    data_with_rule = self.__get_data_frame_from_db(
                        query_without_rule, df_columns)
                    data_without_rule = self.__get_data_frame_from_db(
                        query_with_rule, df_columns)

                    # Merge the data frames on the merged columns index
                    df_merged = pd.merge(
                        data_with_rule, data_without_rule, how="inner", on=df_merged_cols_index)

                    # Calculate the percentage of results in the second data frame relative to the first data frame
                    df_merged['Results_p'] = round(
                        (df_merged['Results_y'] / df_merged['Results_x']) * 100, 3)

                    # Iterate over the rows in the merged data frame
                    for index, row in df_merged.iterrows():
                        fields_with_issues = ""

                        # Build a string containing the fields with issues
                        for field in df_merged_cols_index:
                            fields_with_issues += f'{field} = {row[field]}, '

                        # Build the message to log to New Relic
                        message = self.__get_message_to_newrelic(str(row.Results_p), str(row.Results_x),
                                                                 str(row.Results_y), rule_item['IssueType'], fields_with_issues)

                        # Increment the count of detected issues
                        ISSUES_DETECTED += 1

                        # Log the data to New Relic
                        create_log_into_newrelic(rule_item['Table'], rule_item['Column'], message, self.aniocampana, query_with_rule,
                                                 row.codpais, row.Results_x, row.Results_y, row.Results_p, rule_item['IssueType'], "Yes")

                case "Evaluation":

                    # Parse the detection query
                    parsed_query = self.__parse_evaluation_query(
                        rule_item, excluded_countries)

                    query = parsed_query[0]
                    all_columns = parsed_query[1]
                    groupby_columns = parsed_query[2]

                    # Retrieve the data frame from the database
                    data_frame = self.__get_data_frame_from_db(
                        query, all_columns)

                    # Group the data frame by the groupby columns
                    grouped_data_frame = data_frame.groupby(groupby_columns)[
                        all_columns]

                    # Iterate over the filtered data frames
                    for df_filter in grouped_data_frame:
                        # Extract the data frame from the group
                        df_filter = df_filter[1]

                        # Calculate the confidence interval for the data frame
                        confidence_interval_high = self.__confidence_interval_calculator(
                            df_filter, 'Results', CI_MULTIPLIER_FACTOR_HIGH)
                        confidence_interval_mid = self.__confidence_interval_calculator(
                            df_filter, 'Results', CI_MULTIPLIER_FACTOR_MID)

                        # Initialize the fields with issues string
                        fieldswithissues = ""

                        # Check if the confidence interval is within the acceptable range
                        if confidence_interval_high == 0:
                            if confidence_interval_mid == 1:

                                # Confidence interval is within the medium range
                                level = "Nivel MEDIO"

                                # Extract the last row of the data frame
                                df_values_error_matching = df_filter.iloc[-1]

                                # Check if the data frame is from the current campaign
                                if (df_values_error_matching.aniocampana == self.aniocampana):
                                    # Build the fields with issues string
                                    for x in groupby_columns:
                                        fieldswithissues += f"{x} = {df_values_error_matching[x]}, "

                                    # Build the message to log to New Relic
                                    message = self.__get_message_to_newrelic(str(round((1/len(df_filter))*100, 3)), str(len(df_filter)),
                                                                             str(1), rule_item['IssueType'], fieldswithissues, level)

                                    # Extract the relevant values from the data frame
                                    country = df_values_error_matching.codpais
                                    tablename = rule_item['Table']
                                    column_name = rule_item['Columns']
                                    results_x = 0
                                    results_y = 0
                                    results_p = 0
                                    ticket_to_jira = "No"

                                    create_log_into_newrelic(tablename, column_name, message, self.aniocampana,
                                                             query, country, results_x, results_y, results_p, rule_item['IssueType'], ticket_to_jira)

                                    ISSUES_DETECTED = ISSUES_DETECTED+1

                        else:
                            # Extract the last row of the data frame
                            df_values_error_matching = df_filter.iloc[-1]

                            # Check if the aniocampana value in the last row of the data frame matches the current aniocampana value
                            if (df_values_error_matching.aniocampana == self.aniocampana):
                                # Iterate through the groupby_columns and add the field and value to the fieldswithissues string
                                for field in groupby_columns:
                                    fieldswithissues += f"{field} = {df_values_error_matching[field]}, "

                                # Calculate the percentage of errors in the data frame as a string
                                error_percentage = str(
                                    round((1/len(df_filter))*100, 3))
                                # Convert the length of the data frame to a string
                                df_length = str(len(df_filter))

                                level = "Nivel ALTO"
                                # Get the message to be sent to New Relic
                                message = self.__get_message_to_newrelic(error_percentage, df_length, str(
                                    1), rule_item['IssueType'], fieldswithissues, level)

                                country = df_values_error_matching.codpais
                                tablename = rule_item['Table']
                                column_name = rule_item['Columns']
                                results_x = 0
                                results_y = 0
                                results_p = 0
                                # Set the ticket_to_jira variable to "Yes"
                                ticket_to_jira = "Yes"

                                # Call the create_log_into_newrelic function
                                create_log_into_newrelic(tablename, column_name, message, self.aniocampana,
                                                         query, country, results_x, results_y, results_p, rule_item['IssueType'], ticket_to_jira)

                                # Increment the ISSUES_DETECTED variable
                                ISSUES_DETECTED = ISSUES_DETECTED+1
           


parser = argparse.ArgumentParser()
parser.add_argument("aniocampana", help="Campaign Year", type=int)
args = parser.parse_args()

# Instantiation of the class
start_checking_db_issues = CheckDataBaseIssues(sys.argv)
start_checking_db_issues.validate_issues()
