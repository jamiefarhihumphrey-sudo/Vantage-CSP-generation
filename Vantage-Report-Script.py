import requests, json, time, pandas as pd, openpyxl
import datetime, calendar    #, xlsxwriter
from datetime import date
import boto3
from botocore.exceptions import ClientError


def get_secrets() -> dict[str, str]:
    """
    Retrieve Datadog and Vantage API keys from AWS Secrets Manager.
    Returns:
        dict[str, str]: Dictionary containing the secrets.
    """
    secret_name: str = ############
    region_name: str = "us-east-2"
    session = boto3.session.Session()
    client = session.client(service_name='secretsmanager', region_name=region_name)
    try:
        get_secret_value_response: dict = client.get_secret_value(SecretId=secret_name)
    except ClientError as e:
        raise e
    json_string: str = get_secret_value_response['SecretString']
    secrets = json.loads(json_string)
    secrets = secrets['############']
    return secrets


Vantage_Token = get_secrets()


def calcdate(input_date=None):
    """
    Calculates the first and last day of a month based on the input date or current date.

    Args:
        input_date (datetime.date, optional): A date object. If None, the current date is used.

    Returns:
        tuple: A tuple containing two datetime.date objects (first_day_of_month, last_day_of_month).
    """

    if input_date:
        # If a date is provided
        print(f"Using provided date.")
        startdate = input_date.replace(day=1)
        _, num_days = calendar.monthrange(input_date.year, input_date.month)
        enddate = input_date.replace(day=num_days)
        return startdate, enddate
    else:
        # If no date is provided, use the current date
        print(f"Generating date off of runtime.")
        current_date = datetime.date.today()
        print(f"Today: {current_date}")
        if current_date.day < 15:
            # If the current date is less than the 15th
            # Calculate the first and last day of the previous month
            print(f"Final run for previous month.")
            startdate = current_date.replace(day=1)
            enddate = startdate - datetime.timedelta(days=1)
            startdate = enddate.replace(day=1)
            return startdate, enddate
        else:
            # If the current date is the 15th or later
            # Calculate the first and last day of the current month
            print(f"Preliminary run for this month.")
            startdate = current_date.replace(day=1)
            _, num_days = calendar.monthrange(current_date.year, current_date.month)
            enddate = current_date.replace(day=num_days)
            return startdate, enddate


def get_reports(startdate, enddate):
    url = "https://api.vantage.sh/v2/costs/data_exports"

    payloadamortized = {
        "schema": "vntg",
        "settings[include_credits]": False,
        "settings[include_refunds]": False,
        "settings[include_discounts]": True,
        "settings[include_tax]": True,
        "settings[amortize]": True,
        "settings[unallocated]": False,
        "settings[aggregate_by]": "cost",
        "cost_report_token": "####",    # amoritized report token
        "workspace_token": "####",
        "start_date": startdate,
        "end_date": enddate
    }
    payloadnonamortized = {
        "schema": "vntg",
        "settings[include_credits]": False,
        "settings[include_refunds]": False,
        "settings[include_discounts]": True,
        "settings[include_tax]": True,
        "settings[amortize]": False,
        "settings[unallocated]": False,
        "settings[aggregate_by]": "cost",
        "cost_report_token": "####",    # non-amortized token
        "workspace_token": "####",
        "start_date": startdate,
        "end_date": enddate
    }
    headers = {
        "accept": "application/json",
        "content-type": "application/x-www-form-urlencoded",
        "authorization": "Bearer " + Vantage_Token
    }
    #Trigger report creation
    amortized = requests.post(url, data=payloadamortized, headers=headers)
    nonamortized = requests.post(url, data=payloadnonamortized, headers=headers)

    #get location of download request
    amorturl = amortized.headers['location']
    nonamorturl = nonamortized.headers['location']
    #time for report generation
    # could use an active polling loop, but as this was intended to be run once a month on schedule
    # time efficiency was not a major concern.
    print("sleeping 10 minutes to let reports generate")
    time.sleep(600)

    #request download links
    amort = requests.get(amorturl, data=None, headers=headers)
    nonamort = requests.get(nonamorturl, data=None, headers=headers)

    aparsed = json.loads(amort.content)
    nparsed = json.loads(nonamort.content)
    print(aparsed)
    print(nparsed)
    aurl = str(aparsed['manifest']['files'])[2:-2]
    nurl = str(nparsed['manifest']['files'])[2:-2]
    print("Amortized report url: " + aurl)
    print("NON-Amortized report url: " + nurl)
    #downloading reports
    response1 = requests.get(aurl, data=None, headers=None)
    if response1.status_code == 200:
        with open(f"amortized-report-{startdate}.csv", "wb") as f:    # Open in binary write mode
            f.write(response1.content)
        print("Amortized Report downloaded successfully!")
    else:
        print("Error downloading file: {response1.status_code}")

    response2 = requests.get(nurl, data=None, headers=None)
    if response2.status_code == 200:
        with open(f"non-amortized-report-{startdate}.csv",
                  "wb") as f:    # Open in binary write mode
            f.write(response2.content)
        print("Non-Amortized report downloaded successfully!")
    else:
        print("Error downloading file: {response2.status_code}")
    return (response1.status_code, response2.status_code)


def combine_reports(startdate):
    amortized = pd.read_csv(f"amortized-report-{startdate}.csv")
    regular = pd.read_csv(f"non-amortized-report-{startdate}.csv")
    #reports are generated with 'month' as the column header.
    #Side by side, they need to be labeled 'amortized' or 'non-amortized'
    amortized.rename(columns={amortized.columns[-1]: 'Amortized'}, inplace=True)
    regular.rename(columns={regular.columns[-1]: 'Non-Amortized'}, inplace=True)
    #removing service, provider columns that aren't wanted
    amortized.drop(['Provider', 'Service'], axis=1, inplace=True)
    regular.drop(['Provider', 'Service'], axis=1, inplace=True)
    amortized = amortized.groupby(['Account', 'Account Name'])['Amortized'].sum()
    regular = regular.groupby(['Account', 'Account Name'])['Non-Amortized'].sum()

    combined_df = pd.merge(amortized, regular, on=['Account', 'Account Name'], how='outer')
    #combined_df has the stitched-together values.
    #still requires label, allocation, title
    reference = pd.read_csv("Account Reference.csv")
    combined_df = pd.merge(reference, combined_df, on=['Account', 'Account Name'], how='left')
    pared_df = combined_df[combined_df['Label'] != 'NAN']
    print(pared_df)
    ###sheet is now finalized and correctly formatted, minus two columns
    pared_df['Attribution'] = pared_df['Amortized'] - pared_df['Non-Amortized']
    atotal = pared_df['Attribution'].sum()
    pared_df['Attribution Percentage'] = pared_df['Attribution'] / atotal
    print(pared_df)
    return (pared_df)


def label_report(pared_df):
    labeldf = pared_df.groupby('Label').agg(Total_Spend=('Attribution', 'sum'), Percentage_Spend=('Attribution Percentage','sum')).reset_index()
    labeldf.sort_values(by='Total_Spend', ascending=False, inplace=True)
    # Calculate the totals for 'Total Spend' and 'Percentage Spend'
    total_spend_sum = labeldf['Total_Spend'].sum()
    percentage_spend_sum = labeldf['Percentage_Spend'].sum()
    # Create a new DataFrame for the total row
    total_row = pd.DataFrame({
        'Label': ['Total'],
        'Total_Spend': [total_spend_sum],
        'Percentage_Spend': [percentage_spend_sum]
    })
    # Concatenate the new_df with the total_row
    labeldf = pd.concat([labeldf, total_row], ignore_index=True)
    labeldf = labeldf.round(2)
    print(labeldf)
    return (labeldf)


def allocation_report(pared_df):
    allocdf = pared_df.groupby('Allocation').agg(Total_Spend=('Attribution', 'sum'),Percentage_All=('Attribution Percentage', 'sum')).reset_index()
    allocdf.sort_values(by='Total_Spend', ascending=False, inplace=True)
    #calculating allocation-only percentages
    allocation_total = allocdf['Total_Spend'].sum()
    allocdf['Percentage_Spend'] = allocdf['Total_Spend'] / allocation_total

    # Calculate the totals for 'Total Spend' and 'Percentage Spend'
    total_spend_sum = allocdf['Total_Spend'].sum()
    percentage_spend_sum = allocdf['Percentage_Spend'].sum()
    percentage_all_sum = allocdf['Percentage_All'].sum()
    # Create a new DataFrame for the total row
    total_row = pd.DataFrame({
        'Allocation': ['Total'],
        'Total_Spend': [total_spend_sum],
        'Percentage_Spend': [percentage_spend_sum],
        'Percentage_All': [percentage_all_sum]
    })
    # Concatenate the new_df with the total_row
    allocdf = pd.concat([allocdf, total_row], ignore_index=True)
    #reorder columns
    allocdf = allocdf.loc[:, ['Allocation', 'Total_Spend', 'Percentage_Spend', 'Percentage_All']]
    allocdf = allocdf.round(2)
    print(allocdf.reset_index(drop=True))
    return (allocdf.reset_index(drop=True))


def title_report(pared_df):
    titledf = pared_df.groupby('Title').agg(Total_Spend=('Attribution', 'sum'),
                                            Percentage_All=('Attribution Percentage',
                                                            'sum')).reset_index()
    #calculating allocation-only percentages
    title_total = titledf['Total_Spend'].sum()
    titledf['Percentage_Spend'] = titledf['Total_Spend'] / title_total
    #sorting
    titledf.sort_values(by='Total_Spend', ascending=False, inplace=True)
    # Calculate the totals for 'Total Spend' and 'Percentage Spend'
    total_spend_sum = titledf['Total_Spend'].sum()
    percentage_spend_sum = titledf['Percentage_Spend'].sum()
    percentage_all_sum = titledf['Percentage_All'].sum()
    # Create a new DataFrame for the total row
    total_row = pd.DataFrame({
        'Title': ['Total'],
        'Total_Spend': [total_spend_sum],
        'Percentage_Spend': [percentage_spend_sum],
        'Percentage_All': [percentage_all_sum]
    })
    # Concatenate the new_df with the total_row
    titledf = pd.concat([titledf, total_row], ignore_index=True)
    # reorder columns
    titledf = titledf.loc[:, ['Title', 'Total_Spend', 'Percentage_Spend', 'Percentage_All']]
    titledf = titledf.round(2)
    print(titledf.reset_index(drop=True))
    return (titledf.reset_index(drop=True))


def main(date_str=None):    #event, context):

    #date_str=event.get('date')
    #date_str='2025-04-04'
    if date_str:
        try:
            date_str = datetime.datetime.strptime(date_str, '%Y-%m-%d').date()
        except ValueError:
            return {
                'statusCode': 400,
                'body': json.dumps('Invalid date format, please use YYYY-MM-DD')
            }

    startdate, enddate = calcdate(date_str)
    print(f"Range: {startdate}, {enddate}")

    #startdate = "2025-04-01"
    #enddate = "2025-04-30"
    statuscodes = get_reports(startdate, enddate)
    print(statuscodes)
    #def main():
    stitched_report = combine_reports(startdate)
    labeldf = label_report(stitched_report)
    allocdf = allocation_report(stitched_report)
    titledf = title_report(stitched_report)
    stitched_report = stitched_report.round(2)
    with pd.ExcelWriter(f"{startdate} CSP impacted by AWS Account.xlsx") as writer:
        labeldf.to_excel(writer, sheet_name="by Label")
        allocdf.to_excel(writer, sheet_name="by Allocation")
        titledf.to_excel(writer, sheet_name="by Title")
        stitched_report.to_excel(writer, sheet_name="Data")
