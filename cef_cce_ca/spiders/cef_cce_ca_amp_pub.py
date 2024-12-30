from scrapy.cmdline import execute
from lxml.html import fromstring
from datetime import datetime
from typing import Iterable
from scrapy import Request
import pandas as pd
import unicodedata
import random
import scrapy
import evpn
import time
import re
import os


# Function to convert date formats
def convert_date_format(text):
    # Replace non-breaking spaces with regular spaces
    text = text.replace('\xa0', ' ')  # '\xa0' is unicode for non-breaking space
    # Regular expression to find standalone dates and date ranges
    date_pattern = r'(\w+ \d{1,2}, \d{4})|Between (\w+ \d{1,2}) and (\w+ \d{1,2}, \d{4})'

    # Replace dates with the new format
    def replace_date(match):
        if match.group(1):  # Standalone date
            date_str = match.group(1)
            date_obj = datetime.strptime(date_str, '%B %d, %Y')
            return date_obj.strftime('%Y-%m-%d')
        elif match.group(2) and match.group(3):  # Date range
            start_date_str = match.group(2) + f", {match.group(3)[-4:]}"  # Add year to start date
            start_date_obj = datetime.strptime(start_date_str, '%B %d, %Y')
            end_date_obj = datetime.strptime(match.group(3), '%B %d, %Y')
            return f"Between {start_date_obj.strftime('%Y-%m-%d')} and {end_date_obj.strftime('%Y-%m-%d')}"

    # Substitute the dates in the text
    converted_text = re.sub(date_pattern, replace_date, text)
    return converted_text


def get_date_modified(selector, xpath_date_modified):
    date_modified = selector.xpath(xpath_date_modified)[0]
    return date_modified if date_modified not in ['', ' '] else 'N/A'


def replace_with_na(text):
    return re.sub(pattern=r'^[\s_-]+$', repl='N/A', string=text)  # Replace _, __, -, --, --- with N/A


def remove_punctuation(text):
    if text == 'N/A':
        return text
    return ''.join(char for char in text if not (unicodedata.category(char).startswith('P') and char != '|'))


# Function to remove Extra Spaces from Text
def remove_extra_spaces(text: str) -> str:
    return re.sub(pattern=r'\s+', repl=' ', string=text).strip()  # Regular expression to replace multiple spaces and newlines with a single space


def remove_diacritics(input_str):
    return input_str if input_str == 'N/A' else ''.join(char for char in unicodedata.normalize('NFD', input_str) if not unicodedata.combining(char))


def df_cleaner(data_frame) -> pd.DataFrame:
    # Apply the function to all columns for Cleaning
    data_frame = data_frame.astype(str)  # Convert all data to string
    data_frame.drop_duplicates(inplace=True)  # Remove duplicate data from DataFrame

    for column in data_frame.columns:
        data_frame[column] = data_frame[column].astype(str).apply(replace_with_na)  # Convert to string before applying

        if 'date' in column:
            data_frame[column] = data_frame[column].apply(convert_date_format)
        elif 'name' in column:
            data_frame[column] = data_frame[column].apply(remove_punctuation)  # Remove punctuation

        data_frame[column] = data_frame[column].apply(remove_extra_spaces)  # Remove extra spaces
        data_frame[column] = data_frame[column].apply(remove_diacritics)  # Remove diacritics characters
    data_frame.drop(columns=['name_of_individual_corporation_or_entity'], errors='ignore', inplace=True)  # Remove column as 'name' already exists in another column
    data_frame.replace(to_replace=r'^\s*$', value=None, regex=True, inplace=True)
    data_frame.replace(to_replace='nan', value=pd.NA, inplace=True)  # After cleaning, replace 'nan' strings back with actual NaN values
    data_frame.fillna(value='N/A', inplace=True)  # Replace NaN values with "N/A"
    return data_frame


class CefCceCaAmpPubSpider(scrapy.Spider):
    name = "cef_cce_ca_amp_pub"

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        print('Connecting to VPN (CANADA)')
        self.api = evpn.ExpressVpnApi()  # Connecting to VPN (CANADA)
        self.api.connect(country_id='79')  # canada country code
        time.sleep(10)  # keep some time delay before starting scraping because connecting
        print('VPN Connected!' if self.api.is_connected else 'VPN Not Connected!')

        self.final_data_list = list()

        # Path to store the Excel file can be customized by the user
        self.excel_path = r"../Excel_Files"  # Client can customize their Excel file path here (default: govtsites > govtsites > Excel_Files)
        os.makedirs(self.excel_path, exist_ok=True)  # Create Folder if not exists
        self.filename = fr"{self.excel_path}/{self.name}.xlsx"  # Filename with Scrape Date

        self.cookies_table_page = {
            'cookiesession1': '678B286DC8FB3CADC7C4041793DB9AD3',
            'AMCVS_A90F2A0D55423F537F000101%40AdobeOrg': '1',
            'AMCV_A90F2A0D55423F537F000101%40AdobeOrg': '-1124106680%7CMCIDTS%7C20015%7CMCMID%7C88163800731590971171861175626912495338%7CMCAAMLH-1729846327%7C3%7CMCAAMB-1729846327%7CRKhpRz8krg2tLO6pguXWp5olkAcUniQYPHaMWWgdJ3xzPWQmdj0y%7CMCOPTOUT-1729248727s%7CNONE%7CMCAID%7CNONE%7CvVersion%7C5.2.0',
            'gpv_pu': 'www.cef-cce.ca%2Fcontent.asp',
            'gpv_pthl': 'blank%20theme',
            'gpv_pc': 'Commissioner%20of%20Canada%20Elections',
            'gpv_url': 'www.cef-cce.ca%2Fcontent.asp',
            's_cc': 'true',
            's_sq': '%5B%5BB%5D%5D',
            'gpv_pt': 'Publication%20of%20AMPs',
            'gpv_pqs': '%3Fsection%3Damp%26dir%3Dpub%26document%3Dindex%26lang%3De',
            's_plt': '0.77',
            's_ips': '2737.60009765625',
            's_tp': '6210',
            's_ppv': 'Publication%2520of%2520AMPs%2C78%2C44%2C4846%2C6%2C8',
        }
        self.cookies_data_page = {
            'cookiesession1': '678B286DC8FB3CADC7C4041793DB9AD3',
            'AMCVS_A90F2A0D55423F537F000101%40AdobeOrg': '1',
            's_cc': 'true',
            's_sq': '%5B%5BB%5D%5D',
            'gpv_pu': 'www.cef-cce.ca%2Fcontent.asp',
            'gpv_pthl': 'blank%20theme',
            'gpv_pc': 'Commissioner%20of%20Canada%20Elections',
            'gpv_url': 'www.cef-cce.ca%2Fcontent.asp',
            's_plt': '1.76',
            'AMCV_A90F2A0D55423F537F000101%40AdobeOrg': '-1124106680%7CMCIDTS%7C20015%7CMCMID%7C88163800731590971171861175626912495338%7CMCAAMLH-1729860138%7C12%7CMCAAMB-1729860138%7CRKhpRz8krg2tLO6pguXWp5olkAcUniQYPHaMWWgdJ3xzPWQmdj0y%7CMCOPTOUT-1729262538s%7CNONE%7CMCAID%7CNONE%7CvVersion%7C5.2.0',
            'gpv_pt': 'Summary%20of%20the%20Notice%20of%20Violation',
            'gpv_pqs': '%3Fsection%3Damp%26dir%3Dpub%26document%3Dmay0621-kl%26lang%3De',
            's_ips': '1250.2000122070312',
            's_tp': '1319',
            's_ppv': 'Summary%2520of%2520the%2520Notice%2520of%2520Violation%2C95%2C95%2C1250.2000122070312%2C1%2C1',
        }
        self.headers = {
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7',
            'Accept-Language': 'en-US,en;q=0.9,id;q=0.8',
            'Cache-Control': 'max-age=0',
            'Connection': 'keep-alive',
            'Sec-Fetch-Dest': 'document',
            'Sec-Fetch-Mode': 'navigate',
            'Sec-Fetch-Site': 'none',
            'Sec-Fetch-User': '?1',
            'Upgrade-Insecure-Requests': '1',
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/129.0.0.0 Safari/537.36',
            'sec-ch-ua': '"Google Chrome";v="129", "Not=A?Brand";v="8", "Chromium";v="129"',
            'sec-ch-ua-mobile': '?0',
            'sec-ch-ua-platform': '"Windows"',
        }
        self.browsers = ["chrome110", "edge99", "safari15_5"]

    def start_requests(self) -> Iterable[Request]:
        yield scrapy.Request(url='https://www.cef-cce.ca/content.asp?section=amp&dir=pub&document=index&lang=e', cookies=self.cookies_table_page, headers=self.headers,
                             method='GET', meta={'impersonate': random.choice(self.browsers)}, callback=self.parse, dont_filter=True)

    def parse(self, response, **kwargs):
        selector = fromstring(response.text)
        table = selector.xpath('//div[@id="accordionGroup"]')[0]
        year_divs = table.xpath('.//h2')
        data_divs = table.xpath('./div')

        for year_div, data_div in zip(year_divs, data_divs):
            year = ' '.join(year_div.xpath('.//span//text()'))
            months_divs = data_div.xpath('.//fieldset//table')

            for month_div in months_divs:
                month = ' '.join(month_div.xpath('.//tr[1]//th//text()'))
                headers_divs = month_div.xpath('.//tr[2]//th')  # Get headers from the second row
                headers = [' '.join(header_div.xpath('.//text()')) for header_div in headers_divs]

                # Iterate through the data rows (skipping the header rows)
                data_rows = month_div.xpath('.//tr[position() > 2]')  # Get all rows after the header rows
                for data_row in data_rows:
                    data_dict = {'url': response.url}
                    for header_index, header in enumerate(headers):
                        # Get the corresponding td for the header
                        td_xpath = f'.//td[position()={header_index + 1}]//text()'  # 1-based index
                        data_dict[header] = ' '.join(data_row.xpath(td_xpath)).strip()

                        # Check if this header is "Name" to extract the link
                        if header == 'Name':
                            link_xpath = f'.//td[position()={header_index + 1}]//a/@href'  # Get the href attribute
                            data_dict['data_page_url'] = 'https://www.cef-cce.ca/' + data_row.xpath(link_xpath)[0] if data_row.xpath(link_xpath) else 'N/A'

                    data_dict['year'] = year
                    data_dict['month'] = month

                    # Sending request on Detail page url
                    if data_dict['data_page_url'] != 'N/A':
                        yield scrapy.Request(url=data_dict['data_page_url'], headers=self.headers, cookies=self.cookies_data_page, cb_kwargs={'data_dict': data_dict},
                                             method='GET', meta={'impersonate': random.choice(self.browsers)}, callback=self.parse_data_page)

    def parse_data_page(self, response, **kwargs):
        selector = fromstring(response.text)
        data_dict = kwargs['data_dict']

        # Extract date modified
        xpath_date_modified = '''//dl[@id='wb-dtmd' and @property="dateModified"]/dd/time/text()'''
        date_modified = get_date_modified(selector, xpath_date_modified)
        data_dict['date_modified'] = date_modified

        # Extract data from the table
        rows = selector.xpath('//table[@class="table table-striped table-hover"]/tr')
        for row in rows:
            header = row.xpath('th/text()')
            value = row.xpath('td/text()')

            if header and value:
                header_text = header[0].strip()  # Get the header text
                value_text = value[0].strip()  # Get the value text
                data_dict[header_text] = value_text  # Add to the dictionary

        # Extract text from 'Key facts of violation'
        key_facts_paragraphs = selector.xpath('//h3[normalize-space()="Key facts of violation"]/following-sibling::p/text()')
        key_facts_text = ' '.join(paragraph.strip() for paragraph in key_facts_paragraphs)
        data_dict['key_facts_of_violation'] = key_facts_text

        # Debugging output
        # print(data_dict)
        self.final_data_list.append(data_dict)
        print('-' * 100)

    def close(self, reason):
        print('closing spider...')
        if self.final_data_list:
            print("Converting List of Dictionaries into DataFrame then into Excel file...")
            try:
                print("Creating Excel sheet...")
                data_df = pd.DataFrame(self.final_data_list)
                data_df.drop_duplicates(inplace=True)  # Removing Duplicate data from DataFrame
                data_df = data_df.astype(str)  # Convert all data to string
                # normalize headers by joining each with '_' instead of blank-space and lowercase too
                # Clean the column names
                data_df.columns = [remove_punctuation(col) for col in data_df.columns]  # Removing punctuation
                # Convert the Index to a Series to use str methods
                data_df.columns = pd.Series(data_df.columns).str.lower().str.replace(' ', '_')

                # native_df.columns = [col.replace(',', '') for col in native_df.columns]  # Removing comma ',' from column names
                data_df = df_cleaner(data_frame=data_df)  # Apply the function to all columns for Cleaning
                with pd.ExcelWriter(path=self.filename, engine='xlsxwriter', engine_kwargs={"options": {'strings_to_urls': False}}) as writer:
                    data_df.insert(loc=0, column='id', value=range(1, len(data_df) + 1))  # Add 'id' column at position 1
                    data_df.to_excel(excel_writer=writer, index=False)

                print("Excel file Successfully created.")
            except Exception as e:
                print('Error while Generating Native Excel file:', e)
        else:
            print('Final-Data-List is empty, Hence not generating Excel File.')
        if self.api.is_connected:  # Disconnecting VPN if it's still connected
            self.api.disconnect()


if __name__ == '__main__':
    execute(f'scrapy crawl {CefCceCaAmpPubSpider.name}'.split())
