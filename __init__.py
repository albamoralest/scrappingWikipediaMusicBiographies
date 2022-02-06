# wikipedia library
import pandas as pd
import wikipedia
import time
import json
import os
import csv
# set wikipedia library configuration
language = "en"
wikipedia.set_lang(language)

# read file with identifiers of people in music
# TODO: read the file with the list of people in music
#  the list has ~ 34000 people, read the file in cluster
#  use the file configuration to store the current cluster and the last id read
#  start reading each id with the given library and then store the results in a text file
#  name the file wikiId_Name, name should be converted getting rid of special characters and
#  concatenating name and surname

# testing
# people_list = ['Glenn_Miller', 'Andrés_Segovia', 'Django_Reinhardt', 'Maria_Callas', 'Édith_Piaf', 'John_Lennon']
# wikiId_list = ['64610', '71932', '9039', '64966', '64963', '15852']

# resources_directory = "listBiographies_byDOB/"
resources_directory = "sparqlQueryResults/"
dataset_directory = "dataset/"
file_name = "TOTAL_biography.csv"
configuration_file = "download_progress.json"


# # reading with wikipedia library return only text, no HTML tags
# def read_biography():
#     for person in wikiId_list:
#         print(person)
#         page = wikipedia.page(pageid=person)
#         content = page.content
#         time.sleep(5)
#         write_text_file(person, content)


def write_text_file(filename, file_content):
    """Write text file
    Args:
    filename: name of the file
    content: text to write in the file"""
    f = open(dataset_directory + filename + '.txt', "w")
    f.writelines(file_content)
    f.close()


def create_download_progress_file():
    file_content = {}
    file_content['file_id'] = 0
    file_content['first_identifier'] = 0
    file_content['last_identifier'] = 0
    file_content['last_biography'] = 0

    with open(resources_directory + configuration_file, 'w') as outfile:
        json.dump(file_content, outfile)


def update_configuration_file(file_content):
    with open(resources_directory + configuration_file, 'w') as outfile:
        json.dump(file_content, outfile)


def load_configuration_file():
    obj = None
    if os.path.isfile(resources_directory + configuration_file):
        with open(resources_directory + configuration_file) as json_file:
            obj = json.load(json_file)
    return obj


def create_csv_results():
    """Write csv file with updated results of recent downloaded web pages
    Args:
    filename: name of the file
    file_content: text to write in the file"""
    file_name = "listBiographies_results"
    field_header = ['wikiId', "s", "birth", 'fileName']
    with open(resources_directory + file_name + '.csv', mode='w') as csv_file:
        writer = csv.DictWriter(csv_file, fieldnames=field_header)
        writer.writeheader()


def append_dataframe(fileName, newDataFrame):
    newDataFrame.to_csv(resources_directory + fileName, mode='a', index=False, header=False)


result = None
# configuration file, create if not exists
if not os.path.isfile(resources_directory + configuration_file):
    create_download_progress_file()

# read file
configurations = load_configuration_file()
start = 1
if configurations['file_id'] == 0:
    create_csv_results()
    start = 1
else:
    start = configurations['file_id']

# review if web page already exist every time a new chunck is loaded in memory
check_download = True

# for item in range(start, 4):
# df_list = pd.read_csv("20211205_sparql_list_"+str(1)+".csv")
# read the file with the list of people in music, divide in chunks of 100 rows
# the list has ~ 21000 people, read the file in cluster
# update file
# configurations['file_id'] = item
# print(item)

# if check_download == True
# List the web pages already downloaded and create a temporal doc with the new list of
# web pages to download, update accordingly the download_progress file
# return a list object of files in the given folder
if check_download:
    files_list = [f for f in os.listdir(dataset_directory) if not f.startswith('.')]
    df_files_list = pd.DataFrame(files_list, columns=['file_name'])
    df_files_list['file_name'] = df_files_list['file_name'].str.replace('.txt', '')

    # read total web pages list
    df_list_biographies = pd.read_csv(resources_directory + file_name)
    df_remaining = df_list_biographies[~df_list_biographies['id'].isin(df_files_list['file_name'])]
    df_remaining.to_csv(resources_directory + 'TEMP_' + file_name, index=False)
    file_name = 'TEMP_'+file_name

for chunk in pd.read_csv(resources_directory+file_name, chunksize=100):
    df_biographies = pd.DataFrame()
    try:
        df_errors = pd.DataFrame()
        # read each identifier
        # TODO:
        #  a) save a register of the biographies effectively read, update everytime it reads a new
        #  control or when a problem arise, and try to read again
        #  b) save the last read in order to start again
        df_biographies['id'] = chunk['id']
        df_biographies['s'] = chunk['s']
        # df_biographies['birth'] = chunk['birth']
        df_biographies['id'] = df_biographies['id'].map(str)
        # print(df_biographies.iat[0, 0])
        # print(configurations['last_identifier'])
        # print(configurations['first_identifier'])
        # if id in first row of chunk is lower than (first identifier of chunk)
        if int(df_biographies.iat[0, 0]) < int(configurations['first_identifier']):
            continue
        # verify if read
        else:
            # column Zero in df_biographies is ID = wikiId
            configurations['first_identifier'] = df_biographies.iat[0, 0]
            configurations['last_identifier'] = df_biographies.iat[len(df_biographies)-1, 0]
            update_configuration_file(configurations)
            # start reading all the id's in file
            for df_row in df_biographies.itertuples():
                print(df_row.id)
                # print(configurations['last_biography'])
                if int(df_row.id) <= int(configurations['last_biography']):
                    continue

                # Scrapping wiki page
                page = wikipedia.page(pageid=df_row.id)
                # read text only
                content = page.content
                # save results in text file
                write_text_file(df_row.id, content)
                # # update register
                df_series = {'id': df_row.id, 's': df_row.s, 'filename': df_row.id+'.txt'}
                append_dataframe('listBiographies_results.csv', pd.DataFrame([df_series]))
                # # update configuration file
                configurations['last_biography'] = df_row.id
                update_configuration_file(configurations)
                time.sleep(15)
    except Exception as ex:
        if hasattr(ex, 'message'):
            print(ex.message)
        else:
            print(ex)
        df_errors.append([{'id': df_row.id, 'err_mess': ex}])
    finally:
        # update configuration file
        update_configuration_file(configurations)

    df_errors.to_csv(resources_directory+'errors.csv', index=False)


# for person in wikiId_list:
#     print(person)
