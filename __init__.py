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
#  the list has ~ 21000 people, read the file in cluster
#  use the file configuration to store the current cluster and the last id read
#  start reading each id with the given library and then store the resuls in a text file
#  name the file wikiId_Name, name should be converted getting rid of special characters and
#  concatenating name and surname

people_list = ['Glenn_Miller', 'Andrés_Segovia', 'Django_Reinhardt', 'Maria_Callas', 'Édith_Piaf', 'John_Lennon']
wikiId_list = ['64610', '71932', '9039', '64966', '64963', '15852']

resources_directory = "listBiographies_byDOB/"
dataset_directory = "dataset/"
file_name = "20211205_sparql_list_"
configuration_file = "configurations.json"


# reading with wikipedia library return only text, no HTML tags
def read_biography():
    for person in wikiId_list:
        print(person)
        page = wikipedia.page(pageid=person)
        content = page.content
        time.sleep(5)
        write_text_file(person, content)


def write_text_file(filename, content):
    """Write text file
    Args:
    filename: name of the file
    content: text to write in the file"""
    f = open(dataset_directory + filename + '.txt', "w")
    f.writelines(content)
    f.close()


def create_configuration_file():
    content = {}
    content['file_id'] = 0
    content['first_identifier'] = 0
    content['last_identifier'] = 0
    content['last_biography'] = 0

    with open(resources_directory + configuration_file, 'w') as outfile:
        json.dump(content, outfile)


def update_configuration_file(content):
    with open(resources_directory + configuration_file, 'w') as outfile:
        json.dump(content, outfile)


def load_configuration_file():
    obj = None
    if os.path.isfile(resources_directory + configuration_file):
        with open(resources_directory + configuration_file) as json_file:
            obj = json.load(json_file)
    return obj


def create_csv_results():
    # 1. File one: Answers to all the CES annotations
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
    create_configuration_file()

# read file
configurations = load_configuration_file()
start = 1
if configurations['file_id'] == 0:
    create_csv_results()
    start = 1
else:
    start = configurations['file_id']

for item in range(start, 4):
    # df_list = pd.read_csv("20211205_sparql_list_"+str(1)+".csv")
    # read the file with the list of people in music, divide in chunks of 100 rows
    # the list has ~ 21000 people, read the file in cluster
    # update file
    configurations['file_id'] = item
    print(item)

    for chunk in pd.read_csv(resources_directory+file_name+str(item)+".csv", chunksize=100):
        df_biographies = pd.DataFrame()
        try:
            df_errors = pd.DataFrame()
            # read each identifier
            # TODO:
            #  a) save a register of the biographies effectively read, update everytime it reads a new
            #  control when a problem arise, and try to read again
            #  b) save the last read in order to start again
            df_biographies['id'] = chunk['id']
            df_biographies['s'] = chunk['s']
            df_biographies['birth'] = chunk['birth']
            df_biographies['id'] = df_biographies['id'].map(str)
            # print(df_biographies.iat[0, 0])
            # print(configurations['last_identifier'])
            # print(configurations['first_identifier'])
            if int(df_biographies.iat[0, 0]) < int(configurations['first_identifier']):
                continue
            # verify if read
            else:
                # column Zero in df_biographies is ID = wikiId
                configurations['first_identifier'] = df_biographies.iat[0, 0]
                configurations['last_identifier'] = df_biographies.iat[len(df_biographies)-1, 0]
                update_configuration_file(configurations)
                # TODO:
                #  update results in json file:
                for df_row in df_biographies.itertuples():
                    print(df_row.id)
                    # print(configurations['last_biography'])
                    if int(df_row.id) <= int(configurations['last_biography']):
                        continue

                    # Scrapping wiki page
                    page = wikipedia.page(pageid=df_row.id)
                    content = page.content
                    write_text_file(df_row.id, content)
                    # # update register
                    df_series = {'id': df_row.id, 's': df_row.s, 'birth': df_row.birth, 'filename': df_row.id+'.txt'}
                    append_dataframe('listBiographies_results.csv', pd.DataFrame([df_series]))
                    # # update configuration file
                    configurations['last_biography'] = df_row.id
                    time.sleep(20)
        except Exception as ex:
            if hasattr(ex, 'message'):
                print(ex.message)
            else:
                print(ex)
            df_errors.append([{'id': df_row.id}])
        finally:
            # update configuration file
            update_configuration_file(configurations)

        df_errors.to_csv(resources_directory+str(item)+'errors.csv', index=False)


# for person in wikiId_list:
#     print(person)
