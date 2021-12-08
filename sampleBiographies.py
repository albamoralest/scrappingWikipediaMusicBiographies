import pandas as pd
import os
import shutil


def list_files(directory):
    files_list = [f for f in os.listdir(directory) if not f.startswith('.')]
    return files_list


list_of_files = list_files("dataset/")
df_files = pd.DataFrame(list_of_files, columns=['file_name'])

ds_shuffle = df_files.sample(n=1000)
ds_shuffle.info()
ds_shuffle.head()
ds_shuffle.to_csv('list_wikiIdSample.csv', index=False)

for file_obj in ds_shuffle.itertuples():
    shutil.copy("dataset/"+file_obj.file_name, 'sample')
