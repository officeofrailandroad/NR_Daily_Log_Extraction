import docx
from docx import Document
from docx.document import Document as _Document
from docx.oxml.text.paragraph import CT_P
from docx.oxml.table import CT_Tbl
from docx.table import _Cell, Table
from docx.text.paragraph import Paragraph
import datetime
import re
import pandas as pd
import numpy as np
from glob import glob
import os
from azure.storage.blob import BlobServiceClient, BlobClient, ContainerClient


def main():
    """
    This is the main function for the extraction of NR Daily log data
    """
    #get a list of all docx files that need to be loaded
    all_files_to_process = glob('word_documents/*.docx')
    
    #loop through each file processing it in turn
    holding_list = []
    for word_doc_name in all_files_to_process:

        doc = docx.Document(word_doc_name)

        docaslist = list()
        finallist = list()
        
        for block in iter_block_items(doc):
            if isinstance(block, Paragraph):
                docaslist.append(block.text)
            
            #elif isinstance(block, Table):
            #    docaslist.append(table_print(block))
        
        #a series of functions to convert single column narrative into a data frame
        docdf = cleanthelist(docaslist)
        docdf = getrouteccil(docdf)
        docdf = getlocation(docdf)
        
        dateofincident = getdate(word_doc_name)
        docdf.insert(0,'incident_date' , dateofincident)

        holding_list.append(docdf)
    
    #join each dataframe together
    full_dataset = pd.concat(holding_list)
        
    full_appended_dataset = process_files(full_dataset)

    #get previously loaded data
    #import_from_blob('nr-daily-logs','nrlog_appended.csv')

    exportfile(full_appended_dataset,'appended_output//','nrlog_appended')

    export_to_blob('appended_output//','nrlog_appended.csv','nr-daily-logs')

    

def process_files(todays_data):
    """
    This takes the previously appended data and then appends the latest cut of data to the dataframe.  There is code
    to handle the case where no appended dataset is in the 'appended output' folder (ie first use of code)

    Parameters:
    todays_data:    A pandas dataframe holding the latest cut of data

    Returns:
    all_data:       A pandas dataframe holding the latest and previously appended data
    """

    list_of_files = glob('appended_output//nrlog_appended.csv')

    #first run when no appended data exists, create blank dataframe
    if not list_of_files:
        appended_data = pd.DataFrame(columns=['incident_date','route','ccil','narrative','found_location','latitude','longitude','postcode']   )
    
        appended_data.to_csv('appended_output//nrlog_appended.csv')

    #get apppended data and then remove it
    appended_data = pd.read_csv('appended_output//nrlog_appended.csv', encoding='cp1252')    
    os.remove('appended_output//nrlog_appended.csv')

    #join previous joined data and remove false index column
    all_data = pd.concat([appended_data,todays_data],ignore_index=True)
    all_data = all_data.loc[:, ~all_data.columns.str.contains('^Unnamed')]

    return all_data


def getlocation(cleandoc):
    """
    This matches the locations in the daily log against a spreadsheet containing known locations and then adds geographical
    co-ordinates with duplicated data being removed and unknown locations being replaced by route names
    
    Parameters
    cleandoc:       A data frame holding the extract NR Log data

    Returns
    doc_with_geog:  A data frame holding the Log data with geographical data
    
    """

    #extract data from csv of locations, remove LUL data and sort data
    print("getting location information from 'location_data' folder")
    location_data = pd.read_csv('location_data\\location_data.csv', encoding='cp1252',usecols=['location_name','latitude','longitude','postcode','location_type'])
    location_data_no_LUL = location_data[location_data['location_type']!= 'LUL_station']
    location_list = list(location_data_no_LUL['location_name'])
    sorted_location_list = sorted(location_list)

    #look for place names after 'at','between' or 'approaching'
    print("looking for locations")
    location_candidates = []
    location_final = []
    for counter, incident in enumerate(cleandoc['narrative']):
        for loc in sorted_location_list:
            
            if ('at ' + loc) in incident or ('between ' + loc) in incident or ('approaching ' + loc) in incident:
                location_candidates.append(loc)
        #remove duplicated location names
        distinct_locations = sorted(set(location_candidates),key = lambda x:location_candidates.index(x))
        
        #check for incidents where no location is found and replace with 'route' placeholder
        if not distinct_locations:
            final_location = 'route'
            
        else:
            #return the location name with the longest name
            final_location = max(distinct_locations,key=len)
        
        location_final.append(final_location)
        location_candidates = []
    
    #insert the location into the "found location" column
    cleandoc['found_location'] = location_final

    #merge the geographical data with daily log dataframe.  remove the unncessary location type column. drop duplicates
    doc_with_geog =  pd.merge(cleandoc,location_data_no_LUL,left_on='found_location',right_on='location_name',how='left')
    doc_with_geog.drop(['location_type'],axis=1,inplace=True)
    doc_with_geog = doc_with_geog.drop_duplicates()

    #drop the location_name column
    doc_with_geog.drop(['location_name'],axis =1, inplace=True)

    #replace the 'route' placeholder with the route column information
    doc_with_geog['found_location'] = np.where(doc_with_geog['found_location']=='route',doc_with_geog['route'],doc_with_geog['found_location'])


    return doc_with_geog



def getrouteccil(docdf):
    """
    This splits the data frame column to produce new columns holding route and ccil information

    Parameters
    docdf:      A dataframe holding the paragraphs of the document

    Returns
    docdf:      A dataframe with new columns in appropriate order

    """
   
    #replace non-standard delimiter in text: NR are not consistent. surprise, surprise
    docdf['narrative'] = docdf['narrative'].apply(lambda x: x.replace(' - ',' – '))
    docdf['narrative'] = docdf['narrative'].apply(lambda x: x.replace('CCIL', '– CCIL'))
    docdf['narrative'] = docdf['narrative'].apply(lambda x: x.replace('. CCIL', '– CCIL'))
    docdf['narrative'] = docdf['narrative'].apply(lambda x: x.replace('. Fault No. ','/ Fault No. '))

    #split the narrative column by the appropriate delimiter
    docdf[['route','narrative']] = docdf['narrative'].str.split(' – ',1,expand=True)
    docdf[['ccil','narrative']]  = docdf['narrative'].str.split('/ ',1,expand=True)
    
    print(docdf['narrative'])

    #remove full stops and hypens from ccil
    docdf['ccil'] = docdf['ccil'].apply(lambda x: x.replace('– CCIL', 'CCIL'))
    docdf['ccil'] = docdf['ccil'].apply(lambda x: x.replace('.',''))
    
    docdf = docdf[['route','ccil','narrative']]

    return docdf
    

def getdate(doc_title):
    """
    Gets date from the title of the file and parsing the string to return a datetime object

    Parameters:
    docobj:            A string containing the title of the word file

    Returns:
    dateofincident:    A datetimeobject representing the date of the incident
    """
    rawdateofincident = doc_title[15:26]
    year = int(rawdateofincident[0:5])
    month = int(rawdateofincident[5:7])
    day = int(rawdateofincident[7:10])

    dateofincident = datetime.date(year,month,day)
    print(f"Now processing data for {day}/{month}/{year}")

    return dateofincident


def cleanthelist(text):
    """
    This takes the list of paragraphs from the word document and removes irrelevant entries.  It finds paragraphs with the key
    reference point CCIL and appends them and the following paragraph to a new list.  This new list is then converted to a dataframe
    
    Parameters
    text:       A docx Document object containing the full document

    Returns
    textdf:     A dataframe holding the relevant text documents
    """
    
    finallist = list()
    
    #remove non-reports
    cleanerdoc = list(filter(None,text))
    #remove first 26 items - the cover page
    #cleanerdoc = cleanerdoc[26:]

    cleanerdoc = [i for i in cleanerdoc if not i.startswith('None')]
    cleanerdoc = [i for i in cleanerdoc if not i.startswith('Disconnected')]

    #mask for the CCIL codes
    ccil = [i for i, s in enumerate(cleanerdoc) if 'CCIL' in s]

    #join ccil codes and ccil text
    for i in ccil:
        finallist.append(cleanerdoc[i] +" / "+ cleanerdoc[i+1])

    textdf = pd.DataFrame(finallist,columns=['narrative'])


    return textdf


##unashamedly stolen from https://github.com/python-openxml/python-docx/issues/276
def iter_block_items(parent):
    """
    Generate a reference to each paragraph and table child within *parent*,
    in document order. Each returned value is an instance of either Table or
    Paragraph. *parent* would most commonly be a reference to a main
    Document object, but also works for a _Cell object, which itself can
    contain paragraphs and tables.
    """
    if isinstance(parent, _Document):
        parent_elm = parent.element.body
    elif isinstance(parent, _Cell):
        parent_elm = parent._tc
    else:
        raise ValueError("something's not right")

    for child in parent_elm.iterchildren():
        if isinstance(child, CT_P):
            yield Paragraph(child, parent)
        elif isinstance(child, CT_Tbl):
            yield Table(child, parent)


def table_print(block):
    tablelist = list()
    table=block
    for row in table.rows:
        for cell in row.cells:
            for paragraph in cell.paragraphs:
                tablelist.append(paragraph.text)
               
    return tablelist


def exportfile(df,destinationpath,filename,numberoffiles=1):
    """
    This procedure exports the finalised file as a CSV file 

    Parameters:
    df        - a dataframe containing the finalised data
    destinationpath     - a string providing the filepath for the csv file
    numberoffiles       - an int with the number of files being processed
    
    Returns:
    None, but does export dataframe df as a csv object
    """
    #formatted_date = datetime.datetime.now().strftime('%Y%m%d_%H-%M')
    #destinationfilename = f'{filename}_{formatted_date}.csv'
    destinationfilename = f'{filename}.csv'
    print(f"Exporting {filename} to {destinationpath}{destinationfilename}\n")
    print(f"Exporting {filename} to {destinationpath}\n")
    print(f"If you want to check on progress, refresh the folder "+ destinationpath + " and check the size of the " + filename + ".csv file. \n")  
    df.to_csv(destinationpath + destinationfilename, encoding='cp1252',index=False)

def import_from_blob(container_name,local_file_name):
    try:
        # Retrieve the connection string for use with the application. The storage
        # connection string is stored in an environment variable on the machine
        # running the application called AZURE_STORAGE_CONNECTION_STRING. If the environment variable is
        # created after the application is launched in a console or with Visual Studio,
        # the shell or application needs to be closed and reloaded to take the
        # environment variable into account.
        
        connect_str = os.getenv('AZURE_STORAGE_CONNECTION_STRING')
        
        # Create the BlobServiceClient object which will be used to connect a container client
        blob_service_client = BlobServiceClient.from_connection_string(connect_str)

        # Define where the file will be downloaded
        download_file_path_and_name = 'appended_output//nrlog_appended.csv'

        #get the container location
        blob_client = blob_service_client.get_blob_client(container=container_name, blob=local_file_name)

        print("\Downloading historic NR daily log data from Azure Storage as blob:\t" + local_file_name)
        #down the file with a context handler
        with open(download_file_path_and_name, "wb") as download_file:
            download_file.write(blob_client.download_blob().readall())

    except Exception as ex:
        print('Exception:')
        print(ex)


def export_to_blob(source_path,source_file_name,container_name):
    try:
        
        # Retrieve the connection string for use with the application. The storage
        # connection string is stored in an environment variable on the machine
        # running the application called AZURE_STORAGE_CONNECTION_STRING. If the environment variable is
        # created after the application is launched in a console or with Visual Studio,
        # the shell or application needs to be closed and reloaded to take the
        # environment variable into account.
        connect_str = os.getenv('AZURE_STORAGE_CONNECTION_STRING')

        # Create the BlobServiceClient object which will be used to connect a container client
        blob_service_client = BlobServiceClient.from_connection_string(connect_str)

        # Define the container
        container_client = blob_service_client.get_container_client(container_name)

        # Create a file in local data directory to upload and download
        local_path = source_path
        local_file_name = source_file_name
        upload_file_path = os.path.join(local_path, local_file_name)

        # Create a blob client using the local file name as the name for the blob
        blob_client = blob_service_client.get_blob_client(container=container_name, blob=local_file_name)

        print("\nUploading NR daily log data to Azure Storage as blob:\t" + local_file_name)

        # Upload the created file
        with open(upload_file_path, "rb") as data:
            blob_client.upload_blob(data,overwrite=True)

    except Exception as ex:
        print('Exception:')
        print(ex)


if __name__ == '__main__':
    main()
