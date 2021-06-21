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
import shutil
from azure.storage.blob import BlobServiceClient, BlobClient, ContainerClient
from blob_modules import export_to_blob, import_from_blob
#from text_mining_tf_idf import text_mining

def main():
    """
    This is the main function for the extraction of NR Daily log data
    """
    #get a list of all docx files that need to be loaded
    all_files_to_process = glob('word_documents/*.docx')
    
    print(all_files_to_process)
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
    
    #remove newlines and carriage returns from dataframe
    print("removing newlines")
    full_dataset.replace(r'\n',' ',regex=True,inplace=True)
    print("removing carriage returns")
    full_dataset.replace(r'\r',' ',regex=True,inplace=True)
    #print("remove odd character")
    #full_dataset.replace(r'\â€“',' ',regex=True,inplace=True)
    
    full_appended_dataset = process_files(full_dataset)

    exportfile(full_appended_dataset,'appended_output//','nrlog_appended.csv')

    export_to_blob('appended_output/','nrlog_appended.csv','nr-daily-logs')
    
    #print("file exported to blob so file held locally is removed")
    #os.remove('appended_output//nrlog_appended.csv')

    print ('moving processed word files out of the way to /word_documents/holding pen/already processed/')
    move_processed_word_docs('word_documents//','word_documents/holding pen//already processed//')



def move_processed_word_docs(origin, destination):
    """  
    A function to move processed word documents out of the word_documents folder and into the already processed folder
    Note: this function does not handle the case of the word file already being in the destination folder

    Parameters:
    origin:         A string holding the file path where processed files were initially placed
    destination:    A strong holding the path path of the destination folder.

    Returns:
    NOne, but moves files as required
    """
    files = glob(os.path.join(origin,"*.docx"))
    print(files)
    for file in files:
        if os.path.isfile(file):
            shutil.move(file,destination)
    


def process_files(todays_data):
    """
    This takes the previously appended data and then appends the latest cut of data to the dataframe.  There is code
    to handle the case where no appended dataset is in the 'appended output' folder (ie first use of code)

    Parameters:
    todays_data:    A pandas dataframe holding the latest cut of data

    Returns:
    all_data:       A pandas dataframe holding the latest and previously appended data
    """

    list_of_files = glob('appended_output\nrlog_appended.csv')

    #first run when no appended data exists, create blank dataframe
    if not list_of_files:
        appended_data = pd.DataFrame(columns=['incident_date','route','ccil','narrative','found_location','latitude','longitude','postcode']   )
    
        appended_data.to_csv('appended_output//nrlog_appended.csv')

    #os.remove('appended_output//nrlog_appended_test.csv')
    #get apppended data and then remove it
    print("here's the import")
    import_from_blob('nr-daily-logs','nrlog_appended.csv','appended_output//nrlog_appended_blob.csv')
    
    
    try:
        appended_data = pd.read_csv(r"appended_output\nrlog_appended_blob.csv", encoding='latin1')    
    except UnicodeEncodeError:
        appended_data = pd.read_csv(r'appended_output\nrlog_appended_blob.csv', encoding='utf-8')  
            
    os.remove('appended_output//nrlog_appended_blob.csv')

    #join previous joined data and remove false index column
    all_data = pd.concat([appended_data,todays_data],ignore_index=True)
    all_data = all_data.loc[:, ~all_data.columns.str.contains('^Unnamed')]

    print(all_data)

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
    docdf['narrative'] = docdf['narrative'].apply(lambda x: x.replace('–CCIL','– CCIL'))
    docdf['narrative'] = docdf['narrative'].apply(lambda x: x.replace('CCIL', '– CCIL'))
    docdf['narrative'] = docdf['narrative'].apply(lambda x: x.replace('. CCIL', '– CCIL'))
    docdf['narrative'] = docdf['narrative'].apply(lambda x: x.replace('. Fault No. ','/ Fault No. '))

    #split the narrative column by the appropriate delimiter
    docdf[['route','narrative']] = docdf['narrative'].str.split(' – ',1,expand=True)
    docdf[['ccil','narrative']]  = docdf['narrative'].str.split('/ ',1,expand=True)
    
    print("this is the narrative in raw. useful to check/n")
    print(docdf['narrative'])

    #remove none rows from docdf
    docdf.dropna(axis=0,subset=['ccil'],inplace=True)

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
    print(f"Have finished processing data for {day}/{month}/{year}")

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
    destinationfilename = f'{filename}'
    print(f"Exporting {filename} to {destinationpath}{destinationfilename}\n")
    print(f"Exporting {filename} to {destinationpath}\n")
    print(f"If you want to check on progress, refresh the folder "+ destinationpath + " and check the size of the " + filename + ".csv file. \n")  
    
    #catching odd encoding/characters in the source file.
    try:
        df.to_csv(destinationpath + destinationfilename, encoding='cp1252',index=False)
    except UnicodeEncodeError:
        df.to_csv(destinationpath + destinationfilename, encoding='utf-8',index=False)


if __name__ == '__main__':
    main()
