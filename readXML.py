"""
This will process all XMLs and produces file output file for publisher of Kafka

incorrect_format_files: list of files having xml tag errors
non_xml_files: list of non XML files in given dir, which will not get processed
invalid_temp_files: files having temperature value error
Developer: Shrikant Bhadane
"""

import xml.etree.ElementTree as ET
import os
import pandas as pd
import json
from operator import itemgetter, attrgetter
import csv

def getCountryNPop(json_dir):
    dict_city_cntry={}
    city=()
    cntry_pop_list=[]
    cntry_pop_lists=[]
    sorted_dict={}
    
    for fn in os.listdir(json_dir):
        
        with open(json_dir+'\\'+fn) as city_fn:
            
            country_data = json.load(city_fn)
            city=country_data['city'].capitalize()
            cntry_pop_list.append(country_data['country'].capitalize())
            cntry_pop_list.append(country_data['population_M'])
            cntry_pop_list.append(country_data['updated_at_ts'])
            cntry_pop_lists.append(cntry_pop_list)

        #If city already present then append to it
        if city in dict_city_cntry.keys():
            dict_city_cntry[city]=dict_city_cntry[city]+cntry_pop_lists
        else:
            dict_city_cntry[city]=cntry_pop_lists
            
        cntry_pop_list=[]
        cntry_pop_lists=[]

    for k,v in dict_city_cntry.items():
        sorted_dict[k]=sorted(v, key = lambda x: x[2], reverse=True)
    return(sorted_dict)


def logErrors(pfields, prows, pfn):
    # field names 
    fields = pfields
    
    # data rows of csv file 
    with open(pfn, 'w') as f:
        # using csv.writer method from CSV package
        write = csv.writer(f)
        write.writerow(pfields)
        write.writerow([prows])
    
def readFiles(xml_dir,city_country_pop_map,outfile_dir):

    incorrect_format_files=[]
    non_xml_files=[]
    invalid_temp_files={}
    unit=''
    both_units=True
    cols=['Country','City','Population','Date','Measured_at_ts','Temp_Celsius','Temp_Fahrenheit']
    vals=[]
    val=[]
    file_cnt=0


    #loop trough all files in give dir, extract data only from XML files
    for fn in os.listdir(xml_dir):
        if '.xml' in fn :
            try:
                tree = ET.parse(os.path.join(xml_dir,fn))
                root = tree.getroot()
                
                #Get Country & city
                for i in root.iter('city'):
                    val.append(city_country_pop_map[i.text.capitalize()][0][0])
                    val.append(i.text.capitalize())
                    val.append(city_country_pop_map[i.text.capitalize()][0][1])
                    
                #Get timestamp
                for i in root.iter('measured_at_ts'):
                    val.append(i.text[0:i.text.index('T')])
                    val.append(i.text[(i.text.index('T')+1):])
                    
                #Get Temp. if it has a value error then log it
                for i in root.iter('value'):

                    try:
                        temp=float(i.text)
                    except ValueError:
                        invalid_temp_files[fn]=i.text
                        val=[]
                    else:
                        unit=tuple(i.attrib.items())[0][1]

                        #Check unit of value
                        if unit.lower() == 'celsius':
                            val.append(round(temp,2))
                            #Convert celsius to fahrenheit
                            #e.g.27°C×9/5+32
                            val.append(round((temp*9/5+32),2))
                            both_units=False
                            
                        if unit.lower() == 'fahrenheit' and both_units:
                            #Convert fahrenheit to celsius, stroing celsius value 1st in list
                            #e.g.(80.6°F-32)×5/9
                            val.append(round(((temp-32)*5/9),2))
                            val.append(round(temp,2))

                        #Don't loop when both units present, take one and convert it
                        vals.append(val)
                        val=[]
                        both_units=True
                        break
                    
                
            except Exception as e:
                #print("Exception:",e)
                incorrect_format_files.append(fn)
        else:
            non_xml_files.append(fn)
        
        file_cnt+=1

    #print("count",file_cnt)
    #print(vals)
    sorted_l=sorted(vals, key=itemgetter(1,3,4), reverse=True)
    df=pd.DataFrame(sorted_l,columns=cols)

    #Create final output file to pass to procuder of kafka
    df.to_csv(outfile_dir+"\\Outfile.csv",sep=",",index=False,header=True,columns=cols)
    
    #Log errors
    logErrors(['FileName'], incorrect_format_files, outfile_dir+"\\FormatErrFiles.csv")
    logErrors(['FileName'], non_xml_files, outfile_dir+"\\NonXMLFiles.csv")
    logErrors(['FileName','Value'], invalid_temp_files, outfile_dir+"\\ValueErrFiles.csv")
       
    
def InitPaths():
    #dir path of xml files
    directory_of_python_script = os.path.dirname(os.path.abspath(__file__))
    xml_dir = directory_of_python_script.replace("bin","data")+r"\temperatures"
    json_dir = directory_of_python_script.replace("bin","data")+r"\city-country"
    outfile_dir=directory_of_python_script.replace("bin","data")
    
    city_country_pop_map=getCountryNPop(json_dir)
    readFiles(xml_dir,city_country_pop_map,outfile_dir)
