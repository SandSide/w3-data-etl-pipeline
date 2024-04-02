import re
import os
# from user_agents import parse

# s = 'Mozilla/4.0+(compatible;+MSIE+7.0;+Windows+NT+5.1;+SIMBAR={425A48EA-D0F7-11DE-A478-001E9090E619};+InfoPath.2;+.NET+CLR+2.0.50727;+OfficeLiveConnector.1.3;+OfficeLivePatch.0.0)'

# def extract_browser(string):
#     parsed_ua = parse(string)
#     return parsed_ua.os.family

# print(extract_browser(s))


s = '2009-11-02,07:58:52,/dmsutils.js,Mozilla/4.0+(compatible;+MSIE+7.0;+Windows+NT+6.0;+WOW64;+SLCC1;+.NET+CLR+2.0.50727;+.NET+CLR+3.5.21022;+.NET+CLR+3.5.30729;+.NET+CLR+3.0.30618;+InfoPath.2;+.NET+CLR+1.1.4322),131.111.37.76,210'
s2 = '2010-08-09,17:32:18,/Darwin/"+++markerList[i][0]+++",Mozilla/5.0+(Windows;+U;+Windows+NT+6.0;+en-US;+rv:1.9.2.8)+Gecko/20100722+Firefox/3.6.8,186.42.4.43,743'



def process_log_line(line):    
    
    split = line.split(',')
    
    clean_s = re.sub(r'[^\w/]', ' ', split[2])
    
    return clean_s

    # if (len(split) == 14):
    #     browser = split[9].replace(',','')
    #     out = split[0] + ',' + split[1] + ',' + split[4] + ',' + browser + ',' + split[8] + ',' + split[13] 
    #     return out
        
    # elif (len(split) == 18):  
    #     browser = split[9].replace(',','')
    #     out = split[0] + ',' + split[1] + ',' + split[4] + ',' + browser + ',' + split[8] + ',' + split[16]
    #     return out

    # else:
    #     return split
    #     return None
    
# def process_log_line(line):    
    
#     split = line.split(',')

#     out = sanitize_string(split[2])
#     return out

# def sanitize_string(string):
#     clean = re.sub(r'[^\w/.]', '', string)
#     return clean

# x = process_log_line(s2)
# print(x)

# def is_valid_string(string):
#     pattern = r'^[a-zA-Z0-9_0./-]+$'
#     return bool(re.match(pattern, string))

in_file = open('src/data/staging/merged-data.txt', 'r')


file_type_mapping = {
    'jpg': 'Image',
    'jpeg': 'Image',
    'png': 'Image',
    'gif': 'Image',
    'pdf': 'Document',
    'doc': 'Document',
    'docx': 'Document',
    'txt': 'Text',
}

robot_count = 0

for line in in_file:
    
    split = line.strip().split(',')
    file_path = split[2]
    #file_path = '/Darwin/Home.aspx+com.othermedia.webkit.exceptions.Resource'
    # file_path =  '/Darwin/"+++markerList[i][0]+++"'
    
    # file_name = file_path.split('/')[-1]
    
    file_directory, file_name = os.path.split(file_path)
    
    if '+' in file_name and  '"' not in file_name:
        file_name = file_name.split('+')[0]
    
    a, file_extension = os.path.splitext(file_name)
    
    if file_name == '':
        file_name = 'undefined'

    if file_extension == '':
        file_extension = 'undefined'       
        
        
    # if file_name is '':
    #     file_name = 'undefined'
    #     file_type = 'undefined'
    # else:
    #     file_type = file_name.split('.')[-1]
        
    # file_directory = file_path[:len(file_path) - len(file_name)]
    
    
    if file_name == 'robots.txt':
        print(split[3])
        robot_count += 1
    
    # print(f'{file_path}, {file_name}, {file_extension}, {file_directory}')
    
    # break
print(robot_count)