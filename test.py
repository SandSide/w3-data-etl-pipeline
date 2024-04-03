import re
import os

RAW_DATA = 'src/data/W3SVC1/'
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

def extract_file_details():
    in_file = open('src/data/staging/merged-data.txt', 'r')

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
    
def determine_bots():
    
    robots_1 = 0
    robots_2 = 0
    in_file = open('src/data/staging/merged-data.txt', 'r')
    
    for line in in_file:
        
        file_path = line.split(',')[2]
        
        if file_path == '/robots.txt':
            robots_1 += 1
                    
        if 'robots' in file_path:
            robots_2 += 1
            
    print(robots_1)
    print(robots_2)
 
    
def process_raw_data():
   
    arr = os.listdir(RAW_DATA)
   
    if not arr:
        print('Raw data folder is empty')
  
    out_file_long = open('test-merged-data.txt', 'w')
    out_file_long.close()
    
    for f in arr:
        process_log_file(f)
        
        
def process_log_file(filename):
    
    type = filename[-3:len(filename)]
    
    if (type == 'log'):
        
        in_file = open(RAW_DATA + filename, 'r')
        out_file = open('test-merged-data.txt', 'a')
        
        lines = in_file.readlines()
        
        for line in lines:
            
            # if (line[0] == '#'):
            #     print(line)
            
            if (line[0] != '#'):
            
                result = process_log_line(line)
                
            
                if result:
                    if not result.endswith('\n'):
                        result += '\n'
                        
                    # print(result)
                        
                    out_file.write(result)

    
        in_file.close()
        out_file.close()
                 
    
def process_log_line(line):    
    
    split = line.split(' ')
    response_time = ''
    status_code = ''
    cs_bytes = '-'
    sc_bytes = '-'
    
    if (len(split) == 14):
        status_code = split[-4]
        response_time = split[13]
        # out = split[0] + ',' + split[1] + ',' + file_path + ',' + browser + ',' + split[8] + ',' + split[13] 
        
    elif (len(split) == 18):  
        status_code = split[-6]
        response_time = split[16]
        sc_bytes = split[-3]
        cs_bytes = split[-2]
        # out = split[0] + ',' + split[1] + ',' + file_path + ',' + browser + ',' + split[8] + ',' + split[16]

    else:
        return 
    
    browser = split[9].replace(',','')
    file_path = split[4].replace(',','')

    out = f'{split[0]},{split[1]},{split[3]},{file_path},{browser},{split[8]},{status_code},{sc_bytes},{cs_bytes},{response_time}'
    
    return out
    
def status_code():
    in_file = open('src/data/staging/merged-data.txt', 'r')
    
    for line in in_file:
        
        file_path = line.split(',')[2]
      
      
      
        
def read_data():
    in_file = open('test-merged-data.txt', 'r')
    
    for line in in_file:
        
        split = line.split(',')
        
        file_path = split[3]
        
        # if 'Home.aspx-com' in file_path:
        
        process_file_path(file_path)
        
    
def process_file_path(raw_file_path):

    raw_file_path = raw_file_path.replace('+', ' ')
    file_directory, file_name = os.path.split(raw_file_path)
    
    if '+++' in file_name:
        i = file_name.find('+++')
        file_name = file_name[:i]
        # print(file_name)
    
    file_name = file_name.replace('+', '-').replace('[', '').replace(']', '')
    file_directory = file_directory.replace('+', '-')
    
    if '?' in file_name:
        i = file_name.find('?')
        file_name = file_name[:i]
        # print(file_name)
 
    if '"' in file_name:
        i = file_name.find('"')
        file_name = file_name[:i]
        # print(file_name)
     
    a, file_extension = os.path.splitext(file_name)
    
    if file_directory.endswith('/'):
        file_path = f'{file_directory}{file_name}' 
    else:
        file_path = f'{file_directory}/{file_name}' 
          
    
    out = (raw_file_path, file_path, file_name, file_extension, file_directory)   
    print(out)

# process_raw_data()
# read_data()
process_file_path('/Darwin/Home.aspx+com.othermedia.webkit.exceptions.Resource')
