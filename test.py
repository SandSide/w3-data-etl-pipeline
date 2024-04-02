import re

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
    
def process_log_line(line):    
    
    split = line.split(',')

    out = sanitize_string(split[2])
    return out

def sanitize_string(string):
    clean = re.sub(r'[^\w/.]', '', string)
    return clean

x = process_log_line(s2)
print(x)

def is_valid_string(string):
    pattern = r'^[a-zA-Z0-9_0./-]+$'
    return bool(re.match(pattern, string))

in_file = open('src/data/staging/merged-data.txt', 'r')

for line in in_file:
    split = line.strip().split(',')
    
    
    # cleaned_string = re.sub(r'[\'"]', '', split[2])
    
    # print
    
    # if split[0] == '2010-01-05' and split[1] == '06:32:18':
    #     print(line)
    #     print(len(split))
    
    
    if "cache" in split[2]:
        print(split[2])
    
    # if cleaned_string != split[2]:
        
    #     print(split[2])
    #     print(cleaned_string)