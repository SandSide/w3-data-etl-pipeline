from user_agents import parse

s = 'Mozilla/4.0+(compatible;+MSIE+7.0;+Windows+NT+5.1;+SIMBAR={425A48EA-D0F7-11DE-A478-001E9090E619};+InfoPath.2;+.NET+CLR+2.0.50727;+OfficeLiveConnector.1.3;+OfficeLivePatch.0.0)'

def extract_browser(string):
    parsed_ua = parse(string)
    return parsed_ua.os.family

print(extract_browser(s))